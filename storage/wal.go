package storage

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"slices"
	"time"

	"github.com/ankur-anand/kvalchemy/storage/wrecord"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/rosedblabs/wal"
)

// Ensure WAL implements the interface.
var _ WalReader = (*wal.WAL)(nil)

// WalReader defines an interface for reading Write-Ahead Log (WAL) entries.
type WalReader interface {
	NewReader() *wal.Reader
	NewReaderWithStart(startPos *wal.ChunkPosition) (*wal.Reader, error)
}

// NewWalReader returns a new instance of WalReader, allowing the caller to
// access WAL logs for replication, recovery, or log processing.
//
// nolint:ireturn
func (e *Engine) NewWalReader() WalReader {
	return e.wal
}

// recoverWAL starts from the last stored sequence in BoltDB.
func (e *Engine) recoverWAL(metadata Metadata, namespace string) error {
	startTime := time.Now()
	slog.Info("Recovering WAL...", "namespace", namespace, "counter", e.globalCounter.Load(), "ops-saved", e.opsFlushedCounter.Load())
	var reader *wal.Reader
	var err error
	ignoreFirstChunk := false

	// if the hlc is zero
	// maybe in first run itself, we never crossed the arena limit, and it was never flushed.
	// or the db file was new, somehow.
	// in both the case try loading the entire wal.
	if metadata.RecordProcessed == 0 {
		slog.Info("WAL RecordProcessed Position is 0. Loading the Complete WAL...", "namespace", namespace)
		reader = e.wal.NewReader()
	}

	if metadata.RecordProcessed != 0 {
		// recovery should happen from last chunk position + 1.
		ignoreFirstChunk = true
		slog.Info("Starting WAL replay from hlc ..", "hlc", metadata.RecordProcessed, "segment", metadata.Pos.SegmentId, "offset", metadata.Pos.ChunkOffset, "namespace", namespace)

		reader, err = e.wal.NewReaderWithStart(metadata.Pos)
		if err != nil {
			return err
		}
	}

	if reader == nil {
		panic("can't recover from WAL reader")
	}

	recordCount := 0

	var lastRecord []byte
	for {
		data, pos, err := reader.Next()
		if err == io.EOF || pos == nil {
			fmt.Println(DecodeHLC(wrecord.GetRootAsWalRecord(lastRecord, 0).Hlc()))
			fmt.Println("emd")

			break
		}

		if ignoreFirstChunk {
			ignoreFirstChunk = false
			continue
		}

		recordCount++
		lastRecord = data
		record := wrecord.GetRootAsWalRecord(data, 0)
		fmt.Println(DecodeHLC(record.Hlc()))
		fmt.Println(record.Operation(), "here")
		// Store in MemTable
		if isMemTableOperation(record.Operation()) {
			var memValue []byte
			if int64(len(data)) <= e.storageConfig.ValueThreshold {
				memValue = append([]byte{1}, data...) // Directly store small values
			} else {
				memValue = append([]byte{0}, pos.Encode()...) // Store WAL reference
			}

			err = e.memTableWrite(record.KeyBytes(), y.ValueStruct{
				Meta:  byte(record.Operation()),
				Value: memValue,
			}, pos)
			if err != nil {
				return err
			}
			e.globalCounter.Add(1)
		}
	}

	slog.Info("WAL Recovery Completed", "namespace", namespace, "record_recovered", recordCount, "duration", time.Since(startTime), "counter", e.globalCounter.Load(), "ops-saved", e.opsFlushedCounter.Load())
	e.recoveredEntriesCount = recordCount
	return nil
}

func isMemTableOperation(op wrecord.LogOperation) bool {
	switch op {
	case wrecord.LogOperationOpInsert, wrecord.LogOperationOpDelete, wrecord.LogOperationOpBatchCommit:
		return true
	}
	return false
}

// readChunksFromWal from Wal reads the chunks value from the wal entries and return all the chunks value.
func readChunksFromWal(w *wal.WAL, startPos *wal.ChunkPosition, startID []byte, completeChecksum uint32) ([][]byte, error) {
	if startPos == nil {
		return nil, ErrInternalError
	}

	var decompressValues [][]byte
	var calculatedChecksum uint32
	nextPos := startPos

	for {
		// read the next pos data
		record, err := w.Read(nextPos)
		if err != nil {
			return nil, fmt.Errorf("failed to read WAL at position %+v: %w", nextPos, err)
		}
		wr := wrecord.GetRootAsWalRecord(record, 0)
		value := wr.ValueBytes()
		id := wr.BatchIdBytes()
		checksum := wr.RecordChecksum()

		if wr.Operation() != wrecord.LogOperationOPBatchInsert && wr.Operation() != wrecord.LogOperationOpBatchStart {
			return nil, fmt.Errorf("unexpected WAL operation %d at position %+v: %w", wr.Operation(), nextPos, ErrRecordCorrupted)
		}

		if !bytes.Equal(startID, id) {
			return nil, fmt.Errorf("mismatched batch ID at position %+v: expected %x, got %x: %w", nextPos, startID, id, ErrRecordCorrupted)
		}

		if wr.Operation() == wrecord.LogOperationOPBatchInsert {
			decompressed, err := DecompressLZ4(value)
			if err != nil {
				return nil, fmt.Errorf("failed to decompress WAL value at position %+v: %w", nextPos, ErrRecordCorrupted)
			}

			if crc32.ChecksumIEEE(decompressed) != checksum {
				return nil, fmt.Errorf(
					"checksum mismatch at position %+v: expected %d, got %d: %w",
					nextPos, checksum, crc32.ChecksumIEEE(decompressed), ErrRecordCorrupted,
				)
			}

			decompressValues = append(decompressValues, decompressed)
		}

		next := wr.LastBatchPosBytes()
		// We hit the last record.
		if next == nil {
			break
		}

		nextPos = wal.DecodeChunkPosition(next)
	}

	// as the data-are read in reverse
	slices.Reverse(decompressValues)

	for i := 0; i < len(decompressValues); i++ {
		calculatedChecksum = crc32.Update(calculatedChecksum, crc32.IEEETable, decompressValues[i])
	}

	if calculatedChecksum != completeChecksum {
		return nil, fmt.Errorf(
			"final checksum mismatch: expected %d, got %d: %w",
			completeChecksum, calculatedChecksum, ErrRecordCorrupted,
		)
	}

	return decompressValues, nil
}
