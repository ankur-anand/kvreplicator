package replicator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/ankur-anand/kvalchemy/logchunk"
	v1 "github.com/ankur-anand/kvalchemy/proto/gen/go/kvalchemy/replicator/v1"
	"github.com/ankur-anand/kvalchemy/storage"
	"github.com/rosedblabs/wal"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const (
	eofRetryInterval       = 30 * time.Second
	dynamicTimeout         = 1 * time.Minute
	backPressureBufferSize = 5
	batchFlushThreshold    = 500 * wal.KB
	batchFlushTimeout      = 50 * time.Millisecond
)

// WalReplicatorServer implements WALReplicationService.
type WalReplicatorServer struct {
	// namespace mapped engine
	storageEngines map[string]*storage.Engine
	v1.UnimplementedWALReplicationServiceServer
	errGrp           *errgroup.Group
	dynamicTimeout   time.Duration
	eofRetryInterval time.Duration
}

// StreamWAL stream the underlying WAL record on the connection.
func (s *WalReplicatorServer) StreamWAL(request *v1.StreamWALRequest, g grpc.ServerStreamingServer[v1.StreamWALResponse]) error {
	namespace := getNamespace(g.Context())

	if namespace == "" {
		return toGRPCError(ErrMissingNamespaceInMetadata)
	}

	engine, ok := s.storageEngines[namespace]
	if !ok {
		return toGRPCError(ErrNamespaceNotExists)
	}

	// it can contain terrible data
	meta, err := decodeMetadata(request.GetMetadata())

	if err != nil {
		return toGRPCError(ErrInvalidMetadata)
	}

	wr := engine.NewWalReader()
	var reader *wal.Reader
	var rErr error
	if meta.Index == 0 {
		reader = wr.NewReader()
	} else {
		reader, rErr = wr.NewReaderWithStart(meta.Pos)
		if rErr != nil {
			return toGRPCError(rErr)
		}
	}

	ctx, cancel := context.WithCancel(g.Context())
	defer cancel()

	ch := make(chan fetchedWalRecord, backPressureBufferSize)
	s.errGrp.Go(func() error {
		s.prefetchWalRecord(ctx, namespace, reader, ch)
		return nil
	})

	return s.streamWalRecords(ctx, namespace, g, ch)
}

// streamWalRecords streams namespaced Write-Ahead Log (WAL) records to the client in batches
// .
// While it might be tempting to stream WAL records indefinitely until the client cancels the RPC,
// this approach has significant drawbacks and is avoided here using dynamic timeout mechanism.
// (Note:  GOAWAY settings handle underlying connection issues.)
// Continuously streaming also has several problems:
//
//   - Resource Exhaustion: A malfunctioning client stream could monopolize the server's buffer,
//   - Unnecessary Processing: The server might fetch and stream WAL records that the client has already
//     received or is no longer interested in.
func (s *WalReplicatorServer) streamWalRecords(ctx context.Context, namespace string, g grpc.ServerStream, prefetchChan <-chan fetchedWalRecord) error {
	var (
		batch          []*v1.WALRecord
		totalBatchSize int
	)

	flushBatch := func() error {
		err := s.flushBatch(batch, g, namespace)
		batch = nil
		totalBatchSize = 0
		return err
	}

	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if errors.Is(err, ErrStreamTimeout) {
				return toGRPCError(ErrStreamTimeout)
			}
			if errors.Is(err, context.Canceled) {
				return ErrStatusOk
			}
			return ctx.Err()

		case response, ok := <-prefetchChan:

			if !ok {
				return ErrStatusOk
			}

			if response.err != nil {
				return toGRPCError(response.err)
			}

			batch = append(batch, response.records...)
			totalBatchSize += response.size

			if totalBatchSize >= batchFlushThreshold {
				if err := flushBatch(); err != nil {
					return toGRPCError(err)
				}
			}

		case <-time.After(batchFlushTimeout):
			if totalBatchSize > 0 {
				if err := flushBatch(); err != nil {
					return toGRPCError(err)
				}
			}
		}
	}
}

func (s *WalReplicatorServer) flushBatch(batch []*v1.WALRecord, g grpc.ServerStream, namespace string) error {
	method := getMethod(g.Context())
	if len(batch) == 0 {
		return nil
	}
	slog.Debug("[GRPC] Batch flushing", "size", len(batch))
	response := &v1.StreamWALResponse{WalRecords: batch}

	start := time.Now()
	defer func() {
		metricsStreamSendLatency.WithLabelValues(namespace, method).Observe(time.Since(start).Seconds())
	}()

	if err := g.SendMsg(response); err != nil {
		slog.Error("[GRPC] Stream: failed to send WAL records", "err", err,
			"records_count", len(batch), "namespace", namespace)
		metricsStreamSendErrors.WithLabelValues(namespace, method).Inc()
		return fmt.Errorf("failed to send WAL records: %w", err)
	}
	return nil
}

// prefetchWalRecord prefetches wal records in advance and employs chunking mechanism to send record,
// if the record size is more than 1 MB.
//
// nolint:gocognit
func (s *WalReplicatorServer) prefetchWalRecord(ctx context.Context, namespace string, reader *wal.Reader, prefetchChan chan<- fetchedWalRecord) {
	defer close(prefetchChan)
	// Track the last valid WAL record time
	lastValidRecordTime := time.Now()
	eofCount := 0
	var lastValidChunkPos *wal.ChunkPosition // Store last valid position
	currentReader := reader

	// skip1 when a new reader is rotated.
	skip1 := false

	for {
		select {
		case <-ctx.Done():
			slog.Debug("Stopping WAL prefetch goroutine", "namespace", namespace)
			return
		default:
			record, chunkPos, err := currentReader.Next()
			// a new reader has been rotated. we need to drop the same record.
			// this is the behaviour of the underlying wal that we use, we need
			// to provide the chunk position from where we want to read, which is
			// the last chunk position of the previous reader.
			if skip1 {
				skip1 = false
				continue
			}

			if err == io.EOF {
				eofCount++
				// Check if we have received EOF continuously for 1 minute
				if time.Since(lastValidRecordTime) > s.dynamicTimeout {
					slog.Warn("WAL prefetch exiting: Continuous EOF for 1 minute", "namespace", namespace)
					select {
					case prefetchChan <- fetchedWalRecord{err: ErrStreamTimeout}:
						metricsEOFTimeouts.WithLabelValues(namespace).Add(1)
						return
					case <-ctx.Done():
						slog.Debug("Stopping WAL prefetch due to context cancellation", "namespace", namespace)
						return
					}
				}

				// we want to wait before going for retry, as this might indicate a slow producer.
				time.Sleep(s.eofRetryInterval)
				if lastValidChunkPos != nil {
					// Try creating a new reader from last known valid position
					newReader, err := s.storageEngines[namespace].NewWalReader().NewReaderWithStart(lastValidChunkPos)
					if err != nil {
						slog.Error("Failed to fetch new WAL reader", "err", err, "namespace", namespace)
						select {
						case prefetchChan <- fetchedWalRecord{err: err}:
						case <-ctx.Done():
							slog.Debug("Stopping WAL prefetch due to context cancellation", "namespace", namespace)
						}
						return
					}
					skip1 = true
					currentReader = newReader // Use the new reader
					slog.Debug("Switched to new WAL reader from last known position", "namespace", namespace)
				}

				continue
			}

			if err != nil {
				slog.Error("WAL read error:", "err", err, "namespace", namespace)
				select {
				case prefetchChan <- fetchedWalRecord{err: err}:
				case <-ctx.Done():
					slog.Debug("Stopping WAL prefetch due to context cancellation", "namespace", namespace)
					return
				}
				return
			}

			// Reset the EOF tracking if we get a valid record
			eofCount = 0
			lastValidRecordTime = time.Now()
			lastValidChunkPos = chunkPos // Store last read chunk position
			// Prepare fetchedWalRecord with size calculation
			chunks := logchunk.ChunkWALRecord(chunkPos.Encode(), record)
			var totalSize int
			for _, chunk := range chunks {
				totalSize += len(chunk.CompressedData)
			}

			// Send response to the channel
			select {
			case prefetchChan <- fetchedWalRecord{records: chunks, size: totalSize}:
			case <-ctx.Done():
				slog.Debug("Stopping WAL prefetch due to context cancellation", "namespace", namespace)
				return
			}
		}
	}
}

type fetchedWalRecord struct {
	records []*v1.WALRecord
	size    int
	err     error
}

// decodeMetadata protects the.
func decodeMetadata(data []byte) (m storage.Metadata, err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
			err = fmt.Errorf("decode ChunkPosition: Panic recovered %v", r)
		}
	}()
	if len(data) == 0 {
		return storage.Metadata{}, nil
	}
	m = storage.UnmarshalMetadata(data)
	return m, err
}
