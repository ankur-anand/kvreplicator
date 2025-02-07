package replicator

import (
	"bytes"
	"context"
	"math/rand/v2"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	v1 "github.com/ankur-anand/kvalchemy/proto/gen/go/kvalchemy/replicator/v1"
	"github.com/ankur-anand/kvalchemy/storage"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const (
	listenerBuffSize = 1024
)

func bufDialer(lis *bufconn.Listener) func(ctx context.Context, s string) (net.Conn, error) {
	return func(ctx context.Context, s string) (net.Conn, error) {
		return lis.Dial()
	}
}

const (
	smallValue       = 512             // 512 bytes
	largeValue       = 2 * 1024 * 1024 // 2MB values
	largeValueChance = 0.2             // 20% chance of having large values
)

func TestServer_Invalid_Request(t *testing.T) {
	var engines = make(map[string]*storage.Engine)
	var nameSpaces = make([]string, 0)

	for i := 0; i < 1; i++ {
		nameSpaces = append(nameSpaces, strings.ToLower(gofakeit.Noun()))
	}

	closeEngines := func(t *testing.T) {
		for _, engine := range engines {
			err := engine.Close()
			if err != nil {
				assert.NoError(t, err)
			}
		}
	}

	dir := os.TempDir()
	temp, err := os.MkdirTemp(dir, "kvalchemy")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(temp)
	for _, nameSpace := range nameSpaces {
		se, err := storage.NewStorageEngine(temp, nameSpace, nil)
		if err != nil {
			panic(err)
		}
		engines[nameSpace] = se
	}

	defer closeEngines(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := &WalReplicatorServer{
		storageEngines:                           engines,
		UnimplementedWALReplicationServiceServer: v1.UnimplementedWALReplicationServiceServer{},
	}

	listener := bufconn.Listen(listenerBuffSize)
	defer listener.Close()
	gS := grpc.NewServer(grpc.ChainStreamInterceptor(RequireNamespaceInterceptor, RequestIDStreamInterceptor, CorrelationIDStreamInterceptor, TelemetryInterceptor))
	defer gS.Stop()

	go func() {
		v1.RegisterWALReplicationServiceServer(gS, server)
		if err := gS.Serve(listener); err != nil {
			assert.NoError(t, err, "failed to grpc start server")
		}
	}()

	// conn, err := grpc.NewClient()
	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	assert.NoError(t, err, "failed to create grpc client")

	client := v1.NewWALReplicationServiceClient(conn)

	t.Run("MissingNamespace", func(t *testing.T) {
		wal, err := client.StreamWAL(ctx, &v1.StreamWALRequest{})
		assert.NoError(t, err, "failed to stream WAL")
		_, err = wal.Recv()
		assert.Error(t, err, "expected error on missing namespace")

		statusErr := status.Convert(err)
		assert.Equal(t, codes.InvalidArgument, statusErr.Code(), "expected error code InvalidArgument")
		assert.Equal(t, ErrMissingNamespaceInMetadata.Error(), statusErr.Message(), "expected error message didn't match")
	})

	// Case 2: Namespace does not exist
	t.Run("NamespaceNotExists", func(t *testing.T) {
		md := metadata.Pairs("x-namespace", "unknown")
		ctx := metadata.NewOutgoingContext(context.Background(), md)
		wal, err := client.StreamWAL(ctx, &v1.StreamWALRequest{})
		assert.NoError(t, err, "failed to stream WAL")
		_, err = wal.Recv()
		assert.Error(t, err, "expected error on non-existent namespace")

		statusErr := status.Convert(err)
		assert.Equal(t, codes.NotFound, statusErr.Code(), "expected error code NotFound")
		assert.Equal(t, ErrNamespaceNotExists.Error(), statusErr.Message(), "expected error message didn't match")
	})

	// Case 3: Invalid Metadata
	t.Run("InvalidMetadata", func(t *testing.T) {
		md := metadata.Pairs("x-namespace", nameSpaces[0])
		ctx := metadata.NewOutgoingContext(context.Background(), md)
		wal, err := client.StreamWAL(ctx, &v1.StreamWALRequest{
			Metadata: []byte{1, 2, 3}, // Corrupt data
		})
		assert.NoError(t, err, "failed to stream WAL")
		_, err = wal.Recv()
		assert.Error(t, err, "expected error on invalid metadata")

		statusErr := status.Convert(err)
		assert.Equal(t, codes.InvalidArgument, statusErr.Code(), "expected error code InvalidArgument")
		assert.Equal(t, ErrInvalidMetadata.Error(), statusErr.Message(), "expected error message didn't match")
	})

}

func TestServer_StreamWAL(t *testing.T) {
	var engines = make(map[string]*storage.Engine)
	var nameSpaces = make([]string, 0)

	for i := 0; i < 2; i++ {
		nameSpaces = append(nameSpaces, strings.ToLower(gofakeit.Noun()))
	}

	closeEngines := func(t *testing.T) {
		for _, engine := range engines {
			err := engine.Close()
			if err != nil {
				assert.NoError(t, err)
			}
		}
	}

	dir := os.TempDir()
	temp, err := os.MkdirTemp(dir, "kvalchemy")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(temp)
	for _, nameSpace := range nameSpaces {
		se, err := storage.NewStorageEngine(temp, nameSpace, nil)
		assert.NoError(t, err)
		assert.NotNil(t, se)
		// for each engine write the records, few of them being more than 1 MB in size.
		// write a total of
		for i := 0; i < 10; i++ {
			key := []byte(gofakeit.Noun())
			valueSize := smallValue
			if rand.Float64() < largeValueChance {
				valueSize = largeValue
			}
			value := []byte(gofakeit.LetterN(50))
			data := bytes.Repeat(value, valueSize)
			err = se.Put(key, data)
			assert.NoError(t, err)
		}
		engines[nameSpace] = se
	}

	defer closeEngines(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errGroup, ctx := errgroup.WithContext(ctx)
	server := &WalReplicatorServer{
		storageEngines:                           engines,
		UnimplementedWALReplicationServiceServer: v1.UnimplementedWALReplicationServiceServer{},
		errGrp:                                   errGroup,
		eofRetryInterval:                         30 * time.Millisecond,
		dynamicTimeout:                           200 * time.Millisecond,
	}

	listener := bufconn.Listen(listenerBuffSize)
	defer listener.Close()
	gS := grpc.NewServer(grpc.ChainStreamInterceptor(RequireNamespaceInterceptor, RequestIDStreamInterceptor, CorrelationIDStreamInterceptor, TelemetryInterceptor))
	defer gS.Stop()

	go func() {
		v1.RegisterWALReplicationServiceServer(gS, server)
		if err := gS.Serve(listener); err != nil {
			assert.NoError(t, err, "failed to grpc start server")
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(bufDialer(listener)),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	assert.NoError(t, err, "failed to create grpc client")
	client := v1.NewWALReplicationServiceClient(conn)

	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		timer := time.NewTimer(150 * time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case <-ticker.C:
				key := []byte(gofakeit.Noun())
				valueSize := smallValue
				if rand.Float64() < largeValueChance {
					valueSize = largeValue
				}
				value := []byte(gofakeit.LetterN(50))
				data := bytes.Repeat(value, valueSize)
				err = engines[nameSpaces[0]].Put(key, data)
				assert.NoError(t, err)
			case <-timer.C:
				return
			}
		}
	}()

	t.Run("wal-replication", func(t *testing.T) {
		md := metadata.Pairs("x-namespace", nameSpaces[0])
		ctx := metadata.NewOutgoingContext(context.Background(), md)
		wal, err := client.StreamWAL(ctx, &v1.StreamWALRequest{})
		assert.NoError(t, err, "failed to stream WAL")

		valuesCount := 0

		for {

			val, err := wal.Recv()
			if err != nil {
				statusErr := status.Convert(err)
				assert.Equal(t, codes.DeadlineExceeded, statusErr.Code(), "expected error code NotFound")
				assert.Equal(t, statusErr.Message(), ErrStreamTimeout.Error(), "expected error message didn't match")
				break
			}
			valuesCount = +len(val.WalRecords)
		}

		assert.Greater(t, valuesCount, 0, "total 20 logs should have streamed")

	})

	errN := errGroup.Wait()
	assert.NoError(t, errN)
}
