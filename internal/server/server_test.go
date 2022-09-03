package server

import (
	"context"
	api "github.com/danielgom/proglog/api/v1"
	"github.com/danielgom/proglog/internal/config"
	"github.com/danielgom/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"testing"
)

func TestServer(t *testing.T) {

	for scenario, fn := range map[string]func(t *testing.T, client api.LogClient, config *Config){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past boundary fails":                        testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(config *Config)) (client api.LogClient, cfg *Config, teardown func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ClientCertFile,
		KeyFile:  config.ClientKeyFile,
		CAFile:   config.CAFile,
	})
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(clientTLSConfig)

	cc, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(clientCreds))
	require.NoError(t, err)

	client = api.NewLogClient(cc)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{CommitLog: clog}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		err = server.Serve(l)
		if err != nil {
			panic("could not serve")
		}
	}()

	return client, cfg, func() {
		server.Stop()
		_ = cc.Close()
		_ = l.Close()
		_ = clog.Remove()
	}

}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.GetOffset()})
	require.NoError(t, err)
	require.Equal(t, want.GetValue(), consume.GetRecord().GetValue())
	require.Equal(t, want.GetOffset(), consume.GetRecord().GetOffset())
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	records := []*api.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second world"), Offset: 1},
	}

	stream, err := client.ProduceStream(ctx)
	require.NoError(t, err)

	for off, record := range records {
		err = stream.Send(&api.ProduceRequest{Record: record})
		require.NoError(t, err)

		res, err := stream.Recv()

		require.NoError(t, err)
		require.Equal(t, res.GetOffset(), uint64(off))
	}

	consumeStream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	require.NoError(t, err)

	for idx, record := range records {
		res, err := consumeStream.Recv()
		require.NoError(t, err)

		require.Equal(t, res.GetRecord(), &api.Record{
			Value:  record.GetValue(),
			Offset: uint64(idx),
		})
	}

}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.GetOffset() + 1})

	require.Nil(t, consume)

	ofRange := api.ErrOffsetOutOfRange{}
	require.Equal(t, status.Code(ofRange.GRPCStatus().Err()), status.Code(err))

}
