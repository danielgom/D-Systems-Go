package server

import (
	"context"
	api "github.com/danielgom/proglog/api/v1"
	"github.com/danielgom/proglog/internal/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	objectWildCard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type CommitLog interface {
	Append(record *api.Record) (uint64, error)
	Read(offset uint64) (*api.Record, error)
}

type Config struct {
	CommitLog  CommitLog
	Authorizer auth.Authorizer
}

type grpcServer struct {
	api.UnimplementedLogServer
	config *Config
}

func NewGRPCServer(config *Config, options ...grpc.ServerOption) (*grpc.Server, error) {

	options = append(options, grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(grpc_auth.StreamServerInterceptor(authenticate))),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(grpc_auth.UnaryServerInterceptor(authenticate))))

	server := grpc.NewServer(options...)
	srv, err := newGrpcServer(config)
	if err != nil {
		return nil, err
	}

	api.RegisterLogServer(server, srv)
	return server, nil
}

func newGrpcServer(config *Config) (*grpcServer, error) {
	srv := &grpcServer{
		config: config,
	}

	return srv, nil
}

type subjectContextKey struct {
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

func authenticate(ctx context.Context) (context.Context, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "couldn't find peer info").Err()
	}

	if p.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := p.AuthInfo.(credentials.TLSInfo)
	sub := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, sub)

	return ctx, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	err := s.config.Authorizer.Authorize(subject(ctx), objectWildCard, produceAction)
	if err != nil {
		return nil, err
	}

	offset, err := s.config.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: offset}, nil

}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	err := s.config.Authorizer.Authorize(subject(ctx), objectWildCard, consumeAction)
	if err != nil {
		return nil, err
	}

	record, err := s.config.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case *api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}

			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
