package server

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const requestIDHeader = "x-request-id"

type requestIDContextKey struct{}

func RequestIDFromContext(ctx context.Context) string {
	if rid, ok := ctx.Value(requestIDContextKey{}).(string); ok {
		return rid
	}
	return ""
}

func requestIDUnaryInterceptor(log *zap.Logger, workerId string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		startedAt := time.Now()
		rid := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			values := md.Get(requestIDHeader)
			if len(values) > 0 {
				rid = values[0]
			}
		}
		if rid == "" {
			rid = uuid.NewString()
		}

		ctx = context.WithValue(ctx, requestIDContextKey{}, rid)
		resp, err := handler(ctx, req)

		code := status.Code(err).String()
		fields := []zap.Field{
			zap.String("request_id", rid),
			zap.String("method", info.FullMethod),
			zap.String("grpc_code", code),
			zap.String("worker_id", workerId),
			zap.Int64("duration_ms", time.Since(startedAt).Milliseconds()),
		}

		if err != nil {
			log.Error("grpc request failed", append(fields, zap.Error(err))...)
			return resp, err
		}

		log.Info("grpc request completed", fields...)
		return resp, nil
	}
}
