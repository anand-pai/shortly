package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func Run(address string, metricsAddress string, register func(*grpc.Server), log *zap.Logger) error {

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen %s: %w", address, err)
	}

	// Export gRPC handling-time histogram buckets so p95/p99 can be computed.
	grpcprom.EnableHandlingTimeHistogram()

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpcprom.UnaryServerInterceptor,
			requestIDUnaryInterceptor(log),
		),
	)

	grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	reflection.Register(grpcServer)
	register(grpcServer)

	// Initialize metrics after all services are registered.
	grpcprom.Register(grpcServer)

	// Start Prometheus metrics HTTP server.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsMux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	metricsServer := &http.Server{
		Addr:         metricsAddress,
		Handler:      metricsMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 2)

	go func() {
		log.Info("gRPC server started", zap.String("address", address))
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			errCh <- serveErr
		}
	}()

	go func() {
		log.Info("metrics server started", zap.String("address", metricsAddress))
		if serveErr := metricsServer.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			errCh <- serveErr
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Info("shutdown signal received", zap.String("signal", sig.String()))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		done := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			metricsServer.Shutdown(ctx) //nolint:errcheck
			close(done)
		}()
		select {
		case <-done:
			log.Info("servers stopped gracefully")
		case <-ctx.Done():
			log.Warn("graceful stop timeout exceeded, forcing stop")
			grpcServer.Stop()
		}
		return nil
	case serveErr := <-errCh:
		return fmt.Errorf("serve: %w", serveErr)
	}
}
