package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"shortly/helpers"
	"shortly/internal/repository"
	"syscall"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// Buckets sit on the expected click path (cache or Scylla lookup plus redirect)
// so p95 and p99 fall inside a bucket instead of being interpolated across a wide gap.
var redirectLatencyBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
}

var (
	// shortly_redirect_duration_seconds supports:
	//   p95: histogram_quantile(0.95, sum by (le) (rate(shortly_redirect_duration_seconds_bucket[5m])))
	//   p99: histogram_quantile(0.99, sum by (le) (rate(shortly_redirect_duration_seconds_bucket[5m])))
	//   avg: sum(rate(shortly_redirect_duration_seconds_sum[5m])) / sum(rate(shortly_redirect_duration_seconds_count[5m]))
	redirectDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "shortly",
			Subsystem: "redirect",
			Name:      "duration_seconds",
			Help:      "Latency of short URL click requests in seconds. histogram_quantile on the buckets yields p95 and p99; sum divided by count yields the average.",
			Buckets:   redirectLatencyBuckets,
		},
		[]string{"code", "method"},
	)

	// shortly_redirect_requests_total is the total click-handler request count.
	redirectRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "shortly",
			Subsystem: "redirect",
			Name:      "requests_total",
			Help:      "Total HTTP requests handled by the short URL click handler.",
		},
		[]string{"code", "method"},
	)
)

// SMS slugs are 8 characters and may contain '-' or '_'. RCS/WA slugs are 11 base62 characters.
// 9-character slugs are still accepted from the previous SMS encoding.
var slugPattern = regexp.MustCompile(`^(?:[0-9A-Za-z_-]{8}|[0-9A-Za-z]{9}|[0-9A-Za-z]{11})$`)

const redirectLookupTimeout = 2 * time.Second

func Run(address string, metricsAddress string, httpAddress string, register func(*grpc.Server), txtlyRepository repository.TxtlyRepository, log *zap.Logger) error {

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen %s: %w", address, err)
	}

	workerID := helpers.GetEnv("POD_NAME")

	// Export gRPC handling-time histogram buckets so p95/p99 can be computed.
	grpcprom.EnableHandlingTimeHistogram()

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpcprom.UnaryServerInterceptor,
			requestIDUnaryInterceptor(log, workerID),
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

	// For exposing metrics for prometheus scrape

	metricsServer := &http.Server{
		Addr:         metricsAddress,
		Handler:      metricsMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// For handling the redirect requests

	if httpAddress == "" {
		httpAddress = ":8001"
	}

	redirectServer := &http.Server{
		Addr:         httpAddress,
		Handler:      newRedirectHandler(txtlyRepository, log),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	errCh := make(chan error, 3)

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

	go func() {
		log.Info("redirect server started", zap.String("address", httpAddress))
		if serveErr := redirectServer.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
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
			metricsServer.Shutdown(ctx)
			redirectServer.Shutdown(ctx)
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

func newRedirectHandler(txtlyRepository repository.TxtlyRepository, log *zap.Logger) http.Handler {
	clicks := promhttp.InstrumentHandlerDuration(
		redirectDuration,
		promhttp.InstrumentHandlerCounter(
			redirectRequests,
			&redirectHandler{txtlyRepo: txtlyRepository, log: log},
		),
	)

	mux := http.NewServeMux()
	mux.Handle("GET /{slug}", clicks)
	return mux
}

type redirectHandler struct {
	txtlyRepo repository.TxtlyRepository
	log       *zap.Logger
}

func (h *redirectHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	if !slugPattern.MatchString(slug) {
		http.NotFound(w, r)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), redirectLookupTimeout)
	defer cancel()

	mapping, err := h.txtlyRepo.GetShortURL(ctx, slug)
	if err != nil {
		if errors.Is(err, repository.ErrSlugNotFound) {
			http.NotFound(w, r)
			return
		}
		h.log.Error("short url lookup failed", zap.String("slug", slug), zap.Error(err))
		http.Error(w, "short url lookup failed", http.StatusInternalServerError)
		return
	}

	target, parseErr := url.Parse(mapping.LongUrl)
	if parseErr != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") {
		h.log.Error("short url target is not redirectable", zap.String("slug", slug))
		http.Error(w, "short url target is invalid", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, mapping.LongUrl, http.StatusFound)
}
