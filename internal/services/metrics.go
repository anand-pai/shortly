package services

import (
	"time"

	"shortly/internal/generator"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
)

var createLatencyBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
}

var (
	createDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "shortly",
			Subsystem: "grpc",
			Name:      "create_duration_seconds",
			Help:      `CreateShortUrl RPC latency in seconds by channel and gRPC status code. p50: histogram_quantile(0.50, sum by (le, channel) (rate(shortly_grpc_create_duration_seconds_bucket{code="OK"}[1m]))); p90: histogram_quantile(0.90, sum by (le, channel) (rate(shortly_grpc_create_duration_seconds_bucket{code="OK"}[1m]))); p99: histogram_quantile(0.99, sum by (le, channel) (rate(shortly_grpc_create_duration_seconds_bucket{code="OK"}[1m]))).`,
			Buckets:   createLatencyBuckets,
		},
		[]string{"channel", "code"},
	)

	createRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "shortly",
			Subsystem: "grpc",
			Name:      "create_requests_total",
			Help:      "Total CreateShortUrl RPCs by channel and gRPC status code. rate(shortly_grpc_create_requests_total[1m]) is requests/sec.",
		},
		[]string{"channel", "code"},
	)
)

func channelLabel(channel int32) string {
	switch channel {
	case generator.ChannelSMS:
		return "sms"
	case generator.ChannelRCSWA:
		return "rcs_wa"
	default:
		return "invalid"
	}
}

func observeCreate(channel int32, code codes.Code, elapsed time.Duration) {
	ch := channelLabel(channel)
	statusCode := code.String()
	createDuration.WithLabelValues(ch, statusCode).Observe(elapsed.Seconds())
	createRequests.WithLabelValues(ch, statusCode).Inc()
}
