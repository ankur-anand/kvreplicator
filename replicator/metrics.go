package replicator

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus Metrics.
var (
	// summary as client can live long.
	rpcDurations = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "grpc_request_duration_seconds",
			Help:       "Summary of gRPC request durations per namespace, method and status code",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, // 50th, 90th, 99th percentiles
		},
		[]string{"namespace", "method", "status"},
	)

	metricsActiveStreams = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wal_replicator_active_streams",
		Help: "Number of currently active WAL replication streams.",
	}, []string{"namespace", "method"})

	metricsStreamSendLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wal_replicator_stream_latency",
		Help:    "Latency of streaming WAL records.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
	}, []string{"namespace", "method"})

	metricsStreamSendErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wal_replicator_send_errors_total",
		Help: "Total number of errors during sending WAL records.",
	}, []string{"namespace", "method"})

	prefetchErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wal_replicator_prefetch_errors_total",
		Help: "Total number of errors during WAL record prefetching.",
	}, []string{"namespace"})

	metricsEOFTimeouts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wal_replicator_eof_timeouts_total",
		Help: "Total number of EOF timeouts during WAL record prefetching.",
	}, []string{"namespace"})
)

// RegisterMetrics registers the Prometheus metrics.
func RegisterMetrics() {
	prometheus.MustRegister(rpcDurations)
}
