package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// RequestDuration records api request latency, split by route and status.
	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vergil_request_duration_seconds",
		Help:    "Latency of HTTP requests to the decision api.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route", "status"})

	// DecisionsTotal counts decisions by classification (ALLOW/REVIEW/BLOCK).
	DecisionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vergil_decisions_total",
		Help: "Decisions made, labelled by classification.",
	}, []string{"classification"})

	// BatchDuration records how long the consumer takes to save+commit a batch.
	BatchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "vergil_batch_duration_seconds",
		Help:    "Time to persist and commit one consumer batch.",
		Buckets: prometheus.DefBuckets,
	})

	// MessagesProcessed counts audit events persisted by the consumer.
	MessagesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vergil_messages_processed_total",
		Help: "DecisionEvents persisted to the audit store.",
	})

	// KafkaLag is the consumer's last observed lag on the decisions topic.
	KafkaLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vergil_kafka_lag",
		Help: "Consumer group lag reported after the last committed batch.",
	})

	// PoolWorkers is the configured width of the consumer worker pool.
	PoolWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vergil_pool_workers",
		Help: "Number of workers in the consumer pool.",
	})
)

// Handler serves the collectors in Prometheus text format.
func Handler() http.Handler {
	return promhttp.Handler()
}
