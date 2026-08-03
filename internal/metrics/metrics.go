package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	namespace = "bsky_firehose"
)

var (
	// MessagesReceived counts the total number of messages received from the firehose
	MessagesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_received_total",
			Help:      "Total number of messages received from the Bluesky firehose",
		},
		[]string{"type"}, // type: commit, error_frame, etc.
	)

	// MessagesProcessed counts the total number of messages successfully processed
	MessagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_processed_total",
			Help:      "Total number of messages successfully processed",
		},
		[]string{"record_type"}, // post, like, repost, etc.
	)

	// MessagesPublished counts the total number of messages published to NATS
	MessagesPublished = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_published_total",
			Help:      "Total number of messages published to NATS",
		},
		[]string{"subject"}, // post, like, repost, etc.
	)

	// PublishErrors counts the number of failed message publishes to NATS
	PublishErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "publish_errors_total",
			Help:      "Total number of failed message publishes to NATS",
		},
		[]string{"subject"},
	)

	// ProcessingErrors counts the number of message processing errors
	ProcessingErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "processing_errors_total",
			Help:      "Total number of message processing errors",
		},
		[]string{"stage"}, // car_read, record_get, marshal, etc.
	)

	// WebSocketReconnects counts the number of WebSocket reconnection attempts
	WebSocketReconnects = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "websocket_reconnects_total",
			Help:      "Total number of WebSocket reconnection attempts",
		},
	)

	// CurrentSequence tracks the current sequence number being processed
	CurrentSequence = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "current_sequence",
			Help:      "Current sequence number being processed",
		},
	)

	// NATSConnectionStatus tracks the NATS connection status (1 = connected, 0 = disconnected)
	NATSConnectionStatus = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "nats_connection_status",
			Help:      "NATS connection status (1 = connected, 0 = disconnected)",
		},
	)

	// WebSocketConnectionStatus tracks the WebSocket connection status (1 = connected, 0 = disconnected)
	WebSocketConnectionStatus = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "websocket_connection_status",
			Help:      "WebSocket connection status (1 = connected, 0 = disconnected)",
		},
	)

	// MessageProcessingTime tracks the time taken to process messages
	MessageProcessingTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "message_processing_seconds",
			Help:      "Time taken to process a message",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"record_type"},
	)

	// NATSPublishTime tracks the time taken to publish messages to NATS
	NATSPublishTime = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "nats_publish_seconds",
			Help:      "Time taken to publish a message to NATS",
			Buckets:   []float64{0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.1, 0.5, 1},
		},
	)

	// registry is the Prometheus registry for our metrics
	registry = prometheus.NewRegistry()
)

// Init initializes all metrics and registers them with the registry
func Init() {
	registry.MustRegister(
		MessagesReceived,
		MessagesProcessed,
		MessagesPublished,
		PublishErrors,
		ProcessingErrors,
		WebSocketReconnects,
		CurrentSequence,
		NATSConnectionStatus,
		WebSocketConnectionStatus,
		MessageProcessingTime,
		NATSPublishTime,
	)
}

// Handler returns the HTTP handler for the metrics endpoint
func Handler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// StartServer starts the metrics HTTP server on the given port
func StartServer(port string) {
	http.Handle("/metrics", Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	go http.ListenAndServe("0.0.0.0:"+port, nil)
}
