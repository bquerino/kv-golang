package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Request metrics
	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kvstore_request_duration_seconds",
		Help:    "Duration of requests in seconds",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
	}, []string{"operation", "node_id", "status"})

	RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kvstore_requests_total",
		Help: "Total number of requests",
	}, []string{"operation", "node_id", "status"})

	// Replication metrics
	ReplicationLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kvstore_replication_latency_seconds",
		Help:    "Latency of replication to followers",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
	}, []string{"target_node", "leader_node"})

	ReplicationErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kvstore_replication_errors_total",
		Help: "Total number of replication errors",
	}, []string{"target_node", "leader_node", "error_type"})

	SuccessfulReplications = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kvstore_successful_replications_total",
		Help: "Total number of successful replications",
	}, []string{"target_node", "leader_node"})

	ReplicationSuccessTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kvstore_replication_success_total",
		Help: "Total number of successful replications (simplified)",
	}, []string{})

	ReplicationFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kvstore_replication_failures_total",
		Help: "Total number of failed replications (simplified)",
	}, []string{"target_node"})

	// Conflict resolution metrics
	ConflictsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kvstore_conflicts_total",
		Help: "Total number of conflicts detected",
	}, []string{"node_id", "key", "resolution_type"})

	ConflictResolutionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kvstore_conflict_resolution_duration_seconds",
		Help:    "Duration of conflict resolution",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
	}, []string{"node_id", "resolution_type"})

	// Leader election metrics
	ElectionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kvstore_election_duration_seconds",
		Help:    "Duration of leader elections",
		Buckets: []float64{0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0},
	}, []string{"node_id", "result"})

	ElectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kvstore_elections_total",
		Help: "Total number of leader elections",
	}, []string{"node_id", "result"})

	LeaderChanges = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kvstore_leader_changes_total",
		Help: "Total number of leader changes",
	}, []string{"old_leader", "new_leader"})

	// Node health metrics
	NodeStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kvstore_node_status",
		Help: "Status of nodes (1=alive, 0=dead)",
	}, []string{"node_id", "address"})

	IsLeader = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kvstore_is_leader",
		Help: "Whether this node is the leader (1=leader, 0=follower)",
	}, []string{"node_id"})

	CurrentTerm = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kvstore_current_term",
		Help: "Current election term",
	}, []string{"node_id"})

	// Storage metrics
	KeysStored = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kvstore_keys_stored",
		Help: "Number of keys stored locally",
	}, []string{"node_id"})

	HintedHandoffQueueSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kvstore_hinted_handoff_queue_size",
		Help: "Number of items in hinted handoff queue",
	}, []string{"node_id", "target_node"})

	// Consistency metrics
	StalenessSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kvstore_staleness_seconds",
		Help:    "Staleness of reads in seconds",
		Buckets: []float64{0, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0},
	}, []string{"node_id", "key"})

	ReadYourWritesViolations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kvstore_read_your_writes_violations_total",
		Help: "Total violations of read-your-writes consistency",
	}, []string{"node_id", "key"})

	MonotonicReadViolations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kvstore_monotonic_read_violations_total",
		Help: "Total violations of monotonic read consistency",
	}, []string{"node_id", "key"})

	// Convergence metrics
	ConvergenceTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kvstore_convergence_time_seconds",
		Help:    "Time for cluster to converge after write",
		Buckets: []float64{0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 300.0},
	}, []string{"operation", "cluster_size"})
)

// Helper functions for timing operations
func Timer(histogram *prometheus.HistogramVec, labels prometheus.Labels) func() {
	start := time.Now()
	return func() {
		histogram.With(labels).Observe(time.Since(start).Seconds())
	}
}

func MeasureLatency(operation string, nodeID string, status string, fn func() error) error {
	timer := Timer(RequestDuration, prometheus.Labels{
		"operation": operation,
		"node_id":   nodeID,
		"status":    status,
	})
	defer timer()

	err := fn()

	// Update counters
	if err != nil {
		RequestsTotal.With(prometheus.Labels{
			"operation": operation,
			"node_id":   nodeID,
			"status":    "error",
		}).Inc()
	} else {
		RequestsTotal.With(prometheus.Labels{
			"operation": operation,
			"node_id":   nodeID,
			"status":    "success",
		}).Inc()
	}

	return err
}

func RecordReplicationMetrics(targetNode, leaderNode string, latency time.Duration, err error) {
	if err != nil {
		ReplicationErrors.With(prometheus.Labels{
			"target_node": targetNode,
			"leader_node": leaderNode,
			"error_type":  "connection_failed",
		}).Inc()
	} else {
		SuccessfulReplications.With(prometheus.Labels{
			"target_node": targetNode,
			"leader_node": leaderNode,
		}).Inc()

		ReplicationLatency.With(prometheus.Labels{
			"target_node": targetNode,
			"leader_node": leaderNode,
		}).Observe(latency.Seconds())
	}
}

func UpdateNodeMetrics(nodeID, address string, isAlive, isLeaderFlag bool, term int64) {
	var aliveValue float64 = 0
	if isAlive {
		aliveValue = 1
	}
	NodeStatus.With(prometheus.Labels{
		"node_id": nodeID,
		"address": address,
	}).Set(aliveValue)

	var leaderValue float64 = 0
	if isLeaderFlag {
		leaderValue = 1
	}
	IsLeader.With(prometheus.Labels{
		"node_id": nodeID,
	}).Set(leaderValue)

	CurrentTerm.With(prometheus.Labels{
		"node_id": nodeID,
	}).Set(float64(term))
}

// Init initializes metrics collection
func Init() {
	// Métricas são automaticamente registradas por promauto
	// Esta função é chamada para garantir que todas as métricas sejam inicializadas
}
