// Package throttler contains code to throttle the rate of writes to a table.
package throttler

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

type Throttler interface {
	Open(ctx context.Context) error
	Close() error
	IsThrottled() bool
	BlockWait(ctx context.Context)
	UpdateLag(ctx context.Context) error
}

// GradualThrottler is an optional extension implemented by throttlers whose
// underlying signal is continuous, not just a binary stop/go. The write-thread
// autoscaler type-asserts for it and only engages when it is present.
//
// The Aurora throttlers (AuroraThreads, CommitLatency) implement it. The
// replica-lag throttler deliberately does not: lag is an SLO-style budget,
// not a load gauge — normalizing it would make the autoscaler treat half the
// lag budget as headroom and park replicas a minute behind. Signals like that
// stay binary, protecting via the IsThrottled/BlockWait hard-stop only.
type GradualThrottler interface {
	Throttler
	// Utilization reports current load relative to this throttler's throttle
	// point: 0 = idle, 1.0 = exactly where IsThrottled() flips true, >1.0 =
	// over. It is the smooth, continuous signal the autoscaler controls on;
	// IsThrottled() remains the binary hard-stop.
	Utilization() float64
}

// NewReplicationThrottler returns a Throttler for MySQL 8.0+ replicas.
// It uses performance_schema to monitor replication lag.
//
// A replica that cannot be polled is set aside, never failed closed: see the
// Replica type. NewReplicationThrottlerForHost names the replica in its logs.
func NewReplicationThrottler(replica *sql.DB, lagTolerance time.Duration, logger *slog.Logger) (Throttler, error) {
	return NewReplicationThrottlerForHost(replica, "", lagTolerance, logger)
}

// NewReplicationThrottlerForHost is NewReplicationThrottler with the
// replica's host:port, which every log line about it carries. Pass the host,
// never the DSN: a DSN embeds the password.
func NewReplicationThrottlerForHost(replica *sql.DB, host string, lagTolerance time.Duration, logger *slog.Logger) (Throttler, error) {
	return &Replica{
		replica:      replica,
		host:         host,
		lagTolerance: lagTolerance,
		logger:       logger,
	}, nil
}
