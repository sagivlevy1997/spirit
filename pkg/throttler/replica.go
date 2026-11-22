package throttler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

var (
	blockWaitInterval = 1 * time.Second
	// loopInterval is how often replica lag is read. 1s, not upstream's 5s:
	// the smallest --replica-max-lag is 1s, and a budget polled less often
	// than its own size is overrun before anything sees it.
	loopInterval = 1 * time.Second
)

// ReplicaSetAsideMsg and ReplicaBackMsg are the two log lines a replica's
// health changes produce, each once per episode, with host=<host:port> and
// (for set-aside) reason=<why>. spirit-migrations parses them to tell the
// DBA, so the text is a contract: change it here and in that app's
// config.REPLICA_SET_ASIDE_MARKER / REPLICA_BACK_MARKER together.
const (
	ReplicaSetAsideMsg = "replica set aside"
	ReplicaBackMsg     = "replica working again"
)

// Replica throttles on replication lag, pausing the copy while the observed
// lag exceeds the configured tolerance (--replica-max-lag).
//
// A broken replica is SET ASIDE, never failed closed. When polling has kept
// failing for staleSignalThreshold (unreachable, deleted, replication
// stopped, lag unreadable) -- or fails on the very first poll -- the replica
// stops counting: IsThrottled() is false for it, and one ReplicaSetAsideMsg
// line names it. The other replicas keep the lag budget. It counts again,
// with one ReplicaBackMsg line, on the first poll that succeeds.
//
// This replaced a fail-closed design that held the copy up to 60s per chunk
// while any replica was unobservable, so a deleted replica made the copy
// crawl until someone restarted the engine without it -- and the pre-cutover
// health check made the engine exit on it outright. Replica state is the
// DBA's information, not a reason to stall or throw away a run.
type Replica struct {
	replica *sql.DB
	// host is host:port from the DSN, for naming this replica in logs. Never
	// the DSN itself: it carries the password.
	host           string
	lagTolerance   time.Duration
	currentLagInMs atomic.Int64
	logger         *slog.Logger
	isClosed       atomic.Bool

	// stale records when the last successful poll was; a gap past
	// staleSignalThreshold is what turns failing polls into "set aside".
	stale staleGuard

	// setAside is true while this replica is excluded from throttling;
	// setAsideWhy is the failure that set it aside. Guarded by mu so the
	// state change and its single log line cannot race between the poll
	// loop and Open.
	mu          sync.Mutex
	setAside    bool
	setAsideWhy string
}

// MySQL8LagQuery is a query that is used to get the lag between the source and the replica.
// The implementation is described in https://github.com/block/spirit/issues/286
// It uses performance_schema instead of a heartbeat injection or seconds_behind_source.
const MySQL8LagQuery = `WITH applier_latency AS (
	SELECT MAX(TIMESTAMPDIFF(MICROSECOND, APPLYING_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP, NOW())/1000 ) AS applier_latency_ms
	FROM performance_schema.replication_applier_status_by_worker
   ), queue_latency AS (
	SELECT MIN(
	CASE
	 WHEN
	  LAST_QUEUED_TRANSACTION = 'ANONYMOUS' OR
	  LAST_APPLIED_TRANSACTION = 'ANONYMOUS' OR
	  GTID_SUBTRACT(LAST_QUEUED_TRANSACTION, LAST_APPLIED_TRANSACTION) = ''
	 THEN 0
	  ELSE
	  TIMESTAMPDIFF(MICROSECOND, LAST_APPLIED_TRANSACTION_IMMEDIATE_COMMIT_TIMESTAMP, NOW(3))/1000
	END
   ) AS queue_latency_ms,
   IF(MIN(TIMESTAMPDIFF(MINUTE, LAST_QUEUED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP, NOW()))>1,'IDLE','ACTIVE') as queue_status
   FROM performance_schema.replication_applier_status_by_worker w
   JOIN performance_schema.replication_connection_status s ON s.channel_name = w.channel_name
   )
   SELECT IFNULL(IF(queue_status='IDLE',0,CEIL(GREATEST(applier_latency_ms, queue_latency_ms))),0) as lagMs FROM applier_latency, queue_latency
`

var _ Throttler = &Replica{}

// Open starts the lag monitor. This is not gh-ost. The lag monitor is primitive
// because the requirement is only for DR, and not for up-to-date read-replicas.
// Because chunk-sizes are typically 500ms, getting fine-grained metrics is not realistic.
// We check the replica every second (see loopInterval), so a budget as tight
// as gh-ost's 3s is measured, not guessed at.
//
// A first poll that fails does not fail Open: the replica starts set aside
// (see the type comment) and the loop keeps polling, so it can still recover.
func (l *Replica) Open(ctx context.Context) error {
	if err := l.UpdateLag(ctx); err != nil {
		l.logger.Debug("first replica poll failed; the replica starts set aside", "host", l.host, "error", err)
	}
	go func() {
		ticker := time.NewTicker(loopInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if l.isClosed.Load() {
					return
				}
				// Debug, not Error: a broken replica fails every second, and
				// its one ReplicaSetAsideMsg line already says so.
				if err := l.UpdateLag(ctx); err != nil {
					l.logger.Debug("error getting lag", "host", l.host, "error", err)
				}
			}
		}
	}()
	return nil
}

func (l *Replica) Close() error {
	l.isClosed.Store(true)
	return nil
}

// IsThrottled returns true when the last observed lag exceeds the tolerance.
// A set-aside replica, or one whose signal has gone stale, never throttles:
// see the type comment.
func (l *Replica) IsThrottled() bool {
	if aside, _ := l.SetAside(); aside {
		return false
	}
	if stale, _ := l.stale.check(staleSignalThreshold); stale {
		return false
	}
	return l.currentLagInMs.Load() >= l.lagTolerance.Milliseconds()
}

// SetAside reports whether this replica is currently excluded from
// throttling, and the failure that set it aside.
func (l *Replica) SetAside() (bool, string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.setAside, l.setAsideWhy
}

// Replica deliberately does NOT implement GradualThrottler: replication lag
// is an SLO-style budget, not a load gauge — normalizing it against the
// tolerance (typically 120s) would make the autoscaler treat half the lag
// budget as headroom and park replicas a minute behind. It is also coarse and
// bistable (near 0 when healthy, climbing without bound when not). Replica
// protection stays binary: IsThrottled/BlockWait hard-stop the copy at the
// tolerance.

// BlockWait blocks until the throttle clears (lag within the tolerance), or
// up to 60s to allow some progress to be made. A set-aside replica returns at
// once. It respects context cancellation.
func (l *Replica) BlockWait(ctx context.Context) {
	timer := time.NewTimer(blockWaitInterval)
	defer timer.Stop()

	for range 60 {
		if !l.IsThrottled() {
			return
		}

		timer.Reset(blockWaitInterval)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// Continue checking
		}
	}
	l.logger.Warn("lag monitor timed out",
		"host", l.host,
		"lag_ms", l.currentLagInMs.Load(),
		"tolerance", l.lagTolerance.String())
}

// UpdateLag is a MySQL 8.0+ implementation of lag that is a better approximation than "seconds_behind_source".
// It requires performance_schema to be enabled.
//
// A poll succeeds only if lag is readable AND both replication threads run:
// the lag query alone may read a stopped SQL thread as no lag at all. Any
// failure goes through applyFailure, which decides whether the replica is
// now set aside.
func (l *Replica) UpdateLag(ctx context.Context) error {
	var newLagValue int64
	if err := l.replica.QueryRowContext(ctx, MySQL8LagQuery).Scan(&newLagValue); err != nil {
		err = fmt.Errorf("could not check replication lag (check that this is a MySQL 8.0 replica, and that performance_schema is enabled): %w", err)
		l.applyFailure(err.Error())
		return err
	}
	running, why, err := ReplicationRunning(ctx, l.replica)
	if err != nil {
		l.applyFailure(err.Error())
		return err
	}
	if !running {
		l.applyFailure(why)
		return fmt.Errorf("replication is not running: %s", why)
	}
	l.applyLag(newLagValue)
	return nil
}

// applyFailure records one failed poll. The replica is set aside -- once per
// episode, with one ReplicaSetAsideMsg line -- when it has never answered in
// this run, or when its last good poll is older than staleSignalThreshold, so
// a single blip does not flap it. Split out so tests can drive the state
// without a real replica.
func (l *Replica) applyFailure(reason string) {
	neverAnswered := l.stale.lastSampleAt.Load() == 0
	if !neverAnswered && !l.stale.gapExceeds(staleSignalThreshold) {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.setAside {
		return
	}
	l.setAside, l.setAsideWhy = true, reason
	l.logger.Warn(ReplicaSetAsideMsg, "host", l.host, "reason", reason)
}

// applyLag updates state from a single successful lag observation. Split out
// so tests can drive the state without a real replica.
func (l *Replica) applyLag(newLagValue int64) {
	l.stale.markFresh()
	l.mu.Lock()
	if l.setAside {
		l.setAside, l.setAsideWhy = false, ""
		l.logger.Info(ReplicaBackMsg, "host", l.host)
	}
	l.mu.Unlock()
	l.currentLagInMs.Store(newLagValue)
	if newLagValue >= l.lagTolerance.Milliseconds() {
		l.logger.Warn("replication delayed, throttling in progress",
			"host", l.host,
			"lag_ms", newLagValue,
			"tolerance", l.lagTolerance.String())
	}
}

// ReplicationRunning reports whether SHOW REPLICA STATUS shows both threads
// running. why says what is wrong when running is false; err is a failure to
// ask at all.
func ReplicationRunning(ctx context.Context, db *sql.DB) (running bool, why string, err error) {
	rows, err := db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return false, "", fmt.Errorf("could not read SHOW REPLICA STATUS: %w", err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return false, "", fmt.Errorf("could not read SHOW REPLICA STATUS: %w", err)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return false, "", fmt.Errorf("could not read SHOW REPLICA STATUS: %w", err)
		}
		ok, why := replicaThreadsState(false, "", "", "", "")
		return ok, why, nil
	}
	values := make([]any, len(columns))
	for i := range values {
		values[i] = new(sql.NullString)
	}
	if err := rows.Scan(values...); err != nil {
		return false, "", fmt.Errorf("could not read SHOW REPLICA STATUS: %w", err)
	}
	field := make(map[string]string, len(columns))
	for i, name := range columns {
		field[name] = values[i].(*sql.NullString).String
	}
	ok, why := replicaThreadsState(true, field["Replica_IO_Running"], field["Replica_SQL_Running"],
		field["Replica_IO_State"], field["Replica_SQL_Running_State"])
	return ok, why, nil
}

// replicaThreadsState turns the SHOW REPLICA STATUS fields into a verdict.
func replicaThreadsState(hasRow bool, io, sqlRunning, ioState, sqlState string) (bool, string) {
	if !hasRow {
		return false, "not a replica (SHOW REPLICA STATUS returned no rows)"
	}
	if io != "Yes" {
		return false, fmt.Sprintf("IO thread not running (Replica_IO_State: %s)", ioState)
	}
	if sqlRunning != "Yes" {
		return false, fmt.Sprintf("SQL thread not running (Replica_SQL_Running_State: %s)", sqlState)
	}
	return true, ""
}
