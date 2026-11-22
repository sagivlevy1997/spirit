package throttler

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func newTestReplica(t *testing.T, tolerance time.Duration) *Replica {
	t.Helper()
	return &Replica{
		lagTolerance: tolerance,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestReplica_LagBasedThrottling(t *testing.T) {
	l := newTestReplica(t, 120*time.Second)

	l.applyLag(5) // 5ms, healthy
	require.False(t, l.IsThrottled())

	l.applyLag(120_000) // exactly at the tolerance
	require.True(t, l.IsThrottled())

	l.applyLag(30_000) // recovered below the tolerance
	require.False(t, l.IsThrottled())
}

// newLoggedTestReplica is newTestReplica with a host name and a log the test
// can read, for asserting the set-aside / working-again lines the app parses.
func newLoggedTestReplica(t *testing.T, tolerance time.Duration) (*Replica, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	return &Replica{
		host:         "db-x:3306",
		lagTolerance: tolerance,
		logger:       slog.New(slog.NewTextHandler(logs, nil)),
	}, logs
}

// A replica whose polling keeps failing is set aside, not failed closed: a
// deleted or unreachable replica must never make the copy wait between
// chunks. Lag protection stays on the replicas that still answer.
func TestReplica_SetAsideInsteadOfFailingClosed(t *testing.T) {
	l, logs := newLoggedTestReplica(t, 120*time.Second)
	l.applyLag(5)
	ageLastSample(&l.stale, staleSignalThreshold+time.Second)

	l.applyFailure("dial tcp: lookup db-x: no such host")
	require.False(t, l.IsThrottled())
	aside, why := l.SetAside()
	require.True(t, aside)
	require.Contains(t, why, "no such host")

	// Still failing: no second line for the same episode.
	l.applyFailure("dial tcp: lookup db-x: no such host")
	require.Equal(t, 1, strings.Count(logs.String(), ReplicaSetAsideMsg))
	require.Contains(t, logs.String(), "host=db-x:3306")
}

// One failed poll inside the stale threshold is a blip, not a broken replica.
func TestReplica_BlipDoesNotSetAside(t *testing.T) {
	l, logs := newLoggedTestReplica(t, 120*time.Second)
	l.applyLag(5)
	l.applyFailure("i/o timeout")
	aside, _ := l.SetAside()
	require.False(t, aside)
	require.NotContains(t, logs.String(), ReplicaSetAsideMsg)
}

// A set-aside replica that answers again counts for lag protection again,
// including throttling if it came back far behind.
func TestReplica_RecoversAndCountsAgain(t *testing.T) {
	l, logs := newLoggedTestReplica(t, 120*time.Second)
	l.applyLag(5)
	ageLastSample(&l.stale, staleSignalThreshold+time.Second)
	l.applyFailure("SQL thread not running")

	l.applyLag(999_999)
	aside, _ := l.SetAside()
	require.False(t, aside)
	require.True(t, l.IsThrottled())
	require.Equal(t, 1, strings.Count(logs.String(), ReplicaBackMsg))
	require.Contains(t, logs.String(), "host=db-x:3306")
}

// A replica that never answered in this run is set aside on its first
// failure: there is no healthy signal to wait out a threshold for.
func TestReplica_FirstPollFailureSetsAsideImmediately(t *testing.T) {
	l, logs := newLoggedTestReplica(t, 120*time.Second)
	l.applyFailure("connection refused")
	aside, _ := l.SetAside()
	require.True(t, aside)
	require.Equal(t, 1, strings.Count(logs.String(), ReplicaSetAsideMsg))
}

func TestReplica_NeverPolledIsNotStale(t *testing.T) {
	// No poll yet and no failure yet: not throttled, not set aside.
	l := newTestReplica(t, 120*time.Second)
	require.False(t, l.IsThrottled())
	aside, _ := l.SetAside()
	require.False(t, aside)
}

// Open must not fail the engine on a replica it cannot reach.
func TestReplica_OpenDoesNotFailOnUnreachable(t *testing.T) {
	db, err := sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/test?timeout=200ms")
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	th, err := NewReplicationThrottlerForHost(db, "127.0.0.1:1", 120*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.NoError(t, th.Open(ctx))
	aside, _ := th.(*Replica).SetAside()
	require.True(t, aside)
	require.False(t, th.IsThrottled())
}

func TestReplica_BlockWaitReturnsAtOnceWhenSetAside(t *testing.T) {
	l, _ := newLoggedTestReplica(t, 120*time.Second)
	l.applyLag(5)
	ageLastSample(&l.stale, staleSignalThreshold+time.Second)
	l.applyFailure("no such host")

	start := time.Now()
	l.BlockWait(t.Context())
	require.Less(t, time.Since(start), 50*time.Millisecond)
}

// Even with no failure recorded (the poll loop stopped), a stale signal no
// longer fails closed.
func TestReplica_StaleSignalDoesNotThrottle(t *testing.T) {
	l, _ := newLoggedTestReplica(t, 120*time.Second)
	l.applyLag(5)
	ageLastSample(&l.stale, staleSignalThreshold+time.Second)
	require.False(t, l.IsThrottled())
}

func TestReplicaThreadsState(t *testing.T) {
	ok, _ := replicaThreadsState(true, "Yes", "Yes", "", "")
	require.True(t, ok)

	ok, why := replicaThreadsState(true, "Yes", "No", "", "")
	require.False(t, ok)
	require.Contains(t, why, "SQL thread not running")

	ok, why = replicaThreadsState(true, "Connecting", "Yes", "Connecting to source", "")
	require.False(t, ok)
	require.Contains(t, why, "IO thread not running")
	require.Contains(t, why, "Connecting to source")

	ok, why = replicaThreadsState(false, "", "", "", "")
	require.False(t, ok)
	require.Contains(t, why, "not a replica")
}

func TestReplica_UpdateLagWrapsCause(t *testing.T) {
	// sql.Open is lazy and does not connect. A pre-canceled context makes
	// QueryRowContext fail deterministically with context.Canceled before any
	// dial, letting us assert the cause survives UpdateLag's wrapping.
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:0)/test")
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	l := newTestReplica(t, 120*time.Second)
	l.replica = db

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err = l.UpdateLag(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled, "UpdateLag must wrap the underlying cause, not replace it")
	require.ErrorContains(t, err, "could not check replication lag")
}

// The smallest lag budget the engine accepts is 1s (see the settings check).
// A budget is only as tight as the poll that measures it: checked every 5s, a
// 3s limit can be overrun by 5s before the copy is held back.
func TestReplica_LagIsPolledAtLeastOnceASecond(t *testing.T) {
	require.LessOrEqual(t, loopInterval, time.Second)
}

// The marker texts are a contract with spirit-migrations, which parses them.
// Every other test goes through the constants, so this is the one that fails
// if someone renames them.
func TestReplica_MarkerTextIsTheContract(t *testing.T) {
	require.Equal(t, "replica set aside", ReplicaSetAsideMsg)
	require.Equal(t, "replica working again", ReplicaBackMsg)
}

// The set-aside reason is what the DBA reads in Slack: the real cause, not
// the lag query's performance_schema hint wrapped around a deleted host.
func TestReplica_SetAsideReasonIsTheRawCause(t *testing.T) {
	db, err := sql.Open("mysql", "user:pass@tcp(127.0.0.1:0)/test")
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	l, _ := newLoggedTestReplica(t, 120*time.Second)
	l.replica = db
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err = l.UpdateLag(ctx)
	require.ErrorContains(t, err, "could not check replication lag") // the returned error keeps its hint
	_, why := l.SetAside()
	require.Contains(t, why, "context canceled")
	require.NotContains(t, why, "performance_schema")
}

// A replica that accepts the connection and never answers (a black hole)
// must not hold a poll -- or the cutover check that shares this bound.
func TestReplica_HungReplicaPollTimesOut(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer conn.Close() // hold it open, never greet
		}
	}()
	prev := ReplicaQueryTimeout
	ReplicaQueryTimeout = 200 * time.Millisecond
	t.Cleanup(func() { ReplicaQueryTimeout = prev })

	db, err := sql.Open("mysql", "u:p@tcp("+ln.Addr().String()+")/test")
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	l, _ := newLoggedTestReplica(t, 120*time.Second)
	l.replica = db

	start := time.Now()
	require.Error(t, l.UpdateLag(t.Context()))
	require.Less(t, time.Since(start), 2*time.Second)
	aside, _ := l.SetAside()
	require.True(t, aside)
}
