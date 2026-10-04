package checksum

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// quietChecker builds a bare SingleChecker for exercising the re-read
// adjudication without a database. Only the fields the drain touches are set;
// `freshRead` is the seam the production wiring points at freshReadChunk.
func quietChecker(freshRead func(context.Context, *table.Chunk) (chunkRead, error)) *SingleChecker {
	return &SingleChecker{
		logger:              slog.New(slog.NewTextHandler(testWriter{}, &slog.HandlerOptions{Level: slog.LevelError})),
		retryDelay:          time.Millisecond,
		maxSrcChangedCycles: DefaultMaxSrcChangedCycles,
		freshRead:           freshRead,
	}
}

// ---------------------------------------------------------------------------
// The adjudication
// ---------------------------------------------------------------------------

// A chunk that mismatched during the pass but whose target has since caught up
// to the source version we witnessed is apply lag, not a difference. It must
// leave the drain with nothing to answer for.
func TestRereadDismissesAChunkThatCaughtUp(t *testing.T) {
	chunk := newTestChunk(0, 1000)
	first := chunkRead{srcCRC: 100, tgtCRC: 99, srcCount: 10, tgtCount: 10}

	c := quietChecker(func(context.Context, *table.Chunk) (chunkRead, error) {
		// Target now equals the source we first saw.
		return chunkRead{srcCRC: 100, tgtCRC: 100, srcCount: 10, tgtCount: 10}, nil
	})
	c.deferMismatch(chunk, first)

	suspects, err := c.rereadPending(t.Context())
	require.NoError(t, err)
	require.Empty(t, suspects, "a target that caught up is not a difference")
	require.Equal(t, uint64(1), c.lagResolved.Load())
	require.Equal(t, uint64(0), c.differencesFound.Load())
}

// The source moved on during the retry window, and the target matches that
// NEWER source. We never witnessed a stable window, but the target is
// demonstrably keeping up, so this passes without being treated as a hot chunk.
func TestRereadDismissesAChunkThatCaughtUpToANewerSource(t *testing.T) {
	chunk := newTestChunk(0, 1000)
	first := chunkRead{srcCRC: 100, tgtCRC: 99, srcCount: 10, tgtCount: 10}

	c := quietChecker(func(context.Context, *table.Chunk) (chunkRead, error) {
		return chunkRead{srcCRC: 200, tgtCRC: 200, srcCount: 11, tgtCount: 11}, nil
	})
	c.deferMismatch(chunk, first)

	suspects, err := c.rereadPending(t.Context())
	require.NoError(t, err)
	require.Empty(t, suspects)
	require.Equal(t, uint64(1), c.lagResolved.Load())
}

// A chunk whose SOURCE keeps changing never gives us a stable window to judge
// in, so it is re-queued rather than condemned. It must not be re-queued
// forever: at the bound it is handed on as a suspect so the pass can finish
// and the chunk is adjudicated on its merits.
func TestRereadRequeuesAHotChunkUpToTheBound(t *testing.T) {
	chunk := newTestChunk(0, 1000)
	first := chunkRead{srcCRC: 1, tgtCRC: 99, srcCount: 10, tgtCount: 10}

	var reads int
	c := quietChecker(func(context.Context, *table.Chunk) (chunkRead, error) {
		reads++
		// Source different every time, target never matching it.
		return chunkRead{srcCRC: int64(100 + reads), tgtCRC: 99, srcCount: 10, tgtCount: 10}, nil
	})
	c.maxSrcChangedCycles = 3
	c.deferMismatch(chunk, first)

	suspects, err := c.rereadPending(t.Context())
	require.NoError(t, err)
	require.Len(t, suspects, 1, "a permanently hot chunk must still be adjudicated, not dropped")
	// One read to detect it is hot, then one per re-queue up to the bound.
	require.Equal(t, 4, reads)
	require.Equal(t, uint64(0), c.lagResolved.Load())
	require.Equal(t, 0, c.pendingDepth(), "the queue must be drained, not left holding the chunk")
}

// A chunk whose source is stable and whose target is still wrong is what a
// real divergence looks like. It survives the re-read as a suspect.
func TestRereadKeepsAStableDivergenceAsASuspect(t *testing.T) {
	chunk := newTestChunk(0, 1000)
	first := chunkRead{srcCRC: 100, tgtCRC: 99, srcCount: 10, tgtCount: 10}

	c := quietChecker(func(context.Context, *table.Chunk) (chunkRead, error) {
		return first, nil // nothing moved; target still wrong
	})
	c.deferMismatch(chunk, first)

	suspects, err := c.rereadPending(t.Context())
	require.NoError(t, err)
	require.Len(t, suspects, 1)
	require.Equal(t, uint64(0), c.lagResolved.Load())
}

// The re-read must not be able to dismiss a chunk by looking at the same bytes
// the pass already looked at. This is the negative control for the whole
// feature: if `freshRead` were wired to the pass's frozen snapshot, every
// mismatch would "reconcile" and the checksum would pass on diverged tables.
func TestARereadThatSeesTheSameStateDoesNotDismissTheChunk(t *testing.T) {
	chunk := newTestChunk(0, 1000)
	first := chunkRead{srcCRC: 100, tgtCRC: 99, srcCount: 10, tgtCount: 9}

	c := quietChecker(func(context.Context, *table.Chunk) (chunkRead, error) {
		return first, nil
	})
	c.deferMismatch(chunk, first)

	suspects, err := c.rereadPending(t.Context())
	require.NoError(t, err)
	require.Len(t, suspects, 1, "an unchanged re-read is not evidence of reconciliation")
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

func TestRetryDefaultsRefuseAnImmediateReRead(t *testing.T) {
	// Zero and negative both mean "unset", not "re-read instantly". A zero
	// delay would re-read the same state and confirm every mismatch, which is
	// the feature silently doing nothing.
	for _, bad := range []time.Duration{0, -time.Minute} {
		cfg := &CheckerConfig{RetryDelay: bad}
		applyRetryDefaults(cfg)
		require.Equal(t, DefaultSingleRetryDelay, cfg.RetryDelay)
		require.Equal(t, DefaultMaxSrcChangedCycles, cfg.MaxSrcChangedCycles)
	}
	// A caller's explicit value is kept.
	cfg := &CheckerConfig{RetryDelay: 5 * time.Second, MaxSrcChangedCycles: 2}
	applyRetryDefaults(cfg)
	require.Equal(t, 5*time.Second, cfg.RetryDelay)
	require.Equal(t, 2, cfg.MaxSrcChangedCycles)
}

// ---------------------------------------------------------------------------
// Against a real server
// ---------------------------------------------------------------------------

// TestFreshSnapshotDoesNotLeakItsTransaction is a regression test.
//
// TrxPool.Get() REMOVES the transaction from the pool's slice, and Close()
// only rolls back what the slice still holds -- so a Get without a matching
// Put leaves the transaction open. Each one holds a SHARED_READ metadata lock
// on both tables and an open REPEATABLE READ view: on a large table that is
// exactly the history-list growth the yield mechanism exists to prevent, and
// the next pass blocks on its table lock for the full 30s timeout and then
// KILLs the connection to get in.
//
// The first version of this file had that bug. It cost 27 seconds per affected
// run and every test still passed.
func TestFreshSnapshotDoesNotLeakItsTransaction(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	c := &SingleChecker{
		db:       db,
		dbConfig: dbconn.NewDBConfig(),
		logger:   slog.New(slog.NewTextHandler(testWriter{}, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	var captured *sql.Tx
	require.NoError(t, c.withFreshSnapshot(t.Context(), func(trx *sql.Tx) error {
		captured = trx
		// It is a usable, consistent read view while we are inside.
		var one int
		return trx.QueryRowContext(t.Context(), "SELECT 1").Scan(&one)
	}))

	require.NotNil(t, captured)
	_, err = captured.ExecContext(t.Context(), "SELECT 1")
	require.ErrorIs(t, err, sql.ErrTxDone, "the snapshot transaction was left open")
}

// TestApplyLagReconcilesAndTheChecksumPasses is the headline behaviour, end to
// end against a real server.
//
// The target starts wrong, so the pass mismatches. Before the deferred re-read
// fires, the target is corrected -- which is what the change feed does on a
// live migration, a moment later than the checksum happened to look. The
// checksum must pass.
//
// FixDifferences is OFF deliberately: with it on, a confirmed difference would
// be repaired and the distinction between "reconciled" and "repaired" would be
// invisible in the result. Off, a confirmed difference is an error, so a pass
// here can only mean the re-read dismissed it.
func TestApplyLagReconcilesAndTheChecksumPasses(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS lagreconcile_t1, _lagreconcile_t1_new, _lagreconcile_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE lagreconcile_t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _lagreconcile_t1_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _lagreconcile_t1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO lagreconcile_t1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _lagreconcile_t1_new VALUES (1, 2, 999)") // behind: c is stale

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "lagreconcile_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_lagreconcile_t1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	config := NewCheckerDefaultConfig()
	config.FixDifferences = false
	config.MaxRetries = 1
	config.RetryDelay = 3 * time.Second
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)

	// The "applier catching up", well inside the retry window and well after
	// the pass has released its table lock.
	repaired := make(chan error, 1)
	go func() {
		time.Sleep(500 * time.Millisecond)
		_, err := db.ExecContext(context.Background(),
			"UPDATE _lagreconcile_t1_new SET c = 3 WHERE a = 1")
		repaired <- err
	}()

	err = checker.Run(t.Context())
	require.NoError(t, <-repaired)
	require.NoError(t, err, "a chunk that reconciled on re-read must not fail the checksum")

	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok)
	require.Equal(t, uint64(0), singleChecker.differencesFound.Load())
	require.Equal(t, uint64(1), singleChecker.lagResolved.Load(),
		"the mismatch should have been recorded as reconciled, not merely missed")
}

// The control for the test above, same shape, with nothing correcting the
// target. A difference that is still there when we look again is real and must
// still fail -- otherwise the change above would just be the checksum giving up.
func TestADifferenceThatDoesNotReconcileStillFails(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS lagpersist_t1, _lagpersist_t1_new, _lagpersist_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE lagpersist_t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _lagpersist_t1_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _lagpersist_t1_chkpnt (a INT)")
	testutils.RunSQL(t, "INSERT INTO lagpersist_t1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _lagpersist_t1_new VALUES (1, 2, 999)") // and it stays wrong

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "lagpersist_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_lagpersist_t1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	config := fastRetryConfig()
	config.FixDifferences = false
	config.MaxRetries = 1
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)

	err = checker.Run(t.Context())
	require.ErrorContains(t, err, "checksum mismatch")

	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok)
	require.Equal(t, uint64(1), singleChecker.differencesFound.Load())
	require.Equal(t, uint64(0), singleChecker.lagResolved.Load())
}
