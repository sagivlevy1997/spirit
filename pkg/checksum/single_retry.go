package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
)

// Delayed re-read for SingleChecker.
//
// # Why this exists
//
// The target legitimately lags the source: the change feed applies binlog
// events asynchronously, so a chunk read at an instant when one of its rows
// has been written on the source but not yet applied to the target reads as
// a mismatch. Nothing is wrong — the row reconciles moments later.
//
// SingleChecker used to count that instant as a difference, permanently:
// `differencesFound` is never decremented and a pass succeeds only at zero,
// so ONE row caught mid-apply, anywhere in the table, burned the whole pass.
// With MaxRetries=3 a table large enough that a pass takes hours, and busy
// enough that something is always mid-apply, can never produce a clean pass.
// It then fails with "checksum found differences on every attempt", which
// reads as "your tables diverged" and is not what happened.
//
// ContinuousChecker (spirit sync) already models this correctly — see the
// convergence model at the top of continuous.go. This is the same
// adjudication, in the shape SingleChecker needs: the walk defers mismatches
// instead of counting them, and a drain pass re-reads each one on a FRESH
// snapshot after a delay.
//
// # Why a fresh snapshot
//
// The walk compares source and target inside one REPEATABLE READ transaction
// so both sides are read at the same instant. That snapshot does not advance,
// so re-reading inside it would return the identical bytes forever and the
// re-read would be a no-op that silently always confirms. The drain therefore
// opens its own consistent-snapshot transaction per attempt.
//
// # Why this does not weaken the checksum
//
// A deferred chunk is only ever dismissed on EVIDENCE that it reconciled:
// the target has to be observed equal to a version of the source we actually
// witnessed. Everything else still fails, and before declaring a divergence
// the drain flushes the change feed and looks once more, so "the applier was
// simply behind" is excluded rather than assumed. A chunk whose source keeps
// moving is re-queued rather than dismissed, and only up to a bound — an
// endlessly hot chunk is adjudicated, not ignored.

const (
	// DefaultSingleRetryDelay is how long a deferred chunk waits before it is
	// re-read. It is the apply-lag budget: long enough that a feed a little
	// behind has caught up, short enough that a pass with a handful of
	// deferrals does not stretch. ContinuousChecker uses the same minute for
	// the same reason.
	DefaultSingleRetryDelay = time.Minute

	// DefaultMaxSrcChangedCycles bounds the "source kept changing" path. A row
	// rewritten continuously would otherwise re-queue forever and a pass would
	// never end. At the bound the chunk stops being re-queued and is
	// adjudicated on its merits (flush the feed, look again, and count it if
	// it is still wrong), so a permanently hot chunk fails loudly rather than
	// hanging.
	DefaultMaxSrcChangedCycles = 10
)

// applyRetryDefaults fills in the re-read settings a caller left unset.
//
// A zero RetryDelay would re-read a deferred chunk immediately. That is not a
// cheaper version of this feature, it is a broken one: the re-read's whole
// value is the time it gives the change feed to apply what it has already
// read, so with no delay it observes the same state and confirms every
// mismatch. Falling back is therefore the safe reading of "unset", and a
// negative value is treated the same rather than being obeyed.
func applyRetryDefaults(config *CheckerConfig) {
	if config.RetryDelay <= 0 {
		config.RetryDelay = DefaultSingleRetryDelay
	}
	if config.MaxSrcChangedCycles <= 0 {
		config.MaxSrcChangedCycles = DefaultMaxSrcChangedCycles
	}
}

// chunkRead is one observation of a chunk on both sides, taken at a single
// instant. Counts travel with the CRCs because a row whose CRC32 is 0
// contributes nothing to the BIT_XOR, so its absence is invisible to the
// checksum and visible only to the count.
type chunkRead struct {
	srcCRC   int64
	tgtCRC   int64
	srcCount uint64
	tgtCount uint64
}

func (r chunkRead) mismatched() bool {
	return compareChunk(r.srcCRC, r.tgtCRC, r.srcCount, r.tgtCount).mismatched()
}

// sameSource reports whether two observations saw the same source bytes.
func (r chunkRead) sameSource(other chunkRead) bool {
	return r.srcCRC == other.srcCRC && r.srcCount == other.srcCount
}

// targetMatches reports whether this observation's TARGET equals the SOURCE of
// `witnessed` — i.e. the target has caught up to a version of the source we
// have actually seen. Matching against a witnessed source (rather than only
// the current one) is what lets a chunk pass when the source has since moved
// on again.
func (r chunkRead) targetMatches(witnessed chunkRead) bool {
	return r.tgtCRC == witnessed.srcCRC && r.tgtCount == witnessed.srcCount
}

// pendingChunk is a chunk whose first read mismatched and which is awaiting
// re-read. `first` is the most recent source observation it is being judged
// against, and is replaced when the source moves.
type pendingChunk struct {
	chunk      *table.Chunk
	first      chunkRead
	notBefore  time.Time
	srcChanged int
}

// chunkChecksumSQL builds the paired source/target checksum queries for a
// chunk. Shared by the walk and the drain so the two can never drift into
// comparing different expressions — which would make a re-read disagree with
// the read that deferred it for reasons that have nothing to do with the data.
func chunkChecksumSQL(chunk *table.Chunk) (source, target string, err error) {
	sourceChecksumCols, targetChecksumCols, err := chunk.ColumnMapping.ChecksumExprs()
	if err != nil {
		return "", "", err
	}
	const tpl = "SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, count(*) as c FROM %s WHERE %s"
	source = fmt.Sprintf(tpl, sourceChecksumCols, chunk.Table.QuotedTableName, chunk.String())
	target = fmt.Sprintf(tpl, targetChecksumCols, chunk.NewTable.QuotedTableName, chunk.String())
	return source, target, nil
}

// readChunkIn reads both sides of a chunk inside an existing transaction.
func readChunkIn(ctx context.Context, trx *sql.Tx, chunk *table.Chunk) (chunkRead, error) {
	source, target, err := chunkChecksumSQL(chunk)
	if err != nil {
		return chunkRead{}, err
	}
	var r chunkRead
	if err := trx.QueryRowContext(ctx, source).Scan(&r.srcCRC, &r.srcCount); err != nil {
		return chunkRead{}, err
	}
	if err := trx.QueryRowContext(ctx, target).Scan(&r.tgtCRC, &r.tgtCount); err != nil {
		return chunkRead{}, err
	}
	return r, nil
}

// withFreshSnapshot runs fn against a transaction whose read view is created
// now, so source and target are still compared at one instant but that instant
// is the present rather than the walk's frozen one.
func (c *SingleChecker) withFreshSnapshot(ctx context.Context, fn func(trx *sql.Tx) error) error {
	pool, err := dbconn.NewTrxPool(ctx, c.db, 1, c.dbConfig)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := pool.Close(); cerr != nil {
			c.logger.Debug("closing retry snapshot", "error", cerr)
		}
	}()
	trx, err := pool.Get()
	if err != nil {
		return err
	}
	// Put it BACK before Close. Get() removes the transaction from the pool's
	// slice, and Close() only rolls back what the slice still holds -- so
	// without this the transaction is never rolled back. It would sit there
	// holding a SHARED_READ metadata lock on both tables and an open
	// REPEATABLE READ view, which is the InnoDB history-list growth the yield
	// mechanism exists to avoid, and the next pass would block on its table
	// lock until the 30s timeout and then KILL this connection to proceed.
	defer pool.Put(trx)
	return fn(trx)
}

// freshReadChunk is the production implementation of the re-read seam.
func (c *SingleChecker) freshReadChunk(ctx context.Context, chunk *table.Chunk) (chunkRead, error) {
	var out chunkRead
	err := c.withFreshSnapshot(ctx, func(trx *sql.Tx) error {
		r, err := readChunkIn(ctx, trx, chunk)
		out = r
		return err
	})
	return out, err
}

// deferMismatch records a chunk for re-read instead of counting it.
func (c *SingleChecker) deferMismatch(chunk *table.Chunk, first chunkRead) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	c.pending = append(c.pending, &pendingChunk{
		chunk:     chunk,
		first:     first,
		notBefore: time.Now().Add(c.retryDelay),
	})
}

// resetPending drops anything deferred by a previous attempt. Called where
// differencesFound is reset, so an attempt never inherits the last one's queue.
func (c *SingleChecker) resetPending() {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	c.pending = nil
}

func (c *SingleChecker) pendingDepth() int {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	return len(c.pending)
}

// waitUntil sleeps until t, or returns ctx's error if it is cancelled first.
func waitUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// resolvePending drains the deferred queue, counting only what survives a
// re-read. Called once a pass has walked every chunk.
//
// Returns an error only for a genuine failure to adjudicate (a read failed,
// the context was cancelled) or, when fixDifferences is off, for a confirmed
// divergence — matching what the walk used to return inline.
func (c *SingleChecker) resolvePending(ctx context.Context) error {
	suspects, err := c.rereadPending(ctx)
	if err != nil {
		return err
	}
	return c.confirmSuspects(ctx, suspects)
}

// rereadPending is the first drain phase: wait out each deferred chunk's
// delay, re-read it, and dismiss the ones that reconciled. Chunks whose
// SOURCE moved during the window never got a stable comparison, so they are
// re-queued rather than judged -- up to maxSrcChangedCycles, after which they
// are passed on to be adjudicated like any other suspect.
//
// Returns the chunks that still look wrong, carrying their latest read.
func (c *SingleChecker) rereadPending(ctx context.Context) ([]*pendingChunk, error) {
	var suspects []*pendingChunk
	for {
		c.pendingMu.Lock()
		if len(c.pending) == 0 {
			c.pendingMu.Unlock()
			return suspects, nil
		}
		p := c.pending[0]
		c.pending = c.pending[1:]
		c.pendingMu.Unlock()

		if err := waitUntil(ctx, p.notBefore); err != nil {
			return nil, err
		}

		reread, err := c.freshRead(ctx, p.chunk)
		if err != nil {
			return nil, err
		}

		// The target caught up to a source version we witnessed, or the two
		// sides simply agree now. Either way this chunk was apply lag.
		if reread.targetMatches(p.first) || !reread.mismatched() {
			c.lagResolved.Add(1)
			c.logger.Info("chunk reconciled on re-read; was apply lag, not a difference",
				"chunk", p.chunk.String())
			continue
		}

		// The source moved while we waited, so we never got a stable window to
		// judge in. Re-baseline against what we just saw and look again.
		if !reread.sameSource(p.first) && p.srcChanged < c.maxSrcChangedCycles {
			p.srcChanged++
			p.first = reread
			p.notBefore = time.Now().Add(c.retryDelay)
			c.pendingMu.Lock()
			c.pending = append(c.pending, p)
			c.pendingMu.Unlock()
			c.logger.Debug("chunk still changing on the source; re-queued",
				"chunk", p.chunk.String(), "cycles", p.srcChanged)
			continue
		}

		p.first = reread
		suspects = append(suspects, p)
	}
}

// confirmSuspects is the second drain phase: give the change feed one last
// chance to explain every remaining suspect, then count what survives.
//
// The flush is done ONCE for the whole set rather than per chunk. Flushing is
// not a per-chunk operation -- it pushes the whole buffered change set through
// -- so doing it inside the loop would repeat the same global work for every
// suspect, which on a pass that deferred many chunks is the difference
// between one drain and hundreds.
func (c *SingleChecker) confirmSuspects(ctx context.Context, suspects []*pendingChunk) error {
	if len(suspects) == 0 {
		return nil
	}
	// Only flush if the feed is actually holding anything. Flush is a global
	// drain that loops on BlockWait until the change set is trivial, so on a
	// feed with nothing buffered it is pure cost -- it waits on binlog traffic
	// that may not be coming -- and it cannot change what the target holds,
	// which is the only reason we would want it here.
	if c.feed.AllChangesFlushed() {
		c.logger.Info("change feed has nothing buffered; judging chunks that still disagree as they stand",
			"suspects", len(suspects))
	} else {
		c.logger.Info("flushing the change feed before judging chunks that still disagree",
			"suspects", len(suspects))
		if err := c.feed.Flush(ctx); err != nil {
			return err
		}
	}
	for _, p := range suspects {
		confirmed, err := c.freshRead(ctx, p.chunk)
		if err != nil {
			return err
		}
		if !confirmed.mismatched() {
			c.lagResolved.Add(1)
			c.logger.Info("chunk reconciled after flushing the change feed",
				"chunk", p.chunk.String())
			continue
		}
		if err := c.recordDifference(ctx, p.chunk, confirmed); err != nil {
			return err
		}
	}
	return nil
}

// recordDifference counts a confirmed difference and repairs it when allowed.
func (c *SingleChecker) recordDifference(ctx context.Context, chunk *table.Chunk, r chunkRead) error {
	mismatch := compareChunk(r.srcCRC, r.tgtCRC, r.srcCount, r.tgtCount)
	c.differencesFound.Add(1)
	c.logger.Warn("chunk verification failed",
		"chunk", chunk.String(),
		"reason", mismatch.reason(r.srcCount, r.tgtCount),
		"sourceChecksum", r.srcCRC, "targetChecksum", r.tgtCRC,
		"sourceCount", r.srcCount, "targetCount", r.tgtCount)

	// Row-level diagnostics. Best-effort: this is what an operator reads to
	// find out WHICH rows disagreed, and losing it must not change the
	// verdict we already reached above.
	if ierr := c.withFreshSnapshot(ctx, func(trx *sql.Tx) error {
		return c.inspectDifferences(ctx, trx, chunk)
	}); ierr != nil {
		c.logger.Warn("could not inspect the differing chunk", "chunk", chunk.String(), "error", ierr)
	}

	if !c.fixDifferences {
		return errors.New("checksum mismatch")
	}
	return c.replaceChunk(ctx, chunk)
}
