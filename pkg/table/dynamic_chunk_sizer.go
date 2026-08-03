package table

import "time"

// dynamicChunkSizer holds the time-based chunk-sizing state shared by the
// optimistic and composite chunkers. The chunker target is "spend
// roughly ChunkerTarget per chunk"; chunkTimingInfo accumulates per-chunk
// durations, and the next chunk's row count is derived from the p90 of
// the history vs the target.
//
// Embed into a chunker struct so call sites can continue to read fields
// as t.chunkSize / t.chunkTimingInfo / t.ChunkerTarget without changing.
type dynamicChunkSizer struct {
	chunkSize             uint64
	chunkTimingInfo       []time.Duration
	ChunkerTarget         time.Duration // e.g. 500ms target per chunk
	disableDynamicChunker bool          // only used by the test suite
	fixedChunkSize        uint64        // when >0, pin chunkSize to this many rows (dynamic sizing disabled)
	minChunkSize          uint64        // composite-only lower clamp; 0 falls back to MinDynamicRowSize
	chunkObservations     []chunkObservation // (rows,duration) history for the overhead-aware sizer
}

// chunkObservation records one completed chunk's copied row count and the
// wall time it took. The overhead-aware sizer uses these to separate the
// fixed per-chunk cost from the marginal per-row cost.
type chunkObservation struct {
	rows uint64
	d    time.Duration
}

// initialChunkSize returns the chunk size used to (re)initialize a chunker:
// the pinned fixedChunkSize when set, otherwise the default StartingChunkSize.
func (d *dynamicChunkSizer) initialChunkSize() uint64 {
	if d.fixedChunkSize > 0 {
		return d.fixedChunkSize
	}
	return StartingChunkSize
}

// updateChunkerTarget applies a recalculated row target after clamping
// it to safe bounds (no more than 1.5x growth per step, capped at
// MaxDynamicRowSize, floored at MinDynamicRowSize). Resets the timing
// history so the next p90 reflects the new chunk size. Caller must hold
// the chunker's mutex.
func (d *dynamicChunkSizer) updateChunkerTarget(newTarget uint64) {
	d.chunkSize = d.boundaryCheckTargetChunkSize(newTarget)
	d.chunkTimingInfo = []time.Duration{}
	d.chunkObservations = nil
}

// boundaryCheckTargetChunkSize clamps a proposed row count to the
// dynamic-chunking bounds. Extracted so tests and the prefetch-switch
// path in the optimistic chunker can verify the same clamping logic.
func (d *dynamicChunkSizer) boundaryCheckTargetChunkSize(newTarget uint64) uint64 {
	newTargetRows := float64(newTarget)

	// Cap growth at 1.5x per step. Prior chunks may have had "gaps" that
	// made them complete faster than expected; we don't want a single
	// fast chunk to balloon the next one.
	if newTargetRows > float64(d.chunkSize)*MaxDynamicStepFactor {
		newTargetRows = float64(d.chunkSize) * MaxDynamicStepFactor
	}

	if newTargetRows > MaxDynamicRowSize {
		newTargetRows = MaxDynamicRowSize
	}
	floor := float64(MinDynamicRowSize)
	if d.minChunkSize > MinDynamicRowSize {
		floor = float64(d.minChunkSize)
	}
	if newTargetRows < floor {
		newTargetRows = floor
	}
	return uint64(newTargetRows)
}

// calculateNewTargetChunkSize returns the row target derived from the
// p90 of the chunkTimingInfo history vs ChunkerTarget, plus the raw p90
// so a caller can react to extreme cases (the optimistic chunker uses
// the p90 to decide whether to switch to prefetch mode). Caller must
// hold the chunker's mutex.
func (d *dynamicChunkSizer) calculateNewTargetChunkSize() (newTargetRows uint64, p90 time.Duration) {
	p90 = LazyFindP90(d.chunkTimingInfo)
	target := float64(d.ChunkerTarget)
	rows := float64(d.chunkSize) * (target / float64(p90))
	return uint64(rows), p90
}

// calculateOverheadAwareTargetChunkSize derives the next chunk size from a
// linear model of per-chunk processing time:
//
//	d ≈ fixed + rows*marginal
//
// fitted by least squares over the recent (rows,d) observations, returning the
// row count whose predicted time equals ChunkerTarget. This replaces the naive
// time/rows ratio for composite / non-int PK tables, where a large fixed
// per-chunk cost (boundary seek + network round-trip) is independent of row
// count. Under the naive model that fixed cost is misattributed to the rows,
// so the controller shrinks — which only spreads the same fixed cost over fewer
// rows and spirals to the floor. Here, when the fixed cost alone meets or
// exceeds the target, shrinking cannot help, so we grow to amortize it.
// Caller must hold the chunker's mutex.
func (d *dynamicChunkSizer) calculateOverheadAwareTargetChunkSize() uint64 {
	n := len(d.chunkObservations)
	if n < 2 {
		return d.chunkSize // not enough data yet; hold steady.
	}
	var sx, sy, sxx, sxy float64
	minRows, maxRows := d.chunkObservations[0].rows, d.chunkObservations[0].rows
	for _, o := range d.chunkObservations {
		x := float64(o.rows)
		y := float64(o.d)
		sx += x
		sy += y
		sxx += x * x
		sxy += x * y
		if o.rows < minRows {
			minRows = o.rows
		}
		if o.rows > maxRows {
			maxRows = o.rows
		}
	}
	nf := float64(n)
	denom := nf*sxx - sx*sx
	// Without spread in the row counts we cannot identify the slope; rather
	// than thrash on noise, keep the current size (still clamped elsewhere to
	// [minChunkSize, MaxDynamicRowSize]).
	if maxRows == minRows || denom == 0 {
		return d.chunkSize
	}
	marginal := (nf*sxy - sx*sy) / denom // ns per row
	fixed := (sy - marginal*sx) / nf     // fixed per-chunk ns
	target := float64(d.ChunkerTarget)
	// Non-positive marginal means time is not growing with rows over this
	// window: the fixed cost dominates. Amortize by growing to the ceiling
	// (the 1.5x step cap keeps the approach gradual).
	if marginal <= 0 {
		return MaxDynamicRowSize
	}
	budget := target - fixed // time left for copying rows after the fixed cost
	if budget <= 0 {
		// Even a single-row chunk cannot beat the target because the fixed
		// per-chunk cost already meets/exceeds it. Shrinking is strictly worse
		// (same fixed cost, fewer rows), so grow to amortize.
		return MaxDynamicRowSize
	}
	return uint64(budget / marginal)
}
