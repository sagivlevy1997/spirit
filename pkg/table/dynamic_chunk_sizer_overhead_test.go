package table

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// obs is a small helper to build a (rows,duration) observation history that
// lies exactly on the line d = fixed + rows*marginal, so the least-squares fit
// recovers fixed/marginal deterministically.
func obs(fixed time.Duration, marginalPerRow time.Duration, rowCounts ...uint64) []chunkObservation {
	out := make([]chunkObservation, 0, len(rowCounts))
	for _, r := range rowCounts {
		out = append(out, chunkObservation{rows: r, d: fixed + time.Duration(r)*marginalPerRow})
	}
	return out
}

// TestMinChunkSizeFloor verifies the composite-only floor clamp: a proposed
// target below minChunkSize is raised to minChunkSize, while the global
// MinDynamicRowSize still applies when minChunkSize is unset.
func TestMinChunkSizeFloor(t *testing.T) {
	d := &dynamicChunkSizer{chunkSize: 1000, minChunkSize: StartingChunkSize}
	require.Equal(t, uint64(StartingChunkSize), d.boundaryCheckTargetChunkSize(10),
		"target below minChunkSize must floor to minChunkSize, not MinDynamicRowSize")

	d.minChunkSize = 0
	require.Equal(t, uint64(MinDynamicRowSize), d.boundaryCheckTargetChunkSize(0),
		"with no minChunkSize the global MinDynamicRowSize floor applies")
}

// TestOverheadAwareNormalSolve verifies the linear-model solve: with a modest
// fixed cost and positive marginal, the target row count is (target-fixed)/marginal.
func TestOverheadAwareNormalSolve(t *testing.T) {
	d := &dynamicChunkSizer{
		chunkSize:     1000,
		ChunkerTarget: 500 * time.Millisecond,
		// fixed=100ms, marginal=1ms/row -> budget=400ms -> 400 rows.
		chunkObservations: obs(100*time.Millisecond, time.Millisecond, 100, 200, 400, 800, 1000),
	}
	got := d.calculateOverheadAwareTargetChunkSize()
	require.InDelta(t, 400, float64(got), 2,
		"rows should solve to (target-fixed)/marginal = 400")
}

// TestOverheadAwareGrowsWhenFixedDominates verifies that when per-chunk time is
// effectively independent of row count (marginal <= 0), we grow to the ceiling
// to amortize the fixed cost instead of shrinking.
func TestOverheadAwareGrowsWhenFixedDominates(t *testing.T) {
	d := &dynamicChunkSizer{
		chunkSize:     1000,
		ChunkerTarget: 2 * time.Second,
		// constant ~2s regardless of rows -> slope 0.
		chunkObservations: obs(2*time.Second, 0, 10, 50, 100, 500, 1000),
	}
	require.Equal(t, uint64(MaxDynamicRowSize), d.calculateOverheadAwareTargetChunkSize(),
		"flat time vs rows must grow to the ceiling, never shrink")
}

// TestOverheadAwareFixedExceedsTarget verifies that when the fixed per-chunk
// cost alone exceeds the target, we grow (not shrink), because shrinking would
// pay the same fixed cost over fewer rows.
func TestOverheadAwareFixedExceedsTarget(t *testing.T) {
	d := &dynamicChunkSizer{
		chunkSize:     1000,
		ChunkerTarget: 2 * time.Second,
		// fixed=3s (> 2s target), tiny positive marginal.
		chunkObservations: obs(3*time.Second, 100*time.Microsecond, 100, 200, 400, 800, 1000),
	}
	require.Equal(t, uint64(MaxDynamicRowSize), d.calculateOverheadAwareTargetChunkSize(),
		"fixed cost above target must grow to amortize, not spiral down")
}

// TestOverheadAwareNoSpreadHolds verifies that without variation in row counts
// the slope is unidentifiable, so the sizer holds the current size rather than
// reacting to noise.
func TestOverheadAwareNoSpreadHolds(t *testing.T) {
	d := &dynamicChunkSizer{
		chunkSize:         1000,
		ChunkerTarget:     500 * time.Millisecond,
		chunkObservations: obs(100*time.Millisecond, time.Millisecond, 1000, 1000, 1000, 1000),
	}
	require.Equal(t, uint64(1000), d.calculateOverheadAwareTargetChunkSize(),
		"no spread in row counts -> hold current chunkSize")
}
