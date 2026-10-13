package migration

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

// onlyDifferences is a checker from before UnverifiedDifferences existed, and
// stands in for the two that still are: ContinuousChecker and every mock of
// the Checker interface.
type onlyDifferences struct{ differences atomic.Uint64 }

func (c *onlyDifferences) DifferencesFound() uint64 { return c.differences.Load() }

// bothCounters is SingleChecker's shape: it separates "this pass repaired
// something" from "something it repaired is still unconfirmed".
type bothCounters struct {
	differences atomic.Uint64
	unverified  atomic.Uint64
}

func (c *bothCounters) DifferencesFound() uint64      { return c.differences.Load() }
func (c *bothCounters) UnverifiedDifferences() uint64 { return c.unverified.Load() }

func TestACheckerThatCannotAnswerKeepsTheOldRule(t *testing.T) {
	// The fallback has to be DifferencesFound, not zero. Zero would let a
	// checker that knows nothing about re-reads persist a watermark covering
	// chunks it never verified -- the exact thing the invariant forbids, and
	// a silent downgrade for every checker that is not SingleChecker.
	checker := &onlyDifferences{}
	assert.Equal(t, uint64(0), unverifiedRepairs(checker))

	checker.differences.Store(3)
	assert.Equal(t, uint64(3), unverifiedRepairs(checker),
		"a checker that cannot answer must fall back to the old, stricter rule")
}

func TestARepairThatWasReconfirmedNoLongerBlocksTheWatermark(t *testing.T) {
	// The whole point. A pass that repaired a chunk and then re-read it clean
	// has met the invariant's own test, so the watermark may describe it --
	// while DifferencesFound stays non-zero, so the pass still runs again.
	checker := &bothCounters{}
	checker.differences.Store(1)
	checker.unverified.Store(0)

	assert.Equal(t, uint64(0), unverifiedRepairs(checker),
		"a confirmed repair still blocked the watermark, so every resume restarts at row one")
	assert.Equal(t, uint64(1), checker.DifferencesFound(),
		"the pass must still count as unclean and run again")
}

func TestARepairThatWasNotReconfirmedStillBlocksIt(t *testing.T) {
	checker := &bothCounters{}
	checker.differences.Store(1)
	checker.unverified.Store(1)

	assert.Equal(t, uint64(1), unverifiedRepairs(checker))
}
