package check

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/throttler"
)

// ErrReplicaNotHealthy was returned when a replica's IO or SQL thread was not
// running. The check no longer fails on replica state (see replicaHealth);
// the error stays exported for callers that still match on it.
var ErrReplicaNotHealthy = fmt.Errorf("replica is not healthy")

// ReplicaIgnoredByCheckMsg is what a replica check logs for a broken replica.
// Deliberately NOT throttler.ReplicaSetAsideMsg: the lag throttler is the one
// source of set-aside state (it pairs every set-aside with a working-again
// line), so a blip seen by a one-shot check cannot leave a healthy replica
// reported as set aside for the rest of the engine's life.
const ReplicaIgnoredByCheckMsg = "replica not healthy; ignored by this check"

func init() {
	registerCheck("replicahealth", replicaHealth, ScopePostSetup|ScopeCutover)
}

// replicaHealth reports every replica whose replication is not running, or
// that cannot be asked, with one ReplicaIgnoredByCheckMsg warning naming it --
// and never returns an error for it. Each replica is bounded by
// throttler.ReplicaQueryTimeout, so a black-holed one cannot stall the
// cutover that runs right after this check.
//
// It used to return the first broken replica's error, which made the engine
// exit at PostSetup or, worse, at cutover after hours of copying: one Wix run
// died on a stopped SQL thread, another on a replica deleted while it waited
// at the sentinel. Replica state is the operator's information; the cutover
// on the primary does not depend on it, and the lag throttler already sets a
// broken replica aside.
func replicaHealth(ctx context.Context, r Resources, logger *slog.Logger) error {
	for i, replica := range r.Replicas {
		qctx, cancel := context.WithTimeout(ctx, throttler.ReplicaQueryTimeout)
		running, why, err := throttler.ReplicationRunning(qctx, replica)
		cancel()
		if err != nil {
			why = err.Error()
		}
		if err != nil || !running {
			logger.Warn(ReplicaIgnoredByCheckMsg, "host", replicaHostAt(r, i), "reason", why)
		}
	}
	return nil
}

// replicaHostAt names replica i by host:port, or "unknown" when the caller
// supplied no host list. Never queries the replica: the hosts that need
// naming are exactly the ones that cannot answer.
func replicaHostAt(r Resources, i int) string {
	if i < len(r.ReplicaHosts) && r.ReplicaHosts[i] != "" {
		return r.ReplicaHosts[i]
	}
	return "unknown"
}
