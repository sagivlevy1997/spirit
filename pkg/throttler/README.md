# Throttlers

Throttlers are designed to limit the rate of changes pushed through the copier, helping maintain the health of the system during migrations.

## Design Philosophy

Throttlers were designed as an **interface from the start**, because it was always believed there would be different use cases for when a schema change should be throttled. The original vision included:

- **Replication lag throttler**: Monitors replica lag and pauses when replicas fall behind
- **HTTP service integration** (e.g., [Freno](https://github.com/github/freno) or [Doorman](https://github.com/youtube/doorman)): Allows schema changes and other background tasks to coordinate through a centralized throttling service
- **Custom implementations**: The interface allows users to implement their own throttling strategies, such as reducing migration activity during stock trading hours or at least market open

## Current State

In practice, throttlers haven't been used as extensively as originally envisioned. Because Spirit is primarily used with Aurora at Block, the replica throttler sees limited internal use. We have also found that by using Dynamic Chunking in the copier, schema changes actually self-throttle pretty well without a throttler.

However, it remains available and maintained for community use, particularly for users running traditional MySQL replication topologies. We are open to contributions to throttler improvements, such as being able to throttle on multiple replicas at once ([issue #220](https://github.com/block/spirit/issues/220)).

Two **Aurora-specific throttlers** were added later: an Aurora threads throttler ([#831](https://github.com/block/spirit/issues/831)) and a commit-latency throttler ([#468](https://github.com/block/spirit/issues/468)). On Aurora the threads throttler is **always enabled**, while commit-latency is enabled **by default** but gated on a positive `--max-commit-latency` (default `100ms`; set `--max-commit-latency=0` to disable it). These are the throttlers most Block migrations actually run, and they double as the continuous load signal that drives the copier's experimental write-thread autoscaler (see [`GradualThrottler`](#gradualthrottler-optional-extension) below).

## Interface

All throttlers implement the `Throttler` interface:

```go
type Throttler interface {
    Open(ctx context.Context) error
    Close() error
    IsThrottled() bool
    BlockWait(ctx context.Context)
    UpdateLag(ctx context.Context) error
}
```

### `GradualThrottler` (optional extension)

Throttlers whose underlying signal is continuous — not just a binary stop/go — may additionally implement `GradualThrottler`:

```go
type GradualThrottler interface {
    Throttler
    // Utilization reports current load relative to this throttler's throttle
    // point: 0 = idle, 1.0 = exactly where IsThrottled() flips true, >1.0 = over.
    Utilization() float64
}
```

The copier's write-thread autoscaler type-asserts for this and only engages when it is present. The two Aurora throttlers implement it; the replication-lag throttler deliberately does **not** — lag is an SLO-style budget, not a load gauge, so steering on it would park replicas behind. Binary-signal throttlers protect only via the `IsThrottled()` / `BlockWait()` hard-stop.

## Implementations

### Noop Throttler

The default throttler that performs no throttling. Used when throttling is not required.

```go
throttler := &throttler.Noop{}
```

### Mock Throttler

A throttler used internally by the test suite to help reduce race conditions when running migration tests across different types of hardware. It injects 1 second of sleep every time `BlockWait()` is called.

### Replication Throttler

Monitors replication lag on MySQL 8.0+ replicas using `performance_schema` metrics. This provides more accurate lag measurements than the traditional `SHOW SLAVE STATUS` approach.

```go
throttler, err := throttler.NewReplicationThrottler(
    replicaDB,
    120*time.Second,  // lag tolerance
    logger,
)
```

**Features:**
- Uses `performance_schema` for accurate lag calculation
- Monitors both applier latency and queue latency
- Automatically detects idle replicas to avoid false positives
- Checks lag every second, so budgets down to 1s are measured
- Blocks copy operations when lag exceeds tolerance (default: up to 60 seconds per check)
- **Fails closed** when lag becomes unobservable: if lag polling keeps failing (e.g. the replica is unreachable) for more than 15 seconds, copying pauses until polling recovers, rather than proceeding at full speed against a lag budget nobody is measuring. Remove the replica DSN to proceed without lag protection.

### Aurora Commit-Latency Throttler

```go
throttler, err := throttler.NewCommitLatencyThrottler(
    db,
    100*time.Millisecond,  // latency threshold
    logger,
)
```

Polls Aurora's cumulative commit counters (`AuroraDb_commits` and `AuroraDb_commit_latency`, from `performance_schema.global_status`) every 5 seconds and throttles when the **window-averaged** commit latency reaches the threshold. Watching commit latency lets it react to storage-layer saturation directly. It implements `GradualThrottler`, reporting `Utilization()` as `avg_latency / threshold`. Aurora-only; see [issue #468](https://github.com/block/spirit/issues/468).

### Aurora Threads Throttler

Assembled internally by `throttler.AuroraSetup.Build`, not constructed directly: the sampling mode is chosen by a privilege probe rather than supplied by the caller (see below).

Polls a running-thread count every 5 seconds and compares it to the instance vCPU count (read from `@@innodb_buffer_pool_instances`, which Aurora pins to the vCPU count). `Utilization()` reports an EWMA-smoothed `count / vCPUs`, so the autoscaler tracks sustained load rather than instantaneous spikes. It runs in one of two modes, chosen once at setup by a privilege probe (`CanReadRedoAwareThreads`):

- **redo-aware** (preferred) — counts `Query`-state threads from `performance_schema.threads`, **subtracting** those parked on redo-log flush (via `events_waits_current`). Those waiters are IO-bound and Aurora group-commits them to storage, so excluding them lets the copy safely oversubscribe the redo log — a staging A/B measured ~18% more copy throughput ([#971](https://github.com/block/spirit/issues/971)). Requires `SELECT` on `performance_schema.threads` and `performance_schema.events_waits_current`. The query excludes its own sampling connection, so the hard-stop trips when the raw count exceeds `vCPUs + 1`.
- **Threads_running** (fallback) — counts all running threads via the `Threads_running` status variable from `global_status`; needs no grant beyond what `IsAurora` already proves. Redo-log waiters count as load here, so it is more conservative (caps concurrency near vCPUs). The hard-stop trips when the raw count exceeds `vCPUs + 2` — a small headroom for Spirit's own monitoring connections, which this mode cannot exclude.

Aurora-only; see [issue #831](https://github.com/block/spirit/issues/831).

Both throttlers require an `IsAurora()` probe — which confirms `performance_schema.global_status` is readable and the Aurora status variables are present. When it succeeds, `throttler.AuroraSetup.Build` assembles them: the Aurora threads throttler is **always** built (its mode chosen by the perf-schema probe above), while commit-latency is added only when `CommitLatencyThreshold > 0` (wired from `--max-commit-latency`). Whichever are enabled run together, and the highest utilization across them drives the autoscaler.

## Usage

Throttlers are integrated into the copier and automatically pause chunk copying when the system is under stress:

```go
copier := &copier.Unbuffered{
    Throttler: throttler,
    // ... other config
}
```

During migration, the copier calls `throttler.BlockWait(ctx)` before each chunk, pausing operations if `IsThrottled()` returns true.

## Extending

To implement a custom throttler (e.g., for Freno integration):

1. Implement the `Throttler` interface
2. Start any background monitoring in `Open()`
3. Update throttling state based on your metrics
4. Return the current state in `IsThrottled()`
5. Block appropriately in `BlockWait()` with context support

Example structure:

```go
type CustomThrottler struct {
    isThrottled atomic.Bool
    // ... your fields
}

func (t *CustomThrottler) Open(ctx context.Context) error {
    // Start monitoring
    go t.monitor(ctx)
    return nil
}

func (t *CustomThrottler) IsThrottled() bool {
    return t.isThrottled.Load()
}

func (t *CustomThrottler) BlockWait(ctx context.Context) {
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()
    
    for t.IsThrottled() {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            // Continue checking
        }
    }
}
```

## See Also

- [MySQL 8.0 Replication Lag Implementation](https://github.com/block/spirit/issues/286)
- [Freno - Throttling Service](https://github.com/github/freno)
- [Doorman - Global Distributed Client Side Rate Limiting](https://github.com/youtube/doorman)
