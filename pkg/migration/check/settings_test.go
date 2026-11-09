package check

import (
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

func TestSettings(t *testing.T) {
	r := Resources{
		Table:           &table.TableInfo{TableName: "test", SchemaName: "test"},
		Threads:         2,
		TargetChunkTime: time.Second * 5,
		ReplicaMaxLag:   time.Hour,
	}

	// 0 means "use default". This is a bit of a hack,
	// but the test suite depends on it.
	if r.TargetChunkTime == 0 {
		r.TargetChunkTime = time.Millisecond * 500
	}
	if r.ReplicaMaxLag == 0 {
		r.ReplicaMaxLag = time.Second * 120
	}

	err := settingsCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // all looks good

	r.Threads = 0
	err = settingsCheck(t.Context(), r, slog.Default())
	require.Error(t, err)

	r.Threads = 65
	err = settingsCheck(t.Context(), r, slog.Default())
	require.Error(t, err)

	r.Threads = 2
	err = settingsCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // all looks good

	r.TargetChunkTime = time.Second * 6
	err = settingsCheck(t.Context(), r, slog.Default())
	require.Error(t, err)

	r.TargetChunkTime = time.Second * 4
	r.ReplicaMaxLag = time.Hour*4 + time.Second
	err = settingsCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
}

// gh-ost throttles at 3s of replica lag (--max-lag-millis=3000), and Spirit
// running beside it on the same clusters has to be able to hold the same
// line. The floor used to be 10s, so a 3s limit died at launch.
func TestSettingsReplicaMaxLagFloor(t *testing.T) {
	r := Resources{
		Table:           &table.TableInfo{TableName: "test", SchemaName: "test"},
		Threads:         2,
		TargetChunkTime: time.Second,
	}

	for _, lag := range []time.Duration{time.Second, 3 * time.Second, 5 * time.Second} {
		r.ReplicaMaxLag = lag
		require.NoError(t, settingsCheck(t.Context(), r, slog.Default()), "replica-max-lag=%s", lag)
	}

	r.ReplicaMaxLag = 999 * time.Millisecond
	require.ErrorContains(t, settingsCheck(t.Context(), r, slog.Default()), "1s-4hr")
}
