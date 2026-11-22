package check

import (
	"bytes"
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

func TestReplicaHealth(t *testing.T) {
	r := Resources{
		Table:     &table.TableInfo{TableName: "test"},
		Statement: statement.MustNew("ALTER TABLE test RENAME TO newtablename")[0],
	}
	err := replicaHealth(t.Context(), r, slog.Default())
	require.NoError(t, err) // if no replicas, it returns no error.

	// use a non-replica. this will return an error identifying which thread
	// is not running and on which host.
	nonReplica, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	r.Replicas = []*sql.DB{nonReplica}
	err = replicaHealth(t.Context(), r, slog.Default())
	require.NoError(t, err) // set aside with a warning, never fatal

	// use an actual replica
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping test because REPLICA_DSN not set")
	}
	replicaDB, err := sql.Open("mysql", replicaDSN)
	require.NoError(t, err)
	r.Replicas = []*sql.DB{replicaDB}
	err = replicaHealth(t.Context(), r, slog.Default())
	require.NoError(t, err) // all looks good of course.

	// use a completely invalid DSN.
	// golang sql.Open lazy loads, so this is possible.
	invalidDB, err := sql.Open("mysql", "msandbox:msandbox@tcp(127.0.0.1:22)/test")
	require.NoError(t, err)
	r.Replicas = []*sql.DB{invalidDB}
	err = replicaHealth(t.Context(), r, slog.Default())
	require.NoError(t, err) // set aside with a warning, never fatal
}

// A broken replica never stops the run at PostSetup or at cutover: it is
// named in one warning and skipped. Needs no MySQL.
func TestReplicaHealth_NeverBlocks(t *testing.T) {
	db, err := sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/test?timeout=200ms")
	require.NoError(t, err)
	defer db.Close()
	var logs bytes.Buffer
	r := Resources{Replicas: []*sql.DB{db}, ReplicaHosts: []string{"db-x:3306"}}
	err = replicaHealth(t.Context(), r, slog.New(slog.NewTextHandler(&logs, nil)))
	require.NoError(t, err)
	require.Contains(t, logs.String(), throttler.ReplicaSetAsideMsg)
	require.Contains(t, logs.String(), "host=db-x:3306")
	require.NotContains(t, logs.String(), "u:p@")
}
