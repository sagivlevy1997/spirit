package check

import (
	"bytes"
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/stretchr/testify/require"
)

func TestReplicaPrivileges(t *testing.T) {
	// use an actual replica
	replicaDSN := os.Getenv("REPLICA_DSN")
	if replicaDSN == "" {
		t.Skip("skipping test because REPLICA_DSN not set")
	}
	r := Resources{
		Table:     &table.TableInfo{TableName: "test"},
		Statement: statement.MustNew("ALTER TABLE test RENAME TO newtablename")[0],
	}
	err := replicaPrivilegeCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // if no replicas, it returns no error.

	replicaDB, err := sql.Open("mysql", replicaDSN)
	require.NoError(t, err) // no error
	r.Replicas = []*sql.DB{replicaDB}
	err = replicaPrivilegeCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // user has privileges
}

// An unreadable replica is a warning, not an error. Called directly: in a
// migration preflight runs before replicas are opened, so going through the
// Runner would pass on the unfixed code too.
func TestReplicaPrivilegeCheck_NeverBlocks(t *testing.T) {
	db, err := sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/test?timeout=200ms")
	require.NoError(t, err)
	defer db.Close()
	var logs bytes.Buffer
	r := Resources{Replicas: []*sql.DB{db}, ReplicaHosts: []string{"db-x:3306"}}
	err = replicaPrivilegeCheck(t.Context(), r, slog.New(slog.NewTextHandler(&logs, nil)))
	require.NoError(t, err)
	require.Contains(t, logs.String(), throttler.ReplicaSetAsideMsg)
	require.Contains(t, logs.String(), "host=db-x:3306")
}
