//go:build singleversion

package migration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TestResumePrimaryKeyDirectionChange: a resume reads the high watermark
// back out of the _new table, which is the one whose key changed.
func TestResumePrimaryKeyDirectionChange(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "pkdir_resume", `CREATE TABLE pkdir_resume (
		metasite_id int NOT NULL,
		version int NOT NULL,
		payload varchar(64) NOT NULL,
		PRIMARY KEY (metasite_id, version)
	)`)
	for site := 0; site < 400; site += 40 {
		var rows []string
		for s := site; s < site+40; s++ {
			for v := range 50 {
				rows = append(rows, fmt.Sprintf("(%d, %d, 'seed_%d_%d')", s, v, s, v))
			}
		}
		testutils.RunSQL(t, "INSERT INTO pkdir_resume VALUES "+strings.Join(rows, ","))
	}
	const alter = "DROP PRIMARY KEY, ADD PRIMARY KEY (`metasite_id`,`version` DESC)"

	m := NewTestRunner(t, "pkdir_resume", alter,
		WithThreads(1),
		WithTargetChunkTime(100*time.Millisecond),
		WithTestThrottler())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := make(chan error, 1)
	go func() {
		c <- m.Run(ctx)
	}()
	waitForCheckpoint(t, m)
	cancel()
	require.Error(t, <-c) // interrupted as soon as a checkpoint is saved
	require.NoError(t, m.Close())

	m2 := NewTestRunner(t, "pkdir_resume", alter, WithThreads(2))
	require.NoError(t, m2.Run(t.Context()))
	require.True(t, m2.usedResumeFromCheckpoint)
	require.NoError(t, m2.Close())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM pkdir_resume").Scan(&count))
	require.Equal(t, 20000, count)
	var tbl, ddl string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SHOW CREATE TABLE pkdir_resume").Scan(&tbl, &ddl))
	require.Contains(t, ddl, "PRIMARY KEY (`metasite_id`,`version` DESC)")
}
