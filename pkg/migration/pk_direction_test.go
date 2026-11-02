package migration

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPrimaryKeyDirectionChange is the end-to-end claim behind the
// primarykey check's one exception: a primary key dropped and added back on
// the same columns, in the same order, with only ASC/DESC changed, copies,
// applies and checksums like any other rebuild. The real statement this came
// from (site_properties.events) also compressed the table, so this does too.
//
// Writes land on the table while it copies, so the applier's REPLACE and
// DELETE ... WHERE (pk) IN run against the DESC key, and the checksum
// compares rows by key across the two tables. The same writes go to a twin
// table nobody migrates; the two must end identical.
func TestPrimaryKeyDirectionChange(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) { testPrimaryKeyDirectionChange(t, "pkdir_u", false) })
	t.Run("buffered", func(t *testing.T) { testPrimaryKeyDirectionChange(t, "pkdir_b", true) })
}

func testPrimaryKeyDirectionChange(t *testing.T, name string, enableBuffered bool) {
	create := `CREATE TABLE %s (
		metasite_id int NOT NULL,
		version int NOT NULL,
		payload varchar(64) NOT NULL,
		PRIMARY KEY (metasite_id, version)
	)`
	tt := testutils.NewTestTable(t, name, fmt.Sprintf(create, name))
	twin := testutils.NewTestTable(t, name+"_twin", fmt.Sprintf(create, name+"_twin"))

	// 200 sites x 50 versions. Composite PK needs unique pairs, so no SeedRows.
	for site := 0; site < 200; site += 20 {
		var rows []string
		for s := site; s < site+20; s++ {
			for v := range 50 {
				rows = append(rows, fmt.Sprintf("(%d, %d, 'seed_%d_%d')", s, v, s, v))
			}
		}
		for _, table := range []string{name, name + "_twin"} {
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES %s", table, strings.Join(rows, ",")))
		}
	}

	m := NewTestRunner(t, name,
		"DROP PRIMARY KEY, ADD PRIMARY KEY (`metasite_id`,`version` DESC), ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
		WithBuffered(enableBuffered),
		WithTestThrottler())
	var logged bytes.Buffer
	m.SetLogger(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})))

	// Each write goes to the twin only once it has landed on the migrating
	// table, so a write refused during cutover is refused on both.
	both := func(format string, args ...any) {
		stmt := fmt.Sprintf(format, args...)
		if _, err := tt.DB.ExecContext(t.Context(), fmt.Sprintf(stmt, name)); err == nil {
			_, err = twin.DB.ExecContext(t.Context(), fmt.Sprintf(stmt, name+"_twin"))
			assert.NoError(t, err)
		}
	}
	var wg sync.WaitGroup
	writesDone := make(chan struct{})
	wg.Go(func() {
		defer close(writesDone)
		if !waitForCopyRows(t.Context(), m) {
			return
		}
		for i := range 100 {
			site := (i * 7) % 200
			both("INSERT INTO %%s VALUES (%d, %d, 'new_%d')", site, 1000+i, i)
			both("UPDATE %%s SET payload = 'upd_%d' WHERE metasite_id = %d AND version = %d", i, site, i%50)
			both("DELETE FROM %%s WHERE metasite_id = %d AND version = %d", (site+1)%200, (i+25)%50)
		}
	})

	require.NoError(t, m.Run(t.Context()))
	select {
	case <-writesDone:
	default:
		t.Fatal("the migration finished before the writes did, so the applier was not exercised")
	}
	wg.Wait()
	require.False(t, m.usedInstantDDL, "INSTANT DDL skips the copier, so this test proves nothing")
	require.False(t, m.usedInplaceDDL, "INPLACE DDL skips the copier, so this test proves nothing")
	output := logged.String()
	require.NoError(t, m.Close())

	var tbl, ddl string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SHOW CREATE TABLE "+name).Scan(&tbl, &ddl))
	assert.Contains(t, ddl, "PRIMARY KEY (`metasite_id`,`version` DESC)")
	assert.Contains(t, ddl, "ROW_FORMAT=COMPRESSED")

	fingerprint := func(table string) string {
		var count int
		var sum string
		require.NoError(t, tt.DB.QueryRowContext(t.Context(), fmt.Sprintf(
			"SELECT COUNT(*), COALESCE(BIT_XOR(CRC32(CONCAT_WS('#', metasite_id, version, payload))), 0) FROM %s", table),
		).Scan(&count, &sum))
		return fmt.Sprintf("%d rows, %s", count, sum)
	}
	assert.Equal(t, fingerprint(name+"_twin"), fingerprint(name),
		"the migrated table must hold exactly what the twin holds after the same writes")
	assert.NotContains(t, output, "chunk verification failed",
		"the checksum had to repair a chunk: the copy or the applier wrote something else under the DESC key")
}

// TestPrimaryKeyReorderIsRefused: the same columns in another order hold the
// same rows unique, but the applier and checksum look rows up by the old
// leading column, which the new key no longer starts with.
func TestPrimaryKeyReorderIsRefused(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "pkreorder", `CREATE TABLE pkreorder (
		a int NOT NULL,
		b int NOT NULL,
		PRIMARY KEY (a, b)
	)`)
	testutils.RunSQL(t, "INSERT INTO pkreorder VALUES (1, 1), (1, 2), (2, 1)")

	m := NewTestRunner(t, "pkreorder", "DROP PRIMARY KEY, ADD PRIMARY KEY (b, a)")
	err := m.Run(t.Context())
	require.ErrorContains(t, err, "dropping primary key is not supported")
	require.NoError(t, m.Close())
}
