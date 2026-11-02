package check

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

func TestPrimaryKey(t *testing.T) {
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (anothercol)")[0],
	}
	err := primaryKeyCheck(t.Context(), r, slog.Default())
	require.Error(t, err) // drop primary key

	r.Statement = statement.MustNew("ALTER TABLE t1 ADD INDEX (anothercol)")[0]
	err = primaryKeyCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // safe modification
}

// pkCheck runs primaryKeyCheck against a table whose primary key is keyColumns.
func pkCheck(t *testing.T, alter string, keyColumns ...string) error {
	t.Helper()
	return primaryKeyCheck(t.Context(), Resources{
		Table:     &table.TableInfo{KeyColumns: keyColumns},
		Statement: statement.MustNew("ALTER TABLE t1 " + alter)[0],
	}, slog.Default())
}

// TestPrimaryKeyDirectionChange: re-adding the primary key on the same
// columns, in the same order, keeps every row's key and every uniqueness
// guarantee. The copier's INSERT IGNORE, the applier's REPLACE and
// DELETE ... WHERE (pk) IN, and the checksum's per-row map all key on the
// column list, which does not change. Only ASC/DESC may.
func TestPrimaryKeyDirectionChange(t *testing.T) {
	require.NoError(t, pkCheck(t,
		"DROP PRIMARY KEY, ADD PRIMARY KEY (`metasite_id`,`version` DESC), ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
		"metasite_id", "version"))
	require.NoError(t, pkCheck(t, "DROP PRIMARY KEY, ADD PRIMARY KEY (a DESC, b ASC)", "a", "b"))
	require.NoError(t, pkCheck(t, "ADD PRIMARY KEY (`A`, b DESC), DROP PRIMARY KEY", "a", "B"),
		"column names are case-insensitive, and MySQL applies the DROP first wherever it sits")
}

// TestPrimaryKeyChangeThatMovesRows: any other change to the key can make
// rows collide on the new key (and INSERT IGNORE / REPLACE drop them without
// an error), or leave the applier and checksum looking rows up by columns the
// new table has no index on.
func TestPrimaryKeyChangeThatMovesRows(t *testing.T) {
	for _, tc := range []struct{ name, alter string }{
		{"different columns", "DROP PRIMARY KEY, ADD PRIMARY KEY (c)"},
		{"fewer columns", "DROP PRIMARY KEY, ADD PRIMARY KEY (a)"},
		{"more columns", "DROP PRIMARY KEY, ADD PRIMARY KEY (a, b, c)"},
		{"reordered columns", "DROP PRIMARY KEY, ADD PRIMARY KEY (b, a)"},
		{"prefix length", "DROP PRIMARY KEY, ADD PRIMARY KEY (a, b(10))"},
		{"drop without add", "DROP PRIMARY KEY"},
		{"drop without add, adding an index", "DROP PRIMARY KEY, ADD UNIQUE KEY u (a, b)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorContains(t, pkCheck(t, tc.alter, "a", "b"), "dropping primary key is not supported")
		})
	}
}

// TestPrimaryKeyDirectionChangeNeedsTheCurrentKey: without the table's
// current key there is nothing to compare against, so the old refusal stands.
func TestPrimaryKeyDirectionChangeNeedsTheCurrentKey(t *testing.T) {
	err := primaryKeyCheck(t.Context(), Resources{
		Statement: statement.MustNew("ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (a, b DESC)")[0],
	}, slog.Default())
	require.ErrorContains(t, err, "dropping primary key is not supported")
}
