package checksum

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestJSONChecksumDecimalTypeDegradation pins the production bug directly:
// a JSON document holding a DECIMAL-typed scalar (e.g. from
// JSON_OBJECT('a', CAST(169.09 AS DECIMAL(12,6)))) renders at its declared
// scale ("169.090000"). Text numbers only ever parse back as
// INTEGER/UNSIGNED/DOUBLE, so a binlog-applied row round-trips through text
// and re-renders shorter ("169.09") — same JSON value, different bytes. The
// target row here is built with exactly that round-trip (CAST(CAST(j AS
// CHAR) AS JSON)) to stand in for what the applier would have written,
// without needing a live migration or concurrent DML to provoke it.
func TestJSONChecksumDecimalTypeDegradation(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chkjsondec1, _chkjsondec1_new, _chkjsondec1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE chkjsondec1 (a INT NOT NULL, j JSON NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsondec1_new (a INT NOT NULL, j JSON NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsondec1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, `INSERT INTO chkjsondec1 VALUES (1, JSON_OBJECT('a', CAST(169.09 AS DECIMAL(12,6))))`)
	testutils.RunSQL(t, `INSERT INTO _chkjsondec1_new SELECT a, CAST(CAST(j AS CHAR) AS JSON) FROM chkjsondec1`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chkjsondec1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_chkjsondec1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, NewCheckerDefaultConfig())
	require.NoError(t, err)
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	require.NoError(t, singleChecker.runChecksum(t.Context()))
}

// TestJSONChecksumDecimalGenuineMismatchStillCaught is the flip side of
// TestJSONChecksumDecimalTypeDegradation: the text round-trip must collapse
// type-tag degradation, but it must not become blind to real value
// differences. The target here is built from a DECIMAL value different from
// the source (42.10 vs 42.00), round-tripped through text exactly like the
// passing case above — so if the checksum were over-relaxed to ignore all
// DECIMAL-sourced JSON differences, this would wrongly pass too.
func TestJSONChecksumDecimalGenuineMismatchStillCaught(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chkjsondec2, _chkjsondec2_new, _chkjsondec2_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE chkjsondec2 (a INT NOT NULL, j JSON NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsondec2_new (a INT NOT NULL, j JSON NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsondec2_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, `INSERT INTO chkjsondec2 VALUES (1, JSON_OBJECT('a', CAST(42.00 AS DECIMAL(12,6))))`)
	testutils.RunSQL(t, `INSERT INTO _chkjsondec2_new VALUES (1, CAST(CAST(JSON_OBJECT('a', CAST(42.10 AS DECIMAL(12,6))) AS CHAR) AS JSON))`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chkjsondec2")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_chkjsondec2_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, NewCheckerDefaultConfig())
	require.NoError(t, err)
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	err = singleChecker.runChecksum(t.Context())
	require.ErrorContains(t, err, "checksum mismatch")
}

// TestJSONChecksumTextToJSONConversion pins a LONGTEXT->JSON conversion
// migration: the source column keeps the raw application text, which can be
// pretty-printed (extra whitespace) while the target's JSON column always
// holds the canonical rendering. castExpr's outer CAST(... AS json) parses
// the source text (the inner char cast is an identity for a text column),
// landing both sides on the same canonical form, so whitespace-only
// differences must not fail the checksum.
func TestJSONChecksumTextToJSONConversion(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chkjsontxt1, _chkjsontxt1_new, _chkjsontxt1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE chkjsontxt1 (a INT NOT NULL, j LONGTEXT NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsontxt1_new (a INT NOT NULL, j JSON NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkjsontxt1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, `INSERT INTO chkjsontxt1 VALUES (1, JSON_PRETTY(JSON_OBJECT('a', 1, 'b', 'x')))`)
	testutils.RunSQL(t, `INSERT INTO _chkjsontxt1_new VALUES (1, JSON_OBJECT('a', 1, 'b', 'x'))`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chkjsontxt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_chkjsontxt1_new")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, chunker.Open())

	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, NewCheckerDefaultConfig())
	require.NoError(t, err)
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	require.NoError(t, singleChecker.runChecksum(t.Context()))
}
