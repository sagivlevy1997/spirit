package checksum

import (
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestBasicChecksum(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS basic_checksum, _basic_checksum_new, _basic_checksum_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE basic_checksum (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _basic_checksum_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _basic_checksum_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO basic_checksum VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _basic_checksum_new VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "basic_checksum")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_basic_checksum_new")
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

	require.NoError(t, checker.Run(t.Context()))
}

func TestBasicValidation(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS basic_validation, basic_validation2, _basic_validation_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE basic_validation (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE basic_validation2 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _basic_validation_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO basic_validation VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO basic_validation2 VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "basic_validation")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "basic_validation2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	feed := change.NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier.NewSingleTargetForTest(t, db), change.NewClientDefaultConfig())
	defer feed.Close()
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, feed.AddSubscription(t1, t2, chunker))
	require.NoError(t, feed.Start(t.Context()))

	_, err = NewChecker(nil, chunker, []change.Source{feed}, NewCheckerDefaultConfig()) // no source DBs
	require.EqualError(t, err, "at least one source database must be provided")

	_, err = NewChecker([]*sql.DB{db}, nil, []change.Source{feed}, NewCheckerDefaultConfig())
	require.EqualError(t, err, "chunker must be non-nil")

	_, err = NewChecker([]*sql.DB{db}, chunker, nil, NewCheckerDefaultConfig()) // no feed
	require.EqualError(t, err, "at least one feed must be provided")
}

func TestUnfixableUniqueChecksum(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS uniqfailuret1, uniqfailuret2`)
	table1 := `CREATE TABLE uniqfailuret1 (
				id int NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NOT NULL,
				PRIMARY KEY (id)
			)`
	table2 := `CREATE TABLE uniqfailuret2 (
				id int NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NOT NULL,
				PRIMARY KEY (id),
				UNIQUE (b)
			)`
	testutils.RunSQL(t, table1)
	testutils.RunSQL(t, table2)
	testutils.RunSQL(t, "INSERT INTO uniqfailuret1 (name, b) VALUES ('a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqfailuret1 (name, b) VALUES ('a', REPEAT('b', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqfailuret1 (name, b) VALUES ('a', REPEAT('c', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqfailuret1 (name, b) VALUES ('a', REPEAT('a', 200))") // will cause unique index failure
	testutils.RunSQL(t, `INSERT IGNORE INTO uniqfailuret2 SELECT * FROM uniqfailuret1`)       // will not copy all data

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "uniqfailuret1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "uniqfailuret2")
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

	config := NewCheckerDefaultConfig()
	config.FixDifferences = true
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	err = checker.Run(t.Context())
	// Adding a UNIQUE INDEX to non-unique data: every attempt finds row
	// differences (and recopies don't help), so we exhaust retries on the
	// "found differences" path. The migration layer wraps this into a more
	// user-friendly "lossy unique-index" message; here we just assert the
	// underlying checksum-layer error.
	require.ErrorContains(t, err, "checksum found differences on every attempt")
}

func TestFixCorrupt(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS fixcorruption_t1, _fixcorruption_t1_new, _fixcorruption_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE fixcorruption_t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _fixcorruption_t1_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _fixcorruption_t1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO fixcorruption_t1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _fixcorruption_t1_new VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _fixcorruption_t1_new VALUES (2, 2, 3)") // corrupt

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "fixcorruption_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_fixcorruption_t1_new")
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

	config := NewCheckerDefaultConfig()
	config.FixDifferences = true
	config.MaxRetries = 2
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	err = checker.Run(t.Context())
	require.NoError(t, err) // yes there is corruption, but it was fixed.

	// Type assert the checker to *SingleChecker to access differencesFound
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	require.Equal(t, uint64(0), singleChecker.differencesFound.Load()) // this is "0", because we fixed it.

	// If we run the checker again, it will report zero differences.
	checker2, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	err = checker2.Run(t.Context())
	require.NoError(t, err)
	singleChecker, ok = checker2.(*SingleChecker)
	require.True(t, ok, "checker2 is not of type *SingleChecker")
	require.Equal(t, uint64(0), singleChecker.differencesFound.Load())
}

// TestRetryDoesNotVacuouslyPass is a regression test for the retry loop in
// Run. A failed attempt leaves isInvalid=true (set by the errgroup workers),
// and the retry reset previously did not clear it. Because isHealthy()
// returns false while isInvalid is set, the next attempt dispatched zero
// chunks and completed with differencesFound==0 — logging "checksum passed"
// and returning nil without having verified a single row. With
// FixDifferences=false a persistent mismatch must fail every attempt and
// surface an error, never nil.
func TestRetryDoesNotVacuouslyPass(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS retrypoison_t1, _retrypoison_t1_new, _retrypoison_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE retrypoison_t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _retrypoison_t1_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _retrypoison_t1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO retrypoison_t1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _retrypoison_t1_new VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _retrypoison_t1_new VALUES (2, 2, 3)") // corrupt: row not in source

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "retrypoison_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_retrypoison_t1_new")
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

	config := NewCheckerDefaultConfig()
	config.FixDifferences = false // surface the mismatch as an error on every attempt
	config.MaxRetries = 2
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)

	err = checker.Run(t.Context())
	require.Error(t, err)
	require.ErrorContains(t, err, "checksum errored on every attempt")
	require.ErrorContains(t, err, "checksum mismatch")

	// The final attempt must have actually re-verified chunks: its counter
	// was reset at the start of the attempt, so a non-zero value proves the
	// mismatch was re-detected rather than skipped.
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	require.Positive(t, singleChecker.differencesFound.Load())
}

// TestRunResetsPriorInvalidState covers the cross-Run leak of isInvalid: a
// prior Run that errored WITHOUT recording differences (e.g. a transient
// connection failure) leaves isInvalid=true and differencesFound==0. A
// subsequent Run on the same checker must start healthy — without the reset
// at the top of Run, attempt 1 skipped every chunk (isHealthy()==false), saw
// differencesFound==0, and returned nil having verified zero rows. Run must
// instead do real work: the chunker ends fully read with rows checked.
func TestRunResetsPriorInvalidState(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS runpoison_t1, _runpoison_t1_new, _runpoison_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE runpoison_t1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _runpoison_t1_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _runpoison_t1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO runpoison_t1 VALUES (1, 2, 3), (2, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _runpoison_t1_new VALUES (1, 2, 3), (2, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "runpoison_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_runpoison_t1_new")
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
	// Simulate the state left by a prior errored Run that found no differences.
	singleChecker.setInvalid(true)

	// The data is identical, so the pass must succeed — with real work done.
	require.NoError(t, checker.Run(t.Context()))
	require.True(t, chunker.IsRead(), "the chunker must be fully read; a vacuous pass reads no chunks")
	require.Positive(t, checker.GetProgress().RowsChecked, "rows must actually be verified")
}

func TestCorruptChecksum(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chkpcorruptt1, _chkpcorruptt1_new, _chkpcorruptt1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE chkpcorruptt1 (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkpcorruptt1_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkpcorruptt1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO chkpcorruptt1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _chkpcorruptt1_new VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _chkpcorruptt1_new VALUES (2, 2, 3)") // corrupt

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chkpcorruptt1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_chkpcorruptt1_new")
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

// TestCorruptBinaryChecksum tests that the checksum detects corruption in a
// fixed-length BINARY(N) column. Previously the checksum cast binary columns
// to binary(0), which truncates every value to zero bytes — so any two values
// produced identical CRCs and the contents of BINARY(N) columns were
// completely invisible to the checksum.
func TestCorruptBinaryChecksum(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS chkpcorruptbin1, _chkpcorruptbin1_new, _chkpcorruptbin1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE chkpcorruptbin1 (a INT NOT NULL, b BINARY(16) NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkpcorruptbin1_new (a INT NOT NULL, b BINARY(16) NOT NULL, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _chkpcorruptbin1_chkpnt (a INT)") // for binlog advancement
	testutils.RunSQL(t, "INSERT INTO chkpcorruptbin1 VALUES (1, UNHEX('00112233445566778899AABBCCDDEEFF'))")
	testutils.RunSQL(t, "INSERT INTO _chkpcorruptbin1_new SELECT * FROM chkpcorruptbin1")
	// Corrupt the binary value on the target: same length, different contents.
	testutils.RunSQL(t, "UPDATE _chkpcorruptbin1_new SET b = UNHEX('FFEEDDCCBBAA99887766554433221100') WHERE a = 1")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "chkpcorruptbin1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_chkpcorruptbin1_new")
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

func TestBoundaryCases(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS checkert1, _checkert1_new, _checkert1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE checkert1 (a INT NOT NULL, b FLOAT, c VARCHAR(255), PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _checkert1_new (a INT NOT NULL, b FLOAT, c VARCHAR(255), PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _checkert1_chkpnt (a INT NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO checkert1 VALUES (1, 2.2, '')")        // null vs empty string
	testutils.RunSQL(t, "INSERT INTO _checkert1_new VALUES (1, 2.2, NULL)") // should not compare

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "checkert1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_checkert1_new")
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
	// Type assert to *SingleChecker to access runChecksum
	singleChecker, ok := checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	require.Error(t, singleChecker.runChecksum(t.Context()))

	// UPDATE t1 to also be NULL
	testutils.RunSQL(t, "UPDATE checkert1 SET c = NULL")
	checker, err = NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, NewCheckerDefaultConfig())
	require.NoError(t, err)
	// Type assert to *SingleChecker to access runChecksum
	singleChecker, ok = checker.(*SingleChecker)
	require.True(t, ok, "checker is not of type *SingleChecker")
	require.NoError(t, singleChecker.runChecksum(t.Context()))
}

func TestChangeDataTypeDatetime(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS tdatetime, _tdatetime_new")
	testutils.RunSQL(t, `CREATE TABLE tdatetime (
	id bigint NOT NULL AUTO_INCREMENT primary key,
	created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	issued_at timestamp NULL DEFAULT NULL,
	activated_at timestamp NULL DEFAULT NULL,
	deactivated_at timestamp NULL DEFAULT NULL
	)`)
	testutils.RunSQL(t, `CREATE TABLE _tdatetime_new (
	id bigint NOT NULL AUTO_INCREMENT primary key,
	created_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	updated_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
	issued_at timestamp(6) NULL DEFAULT NULL,
	activated_at timestamp(6) NULL DEFAULT NULL,
	deactivated_at timestamp(6) NULL DEFAULT NULL
	)`)
	testutils.RunSQL(t, `INSERT INTO tdatetime (created_at, updated_at, issued_at, activated_at, deactivated_at) VALUES
	('2023-05-18 09:28:46', '2023-05-18 09:33:27', '2023-05-18 09:28:45', '2023-05-18 09:28:45', NULL               ),
	('2023-05-18 09:34:38', '2023-05-24 07:38:25', '2023-05-18 09:34:37', '2023-05-18 09:34:37', '2023-05-24 07:38:25'),
	('2023-05-24 07:34:36', '2023-05-24 07:34:36', '2023-05-24 07:34:35', NULL               , NULL               ),
	('2023-05-24 07:41:05', '2023-05-25 06:15:37', '2023-05-24 07:41:04', '2023-05-24 07:41:04', '2023-05-25 06:15:37'),
	('2023-05-25 06:17:30', '2023-05-25 06:17:30', '2023-05-25 06:17:29', '2023-05-25 06:17:29', NULL               ),
	('2023-05-25 06:18:33', '2023-05-25 06:41:13', '2023-05-25 06:18:32', '2023-05-25 06:18:32', '2023-05-25 06:41:13'),
	('2023-05-25 06:24:23', '2023-05-25 06:24:23', '2023-05-25 06:24:22', NULL               , NULL               ),
	('2023-05-25 06:41:35', '2023-05-28 23:45:09', '2023-05-25 06:41:34', '2023-05-25 06:41:34', '2023-05-28 23:45:09'),
	('2023-05-25 06:44:41', '2023-05-28 23:45:03', '2023-05-25 06:44:40', '2023-05-25 06:46:48', '2023-05-28 23:45:03'),
	('2023-05-26 06:24:24', '2023-05-28 23:45:01', '2023-05-26 06:24:23', '2023-05-26 06:24:42', '2023-05-28 23:45:01'),
	('2023-05-28 23:46:07', '2023-05-29 00:57:55', '2023-05-28 23:46:05', '2023-05-28 23:46:05', NULL               ),
	('2023-05-28 23:53:34', '2023-05-29 00:57:56', '2023-05-28 23:53:33', '2023-05-28 23:58:09', NULL               );`)
	testutils.RunSQL(t, `INSERT INTO _tdatetime_new SELECT * FROM tdatetime`)
	// The checkpoint table is required for blockwait, structure doesn't matter.
	testutils.RunSQL(t, "CREATE TABLE IF NOT EXISTS _tdatetime_chkpnt (id int)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "tdatetime")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_tdatetime_new")
	require.NoError(t, t2.SetInfo(t.Context())) // fails

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
	require.NoError(t, checker.Run(t.Context())) // fails
}

func TestYieldTimeout(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS yield_t1, _yield_t1_new, _yield_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE yield_t1 (a INT NOT NULL AUTO_INCREMENT, b VARCHAR(255), c VARCHAR(255), PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _yield_t1_new (a INT NOT NULL AUTO_INCREMENT, b VARCHAR(255), c VARCHAR(255), PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _yield_t1_chkpnt (a INT)") // for binlog advancement

	// Insert enough rows with wide data to produce multiple chunks and ensure
	// the checksum takes long enough for the yield timeout to fire mid-pass.
	// Starting chunk size is 1000, so 100k rows should produce many chunks.
	testutils.RunSQL(t, "INSERT INTO yield_t1 (b, c) SELECT REPEAT('x', 200), REPEAT('y', 200) FROM information_schema.columns a, information_schema.columns b LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO _yield_t1_new SELECT * FROM yield_t1")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "yield_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_yield_t1_new")
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

	config := NewCheckerDefaultConfig()
	config.Concurrency = 1
	// Use a short yield timeout. The initConnPool phase uses the parent
	// context (not the yield context), so lock acquisition always succeeds.
	// The yield context only governs the chunk-processing loop. 100ms is
	// long enough for at least one chunk to complete (setting the watermark)
	// but short enough to trigger multiple yields over 100k rows.
	config.YieldTimeout = 100 * time.Millisecond
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)

	// The checksum should still pass despite yielding — it resumes from the watermark.
	require.NoError(t, checker.Run(t.Context()))

	// Verify that at least one yield actually occurred.
	singleChecker := checker.(*SingleChecker)
	require.Positive(t, singleChecker.yieldsPerformed.Load(), "expected at least one yield to occur")
	t.Logf("yields performed: %d", singleChecker.yieldsPerformed.Load())
}

func TestFromWatermark(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS tfromwatermark, _tfromwatermark_new, _tfromwatermark_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE tfromwatermark (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE _tfromwatermark_new (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO tfromwatermark VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO _tfromwatermark_new VALUES (1, 2, 3)")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "tfromwatermark")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_tfromwatermark_new")
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

	config := NewCheckerDefaultConfig()
	config.Watermark = "{\"Key\":[\"a\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"2\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"3\"],\"Inclusive\":false}}"
	checker, err := NewChecker([]*sql.DB{db}, chunker, []change.Source{feed}, config)
	require.NoError(t, err)
	require.NoError(t, checker.Run(t.Context()))
}

// TestColumnBoundaryShift is a regression test for the missing inter-column
// separator in the checksum expression. Without a separator, CONCAT() lets
// content shift across adjacent column boundaries undetected: the rows
// ('x0', 'y') and ('x', '0y') concatenate to the same string ("x00y0",
// including the ISNULL digits) and therefore the same CRC32. The checksum
// must report these rows as different.
func TestColumnBoundaryShift(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS colshift_t1, _colshift_t1_new, _colshift_t1_chkpnt")
	testutils.RunSQL(t, "CREATE TABLE colshift_t1 (id INT NOT NULL, a VARCHAR(255), b VARCHAR(255), PRIMARY KEY (id))")
	testutils.RunSQL(t, "CREATE TABLE _colshift_t1_new (id INT NOT NULL, a VARCHAR(255), b VARCHAR(255), PRIMARY KEY (id))")
	testutils.RunSQL(t, "CREATE TABLE _colshift_t1_chkpnt (a INT)") // for binlog advancement
	// The trailing '0' of column a has migrated to the front of column b
	// in the target table. The data is different, so the checksum must fail.
	testutils.RunSQL(t, "INSERT INTO colshift_t1 VALUES (1, 'x0', 'y')")
	testutils.RunSQL(t, "INSERT INTO _colshift_t1_new VALUES (1, 'x', '0y')")

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	t1 := table.NewTableInfo(db, "test", "colshift_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "_colshift_t1_new")
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
