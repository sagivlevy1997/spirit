package migration

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnEmptyBlobSurvivesTheDefaultCopier is the end-to-end claim, through a
// real migration with the DEFAULT (buffered) copier -- no --unbuffered.
//
// This is the test that would have caught the original bug. Every unit test
// around Datum.String() asserted "0x00" and agreed with itself; nothing
// copied a table with an empty blob in it and then looked at the bytes.
//
// On wix_quotes.quote_view the corruption was 1:1 and deterministic, 19-52%
// of rows in the ranges measured, and it took three migrations and ~65 hours
// of copying to find, because the checksum's own repair is server-side and so
// overwrote the evidence with correct data every time anyone sampled it.
func TestAnEmptyBlobSurvivesTheDefaultCopier(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS emptyblob, _emptyblob_new, _emptyblob_chkpnt")
	testutils.RunSQL(t, `CREATE TABLE emptyblob (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		payload BLOB,
		sig VARBINARY(16)
	)`)
	// Three shapes that must stay distinguishable after a copy: empty, one
	// NUL byte, and NULL. The bug collapsed the first two into each other.
	testutils.RunSQL(t, `INSERT INTO emptyblob (payload, sig) VALUES
		('', ''),
		(0x00, 0x00),
		(NULL, NULL),
		('abc', 0x616263)`)

	// `ENGINE=InnoDB`, a null rebuild, because the point is to exercise the
	// COPIER. The first version of this test used `ADD COLUMN newcol INT
	// NULL`, which MySQL 8 does as INSTANT DDL -- no copy, no applier, no
	// Datum.String(). It passed with the bug deliberately restored, which is
	// how a test that cannot fail gets written. The two assertions below are
	// the guard against that happening again.
	m := NewTestRunner(t, "emptyblob", "ENGINE=InnoDB")

	// The log is the observable, because the two obvious ones do not work.
	//
	// The finished TABLE is always correct: FixDifferences is on, so a chunk
	// the copier got wrong is detected and recopied server-side with
	// INSERT IGNORE .. SELECT. That repair is exactly why this bug survived
	// ~65 hours of production copying -- every attempt to catch it by looking
	// at the data found correct data.
	//
	// DifferencesFound() is always 0 too: Run's retry loop zeroes the
	// counters before the next attempt, so a successful Run reports the clean
	// FINAL attempt rather than the one that found the mismatch.
	//
	// Both of those were written, run against the restored bug, and seen to
	// PASS before landing on this.
	var logged bytes.Buffer
	m.SetLogger(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})))

	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL, "INSTANT DDL skips the copier, so this test proves nothing")
	require.False(t, m.usedInplaceDDL, "INPLACE DDL skips the copier, so this test proves nothing")

	output := logged.String()
	require.NoError(t, m.Close())

	assert.NotContains(t, output, "chunk verification failed",
		"the copier wrote a chunk the checksum had to repair. With an empty BLOB and an empty "+
			"VARBINARY in the source and nothing else unusual, that is the 0x00 substitution in "+
			"Datum.String -- one NUL byte where the source holds zero bytes")
}
