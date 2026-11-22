package check

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
)

func init() {
	registerCheck("replica", replicaPrivilegeCheck, ScopePreflight)
}

// Check that there is permission to run perfschema queries for replication (8.0)
// on all configured replicas.
func replicaPrivilegeCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	for i, replica := range r.Replicas {
		qctx, cancel := context.WithTimeout(ctx, throttler.ReplicaQueryTimeout)
		why := replicaPrivilegeProblem(qctx, replica)
		cancel()
		if why != "" {
			// Not fatal: the lag throttler sets such a replica aside and the
			// run goes on with the rest. Named, never set aside here.
			logger.Warn(ReplicaIgnoredByCheckMsg, "host", replicaHostAt(r, i), "reason", why)
		}
	}
	return nil
}

// replicaPrivilegeProblem returns why lag cannot be read on replica, or "".
func replicaPrivilegeProblem(ctx context.Context, replica *sql.DB) string {
	var version string
	if err := replica.QueryRowContext(ctx, "select substr(version(), 1, 1)").Scan(&version); err != nil {
		return err.Error() // can not get version
	}
	rows, err := replica.QueryContext(ctx, throttler.MySQL8LagQuery)
	if err != nil {
		return err.Error()
	}
	_, err = scanToMap(rows)
	utils.CloseAndLog(rows)
	if err != nil {
		return err.Error()
	}
	return ""
}

func scanToMap(rows *sql.Rows) (map[string]sql.NullString, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		err = rows.Err()
		if err != nil {
			return nil, err
		} else {
			return nil, nil
		}
	}
	values := make([]any, len(columns))
	for index := range values {
		values[index] = new(sql.NullString)
	}
	err = rows.Scan(values...)
	if err != nil {
		return nil, err
	}
	result := make(map[string]sql.NullString)
	for index, columnName := range columns {
		result[columnName] = *values[index].(*sql.NullString)
	}
	return result, nil
}
