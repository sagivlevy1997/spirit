package check

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func init() {
	registerCheck("primarykey", primaryKeyCheck, ScopePreflight)
}

func primaryKeyCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	alterStmt, ok := (*r.Statement.StmtNode).(*ast.AlterTableStmt)
	if !ok {
		return errors.New("not a valid alter table statement")
	}
	var dropsPK bool
	var addedPK *ast.Constraint
	for _, spec := range alterStmt.Specs {
		if spec.Tp == ast.AlterTableDropPrimaryKey {
			dropsPK = true
		}
		if spec.Tp == ast.AlterTableAddConstraint && spec.Constraint.Tp == ast.ConstraintPrimaryKey {
			addedPK = spec.Constraint
		}
	}
	if dropsPK && !readdsSamePK(addedPK, r) {
		return errors.New("dropping primary key is not supported, except to add it back on the same columns in the same order (only ASC/DESC may change)")
	}
	return nil // no problems
}

// readdsSamePK is true when the statement adds the primary key back on
// exactly the table's current key columns, in the same order, with no prefix
// lengths or expressions. Such a key holds the same rows unique and is found
// by the same column list, so the copier (INSERT IGNORE), the applier
// (REPLACE, DELETE ... WHERE (pk) IN) and the checksum all behave as if the
// key had not changed. Any other key can make rows collide on it -- and be
// silently dropped -- or be looked up by columns the new table has no index
// on.
func readdsSamePK(added *ast.Constraint, r Resources) bool {
	if added == nil || r.Table == nil || len(added.Keys) != len(r.Table.KeyColumns) {
		return false
	}
	for i, part := range added.Keys {
		if part.Column == nil || part.Expr != nil || part.Length > 0 {
			return false
		}
		if part.Column.Name.L != strings.ToLower(r.Table.KeyColumns[i]) {
			return false
		}
	}
	return true
}
