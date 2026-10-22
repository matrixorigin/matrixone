//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/m0admission"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func init() {
	activeM0PlanHooks.selectAdd = selectM0AddColumn
	activeM0PlanHooks.preserveClone = preserveM0CloneLayout
	activeM0PlanHooks.validateAdd = validateM0BoundAddColumn
	activeM0PlanHooks.disableCountNotNull = true
}

func m0PlanBase(table *plan.TableDef) (uint32, error) {
	return m0PlanBaseWithConstraints(table, true)
}

func m0PlanBaseWithConstraints(table *plan.TableDef, rejectConstraints bool) (uint32, error) {
	if table == nil || table.Pkey == nil || table.Pkey.CompPkeyCol == nil ||
		table.ClusterBy != nil || table.IsTemporary || table.TableType == catalog.SystemPartitionRel ||
		rejectConstraints && (len(table.Indexes) != 0 || len(table.Fkeys) != 0 ||
			len(table.RefChildTbls) != 0 || len(table.Checks) != 0) {
		return 0, moerr.NewInternalErrorNoCtxf("M0: table is outside the hidden composite primary-key domain")
	}
	pk := table.Pkey.CompPkeyCol
	if !pk.Hidden || !pk.Primary || pk.Name != catalog.CPrimaryKeyColName ||
		table.Pkey.PkeyColName != pk.Name {
		return 0, moerr.NewInternalErrorNoCtxf("M0: table lacks the hidden composite primary-key anchor")
	}
	if len(table.Cols) < 2 || table.Cols[len(table.Cols)-1].Name != catalog.Row_ID {
		return 0, moerr.NewInternalErrorNoCtxf("M0: rowid is not the final internal column")
	}
	next := uint32(len(table.Cols) - 1)
	base := uint32(pk.Seqnum) + 1
	if table.Version != next-base {
		return 0, moerr.NewInternalErrorNoCtxf("M0: table is outside the append-only version domain")
	}
	for pos, col := range table.Cols[:len(table.Cols)-1] {
		if col == nil || uint32(col.Seqnum) != uint32(pos) {
			return 0, moerr.NewInternalErrorNoCtxf("M0: dense column identity is not preserved")
		}
		if uint32(pos) >= base && (col.Default == nil || !col.Default.NullAbility || col.Typ.NotNullable ||
			col.GeneratedCol != nil || col.OnUpdate != nil ||
			col.Primary || col.Hidden || col.ClusterBy || col.Typ.AutoIncr) {
			return 0, moerr.NewInternalErrorNoCtxf("M0: appended column has unsupported semantics")
		}
	}
	return base, nil
}

func m0ColumnDefinition(
	spec *tree.ColumnTableDef,
	position *tree.ColumnPosition,
) error {
	if spec == nil || position != nil && position.Typ != tree.ColumnPositionNone {
		return moerr.NewInternalErrorNoCtxf("M0: only a trailing additive column is supported")
	}
	for _, attr := range spec.Attributes {
		switch attr := attr.(type) {
		case *tree.AttributeNull:
			if !attr.Is {
				return moerr.NewInternalErrorNoCtxf("M0: appended column must be nullable")
			}
		case *tree.AttributeComment:
			return moerr.NewInternalErrorNoCtxf("M0: appended column comments require COPY")
		case *tree.AttributeDefault:
			// The bound plan below accepts only a NULL literal or no expression.
		case *tree.AttributeCharset, *tree.AttributeCollate:
		default:
			return moerr.NewInternalErrorNoCtxf("M0: appended column has an unsupported attribute")
		}
	}
	return nil
}

func validateM0BoundAddColumn(col *plan.ColDef) error {
	if col == nil || col.Default == nil || !col.Default.NullAbility || col.Typ.NotNullable ||
		col.GeneratedCol != nil || col.OnUpdate != nil ||
		col.Primary || col.Hidden || col.ClusterBy || col.Typ.AutoIncr || col.Comment != "" {
		return moerr.NewInternalErrorNoCtxf("M0: appended column requires nullable/no-backfill semantics")
	}
	if plan.IsLiteralFalseDefault(col) && m0admission.Enabled() {
		return nil
	}
	if col.Default.OriginString != "" {
		return moerr.NewInternalErrorNoCtx("M0: a non-NULL default requires admitted birth-fill capability")
	}
	if col.Typ.Id == int32(types.T_enum) || len(col.Typ.Enumvalues) > 0 {
		return moerr.NewInternalErrorNoCtxf("M0: ENUM and SET appended columns are outside the append domain")
	}
	if col.Default.Expr != nil {
		lit := col.Default.Expr.GetLit()
		if lit == nil || !lit.GetIsnull() {
			return moerr.NewInternalErrorNoCtxf("M0: appended column requires no non-NULL default")
		}
	}
	return nil
}

func selectM0AddColumn(
	ctx CompilerContext,
	options []tree.AlterTableOption,
	table *plan.TableDef,
) (bool, error) {
	var add *tree.ColumnTableDef
	var position *tree.ColumnPosition
	for _, option := range options {
		switch option := option.(type) {
		case *tree.AlterOptionAlgorithm:
			switch option.Type {
			case "", "DEFAULT", "INPLACE":
			default:
				return false, nil
			}
		case *tree.AlterOptionLock:
		case *tree.AlterAddCol:
			if add != nil {
				return false, nil
			}
			add = option.Column
			position = option.Position
		case *tree.AlterTableAddColumnClause:
			if add != nil || len(option.NewColumns) != 1 {
				return false, nil
			}
			add = option.NewColumns[0]
			position = option.Position
		default:
			return false, nil
		}
	}
	if add == nil {
		return false, nil
	}
	if err := m0ColumnDefinition(add, position); err != nil {
		return false, nil
	}
	if _, err := m0PlanBase(table); err != nil {
		return false, nil
	}
	if ctx.GetProcess() == nil || ctx.GetProcess().GetTxnOperator() == nil {
		return false, nil
	}
	op := ctx.GetProcess().GetTxnOperator()
	opts := op.TxnOptions()
	if opts.GetByBegin() || !opts.GetAutocommit() ||
		!op.Txn().IsPessimistic() || !op.Txn().IsRCIsolation() {
		return false, nil
	}
	// Decide from the bound column before selecting INPLACE. A syntactically
	// additive column can still require backfill; COPY must remain available
	// without first entering the metadata-only builder and failing there.
	probe := &plan.AlterTable{TableDef: table, CopyTableDef: DeepCopyTableDef(table, true)}
	bindCtx := initAlterTableContext(table, probe.CopyTableDef, table.DbName)
	if _, err := AddColumn(ctx, probe, &tree.AlterAddCol{Column: add, Position: position}, bindCtx); err != nil {
		return false, err
	}
	bound := FindColumn(probe.CopyTableDef.Cols, add.Name.ColName())
	if plan.IsLiteralFalseDefault(bound) {
		if !m0admission.Enabled() || !m0LiteralFalseColumn(add) {
			return false, nil
		}
		if err := catalog.ValidateM0BirthTableProperties(table); err != nil {
			return false, nil
		}
	} else if table.AppendMissingState != nil {
		// The first extension only appends literal-false columns. A different
		// legal operation exits through COPY rather than entering TN halfway.
		return false, nil
	}
	if err := validateM0BoundAddColumn(bound); err != nil {
		return false, nil
	}
	return true, nil
}

func preserveM0CloneLayout(
	source *TableDef,
	createPlan *Plan,
	database, table string,
	temporary bool,
) (bool, error) {
	if _, err := m0PlanBase(source); err != nil {
		return false, nil
	}
	if createPlan == nil {
		return false, moerr.NewInternalErrorNoCtx("M0: missing clone create plan")
	}
	ct := createPlan.GetDdl().GetCreateTable()
	if ct == nil || ct.TableDef == nil {
		return false, moerr.NewInternalErrorNoCtx("M0: missing clone create table plan")
	}
	target := DeepCopyTableDef(source, true)
	target.TblId = 0
	target.DbId = 0
	target.LogicalId = 0
	target.Name = table
	target.DbName = database
	target.IsTemporary = temporary
	ct.TableDef = target
	return true, nil
}

// Bound constant folding is not SQL eligibility. Accept only a BOOLEAN false
// literal in the original AST, preserving COPY for strings and expressions.
func m0LiteralFalseColumn(column *tree.ColumnTableDef) bool {
	found := false
	for _, attr := range column.Attributes {
		if def, ok := attr.(*tree.AttributeDefault); ok {
			literal, ok := def.Expr.(*tree.NumVal)
			if found || !ok || literal.ValType != tree.P_bool || literal.Bool() {
				return false
			}
			found = true
		}
	}
	return found
}
