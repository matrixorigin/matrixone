// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (builder *QueryBuilder) buildChangeWatermark(
	_ *tree.TableFunction,
	ctx *BindContext,
	exprs []*plan.Expr,
	children []int32,
) (int32, error) {
	if len(exprs) != 0 {
		return 0, moerr.NewInvalidArg(
			builder.GetContext(),
			"change_watermark function has invalid input args length",
			len(exprs),
		)
	}
	return builder.appendNode(newChangeFunctionScan(
		builder,
		"change_watermark",
		children,
		exprs,
		[]*plan.ColDef{varcharChangeColumn("watermark")},
	), ctx), nil
}

func (builder *QueryBuilder) buildTableChanges(
	_ *tree.TableFunction,
	ctx *BindContext,
	exprs []*plan.Expr,
	children []int32,
) (int32, error) {
	if len(exprs) != 4 {
		return 0, moerr.NewInvalidArg(
			builder.GetContext(),
			"table_changes function has invalid input args length",
			len(exprs),
		)
	}
	databaseName, ok := stringLiteral(exprs[0])
	if !ok || strings.TrimSpace(databaseName) == "" {
		return 0, moerr.NewInvalidInput(
			builder.GetContext(),
			"table_changes database name must be a non-empty string literal",
		)
	}
	tableName, ok := stringLiteral(exprs[1])
	if !ok || strings.TrimSpace(tableName) == "" {
		return 0, moerr.NewInvalidInput(
			builder.GetContext(),
			"table_changes table name must be a non-empty string literal",
		)
	}

	objectRef, sourceDef, err := builder.compCtx.Resolve(databaseName, tableName, nil)
	if err != nil {
		return 0, err
	}
	if err := validateTableChangesSource(objectRef, sourceDef); err != nil {
		return 0, err
	}

	cols := []*plan.ColDef{
		varcharChangeColumn("change_type"),
		varcharChangeColumn("commit_ts"),
		{
			Name: "table_id",
			Typ:  plan.Type{Id: int32(types.T_uint64)},
		},
		{
			Name: "schema_version",
			Typ:  plan.Type{Id: int32(types.T_uint32)},
		},
	}
	for _, sourceCol := range sourceDef.Cols {
		if sourceCol.Hidden {
			continue
		}
		col := *sourceCol
		col.Typ.NotNullable = false
		cols = append(cols, &col)
	}

	node := newChangeFunctionScan(
		builder,
		"table_changes",
		children,
		exprs,
		cols,
	)
	if sourceDef.TableType == catalog.SystemClusterRel ||
		(strings.EqualFold(databaseName, catalog.MO_CATALOG) &&
			(strings.EqualFold(tableName, catalog.MO_DATABASE) ||
				strings.EqualFold(tableName, catalog.MO_TABLES))) {
		// Shared physical tables must be opened under the system account before
		// the transaction relation cache is populated. The executor still
		// filters every returned row to the SQL caller's account.
		node.TableDef.TblFunc.Param = []byte{1}
	}
	return builder.appendNode(node, ctx), nil
}

func validateTableChangesSource(objectRef *plan.ObjectRef, tableDef *plan.TableDef) error {
	if tableDef == nil {
		return moerr.NewInvalidInputNoCtx("table_changes source table does not exist")
	}
	if objectRef != nil && objectRef.PubInfo != nil {
		return moerr.NewNotSupportedNoCtx("table_changes does not support subscription tables")
	}
	switch tableDef.TableType {
	case catalog.SystemOrdinaryRel:
	case catalog.SystemClusterRel:
		// Cluster tables have a physical account_id column. The executor applies
		// the same account filter as an ordinary SQL scan.
	default:
		return moerr.NewNotSupportedNoCtxf(
			"table_changes does not support table type %q",
			tableDef.TableType,
		)
	}
	if tableDef.IsTemporary {
		return moerr.NewNotSupportedNoCtx("table_changes does not support temporary tables")
	}
	if tableDef.Partition != nil {
		return moerr.NewNotSupportedNoCtx("table_changes does not support partitioned tables")
	}
	if tableDef.Pkey == nil ||
		len(tableDef.Pkey.Names) == 0 ||
		tableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return moerr.NewNotSupportedNoCtx("table_changes requires an explicit primary key")
	}
	if tableDef.TableType == catalog.SystemClusterRel &&
		!containsChangeKey(tableDef.Pkey.Names, "account_id") {
		return moerr.NewNotSupportedNoCtx(
			"table_changes requires cluster table primary keys to include account_id",
		)
	}
	return nil
}

func containsChangeKey(names []string, target string) bool {
	for _, name := range names {
		if strings.EqualFold(name, target) {
			return true
		}
	}
	return false
}

func stringLiteral(expr *plan.Expr) (string, bool) {
	if expr == nil || expr.GetLit() == nil || expr.GetLit().Isnull {
		return "", false
	}
	return expr.GetLit().GetSval(), expr.Typ.Id == int32(types.T_varchar) ||
		expr.Typ.Id == int32(types.T_char) ||
		expr.Typ.Id == int32(types.T_text)
}

func newChangeFunctionScan(
	builder *QueryBuilder,
	name string,
	children []int32,
	exprs []*plan.Expr,
	cols []*plan.ColDef,
) *plan.Node {
	return &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc:   &plan.TableFunction{Name: name},
			Cols:      cols,
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
}

func varcharChangeColumn(name string) *plan.ColDef {
	return &plan.ColDef{
		Name: name,
		Typ: plan.Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		},
	}
}
