// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"strconv"
	"strings"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBuildFullTextIndexScanBuildsSqlForLiteralPattern(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	ctx := NewBindContext(builder, nil)
	fn := makeFullTextIndexScanTestFunc(
		"",
		"`test`.`src`",
		"`test`.`__mo_fts_idx`",
		fullTextStringNumVal("Matrix"),
		fullTextModeNumVal(tree.FULLTEXT_BOOLEAN),
	)

	nodeID, err := builder.buildFullTextIndexScan(&tree.TableFunction{Func: fn}, ctx, []*planpb.Expr{
		makePlan2StringConstExprWithType(""),
		makePlan2StringConstExprWithType("`test`.`src`"),
		makePlan2StringConstExprWithType("`test`.`__mo_fts_idx`"),
		makePlan2StringConstExprWithType("Matrix"),
		makePlan2Int64ConstExprWithType(int64(tree.FULLTEXT_BOOLEAN)),
	}, nil)
	require.NoError(t, err)

	node := builder.qry.Nodes[nodeID]
	require.Equal(t, planpb.Node_FUNCTION_SCAN, node.NodeType)
	require.Equal(t, fulltext_index_scan_func_name, node.TableDef.TblFunc.Name)
	require.Len(t, node.TblFuncExprList, 4)
	require.NotEmpty(t, node.Stats.Sql)
	require.True(t, strings.Contains(node.Stats.Sql, "`test`.`__mo_fts_idx`"))
	require.True(t, strings.Contains(node.Stats.Sql, "matrix"))
}

func TestGetFullTextSqlSkipsPreparedPattern(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	fn := makeFullTextIndexScanTestFunc(
		"",
		"`test`.`src`",
		"`test`.`__mo_fts_idx`",
		tree.NewParamExpr(0),
		fullTextModeNumVal(tree.FULLTEXT_BOOLEAN),
	)

	sql, err := builder.getFullTextSql(fn, "{")
	require.NoError(t, err)
	require.Empty(t, sql)
}

func TestGetFullTextSqlRequiresConstantModeForLiteralPattern(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)
	fn := makeFullTextIndexScanTestFunc(
		"",
		"`test`.`src`",
		"`test`.`__mo_fts_idx`",
		fullTextStringNumVal("Matrix"),
		tree.NewParamExpr(1),
	)

	_, err := builder.getFullTextSql(fn, "")
	require.ErrorContains(t, err, "mode is not a constant")
}

func TestGetFullTextIndexScanSqlRejectsInvalidParams(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(), false, true)

	sql, err := builder.getFullTextIndexScanSql("{", "`test`.`__mo_fts_idx`", "Matrix", int64(tree.FULLTEXT_BOOLEAN))
	require.Error(t, err)
	require.Empty(t, sql)
}

func makeFullTextIndexScanTestFunc(params, sourceTable, indexTable string, pattern, mode tree.Expr) *tree.FuncExpr {
	return &tree.FuncExpr{
		Func: tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(fulltext_index_scan_func_name)),
		Exprs: tree.Exprs{
			fullTextStringNumVal(params),
			fullTextStringNumVal(sourceTable),
			fullTextStringNumVal(indexTable),
			pattern,
			mode,
		},
	}
}

func fullTextStringNumVal(s string) tree.Expr {
	return tree.NewNumVal(s, s, false, tree.P_char)
}

func fullTextModeNumVal(mode tree.FullTextSearchType) tree.Expr {
	modeInt := int64(mode)
	return tree.NewNumVal(modeInt, strconv.FormatInt(modeInt, 10), false, tree.P_int64)
}
