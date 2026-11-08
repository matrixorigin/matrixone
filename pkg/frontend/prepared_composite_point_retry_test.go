// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pbstats "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedCompositePointRetryKeepsBindings(t *testing.T) {
	base := plan2.NewMockCompilerContext(true, newPlanTestProcess(t))
	_, table, err := base.Resolve("tpch", "partsupp", nil)
	require.NoError(t, err)
	keys := []string{"ps_partkey", "ps_suppkey", "ps_comment"}
	for _, col := range table.Cols {
		for _, key := range keys {
			if col.Name == key {
				col.Typ = plan.Type{Id: int32(types.T_varchar), Width: 16, NotNullable: true, Charset: uint32(types.CharsetUTF8)}
			}
		}
	}
	hidden := plan2.MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
	table.Pkey = &plan.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Names: keys, CompPkeyCol: hidden}
	table.ClusterBy = nil
	rowID := table.Cols[len(table.Cols)-1]
	table.Cols[len(table.Cols)-1] = hidden
	table.Cols = append(table.Cols, rowID)
	table.Name2ColIndex = make(map[string]int32)
	for i, col := range table.Cols {
		table.Name2ColIndex[col.Name] = int32(i)
	}
	ctx := &preparedStatsTestCompiler{
		preparedTestCompiler: &preparedTestCompiler{CompilerContext: base, proc: base.GetProcess()},
		stats:                &pbstats.StatsInfo{TableCnt: 128}, table: table,
	}
	query := "select ps_availqty from partsupp where ps_partkey=? and ((ps_suppkey=? and ps_comment=?) or (ps_suppkey=? and ps_comment=?))"
	ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQLWithCompilerContext(t, 32365, query, ctx)
	ctx.proc, prepared.proc = cw.proc, cw.proc
	prepared.defaultDatabase = "tpch"
	t.Cleanup(func() {
		cw.proc.SetPrepareParams(nil)
		prepared.Close()
		ses.Close()
	})
	values := []string{"tenant", "s1", "r1", "s2", "r2"}
	prepared.params = vector.NewVec(types.T_text.ToType())
	for _, value := range values {
		require.NoError(t, vector.AppendBytes(prepared.params, []byte(value), false, cw.proc.Mp()))
		prepared.ParamTypes = append(prepared.ParamTypes, byte(defines.MYSQL_TYPE_VAR_STRING), 0)
	}
	_, initial, clone, _, owned, err := initExecuteStmtParamWithResolverInSession(execCtx, ses, ses, cw, nil, prepared.Name, ctx.Resolve, ctx)
	if owned && clone != nil {
		clone.Free()
	}
	require.NoError(t, err)
	retry := cw.preparedExecutionRetry()
	require.NotNil(t, retry)
	rebuilt, err := buildPlanForCompileRetry(execCtx.reqCtx, ses, ctx, prepared.PrepareStmt, false, retry)
	require.NoError(t, err)
	for i, value := range values {
		got, err := cw.proc.GetPrepareParamsAt(i)
		require.NoError(t, err)
		require.Equal(t, value, string(got))
	}
	points := func(p *plan.Plan) [][]int32 {
		t.Helper()
		for _, node := range p.GetQuery().Nodes {
			if node.NodeType != plan.Node_TABLE_SCAN || node.TableDef == nil {
				continue
			}
			for _, expr := range node.FilterList {
				fn := expr.GetF()
				if fn == nil || fn.Func == nil || fn.Func.ObjName != "in" || len(fn.Args) != 2 {
					continue
				}
				col := fn.Args[0].GetCol()
				if col == nil || !strings.HasSuffix(col.Name, catalog.CPrimaryKeyColName) || fn.Args[1].GetList() == nil {
					continue
				}
				var tuples [][]int32
				for _, tuple := range fn.Args[1].GetList().List {
					var positions []int32
					require.NoError(t, plan.VisitExprTree(tuple, func(e *plan.Expr) error {
						if p := e.GetP(); p != nil {
							positions = append(positions, p.Pos)
						}
						return nil
					}))
					tuples = append(tuples, positions)
				}
				return tuples
			}
		}
		return nil
	}
	want := [][]int32{{0, 1, 2}, {0, 3, 4}}
	require.Equal(t, want, points(initial))
	require.Equal(t, want, points(rebuilt))
}
