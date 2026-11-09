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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pbstats "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func preparedCompositePointTable(t *testing.T) (*plan2.MockCompilerContext, *plan.TableDef) {
	t.Helper()
	base := plan2.NewMockCompilerContext(true, newPlanTestProcess(t))
	_, table, err := base.Resolve("tpch", "partsupp", nil, nil)
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
	return base, table
}

func preparedCompositePointPositions(t *testing.T, p *plan.Plan) [][]int32 {
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

func setPreparedCompositePointParams(t *testing.T, prepared *PrepareStmt, cw *TxnComputationWrapper, values ...any) {
	t.Helper()
	cw.proc.SetPrepareParams(nil)
	if prepared.params != nil {
		prepared.params.Free(cw.proc.Mp())
	}
	prepared.params = vector.NewVec(types.T_text.ToType())
	prepared.ParamTypes = nil
	for _, value := range values {
		var data []byte
		switch v := value.(type) {
		case nil:
		case string:
			data = []byte(v)
		case []byte:
			data = v
		default:
			t.Fatalf("unsupported test parameter %T", value)
		}
		require.NoError(t, vector.AppendBytes(prepared.params, data, value == nil, cw.proc.Mp()))
		prepared.ParamTypes = append(prepared.ParamTypes, byte(defines.MYSQL_TYPE_VAR_STRING), 0)
	}
}

func executePreparedCompositePoint(t *testing.T, ses *Session, prepared *PrepareStmt, cw *TxnComputationWrapper, execCtx *ExecCtx, ctx plan2.CompilerContext) *plan.Plan {
	t.Helper()
	_, result, clone, _, owned, err := initExecuteStmtParamWithResolverInSession(execCtx, ses, ses, cw, nil, prepared.Name, ctx.Resolve, ctx)
	if owned && clone != nil {
		clone.Free()
	}
	require.NoError(t, err)
	return result
}

func TestPreparedCompositePointRetryKeepsBindings(t *testing.T) {
	base, table := preparedCompositePointTable(t)
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
	rebuilt, err := buildPlanForCompileRetry(execCtx.reqCtx, ses, ctx, prepared.PrepareStmt, false, retry, nil)
	require.NoError(t, err)
	for i, value := range values {
		got, err := cw.proc.GetPrepareParamsAt(i)
		require.NoError(t, err)
		require.Equal(t, value, string(got))
	}
	want := [][]int32{{0, 1, 2}, {0, 3, 4}}
	require.Equal(t, want, preparedCompositePointPositions(t, initial))
	require.Equal(t, want, preparedCompositePointPositions(t, rebuilt))
}

func TestPreparedCompositePointValueDependentAdmission(t *testing.T) {
	base, table := preparedCompositePointTable(t)
	ctx := &preparedStatsTestCompiler{
		preparedTestCompiler: &preparedTestCompiler{CompilerContext: base, proc: base.GetProcess()},
		stats:                &pbstats.StatsInfo{TableCnt: 128}, table: table,
	}
	query := "select ps_availqty from partsupp where ps_partkey=? and ((ps_suppkey=? and ps_comment=?) or (ps_suppkey=? and ps_comment=?))"
	ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQLWithCompilerContext(t, 3236502, query, ctx)
	ctx.proc, prepared.proc = cw.proc, cw.proc
	prepared.defaultDatabase = "tpch"
	t.Cleanup(func() {
		cw.proc.SetPrepareParams(nil)
		prepared.Close()
		ses.Close()
	})
	large := strings.Repeat("x", 1<<20)
	var nonNullKey, nullKey string
	var nullPlan *plan.Plan
	for _, tc := range []struct {
		name      string
		value     any
		wantPoint bool
	}{
		{name: "large first", value: large},
		{name: "small after large", value: "u", wantPoint: true},
		{name: "large after small", value: large},
		{name: "small again", value: "u", wantPoint: true},
		{name: "null", value: nil},
		{name: "small after null", value: "u", wantPoint: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setPreparedCompositePointParams(t, prepared, cw, tc.value, "s1", "r1", "s2", "r2")
			result := executePreparedCompositePoint(t, ses, prepared, cw, execCtx, ctx)
			require.Equal(t, tc.wantPoint, len(preparedCompositePointPositions(t, result)) > 0)
			retry := cw.preparedExecutionRetry()
			require.NotNil(t, retry)
			key := preparedTemporalBindingKey(preparedFieldBindingKey(preparedExecutionBindingKey(retry.bindings, retry.paramVals), retry.fieldTypes), retry.temporalTypes)
			if tc.value == nil {
				nullKey = key
				require.Same(t, prepared, cw.runtimeCacheTarget)
				nullPlan = result
				runtimeCompile := compile.NewCompile("", "", prepared.Sql, "", "", nil, cw.proc, prepared.PrepareStmt, false, nil, time.Now())
				require.True(t, cw.installRuntimeCacheCandidate(runtimeCompile))
			} else if nonNullKey == "" {
				nonNullKey = key
			} else {
				require.Equal(t, nonNullKey, key, "small and large strings must exercise the same cache key")
			}
			if tc.value != nil {
				require.Nil(t, cw.runtimeCacheTarget, "value-dependent admission must not cache either plan")
				if nullPlan != nil {
					require.NotSame(t, nullPlan, result, "a cached NULL plan must not hide a later point candidate")
				}
			}
		})
	}
	require.NotEmpty(t, nullKey)
	require.NotEqual(t, nonNullKey, nullKey, "NULL participates in the prepared cache key")
}

type preparedCompositePointIndexCompiler struct {
	*preparedStatsTestCompiler
	index *plan.TableDef
}

func (c *preparedCompositePointIndexCompiler) ResolveIndexTableByRef(ref *plan.ObjectRef, name string, snapshot *plan2.Snapshot, output *plan2.TableResolution) (*plan.ObjectRef, *plan.TableDef, error) {
	output.Reset()
	if name == c.index.Name {
		return &plan.ObjectRef{SchemaName: "tpch", ObjName: name, Obj: int64(c.index.TblId)}, plan2.DeepCopyTableDef(c.index, true), nil
	}
	return c.preparedStatsTestCompiler.ResolveIndexTableByRef(ref, name, snapshot, output)
}

func TestPreparedCompositePointEliminatedByIndexReusesCache(t *testing.T) {
	for _, tc := range []struct {
		name string
		hint string
	}{
		{name: "forced", hint: "force index(uk_part_supp)"},
		{name: "automatic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testPreparedCompositePointEliminatedByIndexReusesCache(t, tc.hint)
		})
	}
}

func testPreparedCompositePointEliminatedByIndexReusesCache(t *testing.T, hint string) {
	t.Helper()
	base, table := preparedCompositePointTable(t)
	table.Indexes = []*plan.IndexDef{{IndexName: "uk_part_supp", Parts: []string{"ps_partkey", "ps_suppkey"},
		IndexTableName: "uk_point_probe", TableExist: true, Unique: true}}
	keyType := plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	rowType := plan.Type{Id: int32(types.T_Rowid), Width: 16, NotNullable: true}
	index := &plan.TableDef{TableType: catalog.SystemIndexRel, TblId: 92365, Name: "uk_point_probe", Cols: []*plan.ColDef{
		{ColId: 0, Name: catalog.IndexTableIndexColName, OriginName: catalog.IndexTableIndexColName, Typ: keyType, Primary: true, Default: &plan.Default{}},
		{ColId: 1, Name: catalog.IndexTablePrimaryColName, OriginName: catalog.IndexTablePrimaryColName, Typ: table.Pkey.CompPkeyCol.Typ, Default: &plan.Default{}},
		{ColId: 2, Name: catalog.Row_ID, OriginName: catalog.Row_ID, Typ: rowType, Hidden: true, Default: &plan.Default{}},
	}, Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.IndexTableIndexColName, Cols: []uint64{0}, Names: []string{catalog.IndexTableIndexColName}},
		Name2ColIndex: map[string]int32{catalog.IndexTableIndexColName: 0, catalog.IndexTablePrimaryColName: 1}}
	ctx := &preparedCompositePointIndexCompiler{
		preparedStatsTestCompiler: &preparedStatsTestCompiler{
			preparedTestCompiler: &preparedTestCompiler{CompilerContext: base, proc: base.GetProcess()},
			stats:                &pbstats.StatsInfo{TableCnt: 128}, table: table,
		},
		index: index,
	}
	query := "select ps_partkey,ps_suppkey from partsupp " + hint + " where ps_partkey=? and ps_suppkey='s' and ((ps_suppkey=? and ps_comment=?) or (ps_suppkey=? and ps_comment=?))"
	ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQLWithCompilerContext(t, 3236501, query, ctx)
	ctx.proc, prepared.proc = cw.proc, cw.proc
	prepared.defaultDatabase = "tpch"
	t.Cleanup(func() {
		cw.proc.SetPrepareParams(nil)
		prepared.Close()
		ses.Close()
	})
	setPreparedCompositePointParams(t, prepared, cw, "u", "s", "r1", "s", "r2")
	first := executePreparedCompositePoint(t, ses, prepared, cw, execCtx, ctx)
	indexName := ""
	for _, node := range first.GetQuery().Nodes {
		if node.IndexScanInfo.IsIndexScan {
			indexName = node.IndexScanInfo.IndexName
		}
	}
	require.Equal(t, "uk_part_supp", indexName)
	require.Empty(t, preparedCompositePointPositions(t, first))
	require.Same(t, prepared, cw.runtimeCacheTarget)
	require.Same(t, first, cw.runtimeCachePlan)
	runtimeCompile := compile.NewCompile("", "", prepared.Sql, "", "", nil, cw.proc, prepared.PrepareStmt, false, nil, time.Now())
	require.True(t, cw.installRuntimeCacheCandidate(runtimeCompile))
	setPreparedCompositePointParams(t, prepared, cw, "u", "s", "r2", "s", "r3")
	require.Same(t, first, executePreparedCompositePoint(t, ses, prepared, cw, execCtx, ctx), "a same-type execution should reuse the admitted plan")
	require.Nil(t, cw.runtimeCacheTarget)
	value, err := cw.proc.GetPrepareParamsAt(2)
	require.NoError(t, err)
	require.Equal(t, "r2", string(value), "the reused plan must see the new binding")
}
