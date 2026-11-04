// Copyright 2021 Matrix Origin
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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestLoadAssignmentIgnorePolicy(t *testing.T) {
	require.False(t, loadAssignmentIgnore(nil))
	require.False(t, loadAssignmentIgnore(&tree.Load{DuplicateHandling: &tree.DuplicateKeyError{}}))
	require.True(t, loadAssignmentIgnore(&tree.Load{DuplicateHandling: &tree.DuplicateKeyIgnore{}}))
	require.True(t, loadAssignmentIgnore(&tree.Load{Local: true, DuplicateHandling: &tree.DuplicateKeyError{}}))
	require.False(t, loadAssignmentIgnore(&tree.Load{Local: true, DuplicateHandling: &tree.DuplicateKeyReplace{}}))
	require.False(t, loadAssignmentIgnore(&tree.Load{
		DuplicateHandling: &tree.DuplicateKeyError{},
		Param: &tree.ExternParam{ExParamConst: tree.ExParamConst{
			Tail: &tree.TailParameter{IgnoredLines: 1},
		}},
	}))
}

func TestLocalLoadTimeAssignmentAndDecoderPolicies(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy%v", legacy), func(t *testing.T) {
			proc := newPlanTestProcess(t)
			ctx := NewMockCompilerContext(true, proc)
			create, err := mysql.ParseOne(t.Context(), "create table local_time_load(v time(6))", 1)
			require.NoError(t, err)
			defer create.Free()
			ddl, err := BuildPlan(ctx, create, false)
			require.NoError(t, err)
			table := ddl.GetDdl().GetCreateTable().TableDef
			table.TblId = 990004
			ctx.tables["local_time_load"] = table
			ctx.objects["local_time_load"] = &planpb.ObjectRef{Obj: int64(table.TblId), SchemaName: "tpch", ObjName: "local_time_load"}
			parsed, err := mysql.ParseOne(t.Context(), "load data local infile 'client.csv' into table local_time_load", 1)
			require.NoError(t, err)
			defer parsed.Free()
			stmt := parsed.(*tree.Load)
			build := func() (*planpb.Plan, error) {
				if legacy {
					return buildLoad(stmt, ctx, false)
				}
				return BuildPlan(ctx, stmt, false)
			}
			// Reuse one parsed statement through IGNORE, implicit LOCAL, REPLACE
			// and implicit LOCAL again: decoder policy and assignment are distinct.
			for _, tc := range []struct {
				duplicate tree.DuplicateKey
				cast      string
				global    bool
			}{
				{&tree.DuplicateKeyIgnore{}, "cast_ignore", true},
				{&tree.DuplicateKeyError{}, "cast_ignore", false},
				{&tree.DuplicateKeyReplace{}, "cast_assign", false},
				{&tree.DuplicateKeyError{}, "cast_ignore", false},
			} {
				stmt.DuplicateHandling = tc.duplicate
				require.Equal(t, tc.global, tree.IsIgnoreStatement(stmt))
				result, err := build()
				require.NoError(t, err)
				var assignment *planpb.Expr
				for _, n := range result.GetQuery().Nodes {
					for _, expr := range n.ProjectList {
						if fn := expr.GetF(); fn != nil && expr.Typ.Id == int32(types.T_time) && (fn.Func.ObjName == "cast_assign" || fn.Func.ObjName == "cast_ignore") {
							assignment = expr
						}
					}
				}
				require.NotNil(t, assignment)
				require.Equal(t, tc.cast, assignment.GetF().Func.ObjName)
				require.Equal(t, int32(types.T_varchar), assignment.GetF().Args[0].Typ.Id)
			}
			// LOCAL Parquet is rejected by both real planner entries before a
			// reader can run; its nonexistent implicit policy needs no runtime bit.
			stmt.Param.Format = tree.PARQUET
			_, err = build()
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNYI), "%v", err)
			require.ErrorContains(t, err, "load parquet local")
		})
	}
}

func TestApplyLoadAssignmentCasts(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	setLoadAssignmentProtocol(t, builder.compCtx.GetProcess())
	tinyText := planpb.Type{Id: int32(types.T_text), Width: types.MaxTinyTextLen}
	tinyBlob := planpb.Type{Id: int32(types.T_blob), Width: types.MaxTinyTextLen}
	intType := planpb.Type{Id: int32(types.T_int32)}
	timeType := planpb.Type{Id: int32(types.T_time), Scale: 6}
	mediumType := planpb.Type{Id: int32(types.T_int32), Width: 24, Scale: -1}
	unsignedMediumType := planpb.Type{Id: int32(types.T_uint32), Width: 24, Scale: -1}
	tableDef := &planpb.TableDef{Cols: []*planpb.ColDef{
		{Name: "txt", Typ: tinyText},
		{Name: "blob", Typ: tinyBlob},
		{Name: "n", Typ: intType},
		{Name: "t", Typ: timeType},
		{Name: "m", Typ: mediumType},
		{Name: "u", Typ: unsignedMediumType},
	}}
	makeCol := func(typ planpb.Type, pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos}}}
	}
	exprs := map[string]*planpb.Expr{
		"txt":  makeCol(tinyText, 2),
		"blob": makeCol(tinyBlob, 0),
		"n":    makeCol(intType, 1),
		"t":    makeCol(timeType, 3),
		"m":    makeCol(mediumType, 4),
		"u":    makeCol(unsignedMediumType, 5),
	}
	plainInt := exprs["n"]

	require.NoError(t, builder.applyLoadAssignmentCasts(tableDef, exprs, false))
	require.Equal(t, "cast_assign", exprs["txt"].GetF().GetFunc().GetObjName())
	require.Equal(t, int32(2), exprs["txt"].GetF().GetArgs()[0].GetCol().GetColPos())
	require.Equal(t, "cast_assign", exprs["blob"].GetF().GetFunc().GetObjName())
	require.Equal(t, int32(0), exprs["blob"].GetF().GetArgs()[0].GetCol().GetColPos())
	require.Nil(t, exprs["n"].GetF())
	require.Same(t, plainInt, exprs["n"])
	require.Equal(t, "cast_assign", exprs["t"].GetF().GetFunc().GetObjName())
	require.Equal(t, int32(3), exprs["t"].GetF().GetArgs()[0].GetCol().GetColPos())
	for _, name := range []string{"m", "u"} {
		require.Equal(t, "cast_assign", exprs[name].GetF().GetFunc().GetObjName())
		require.Equal(t, int32(24), exprs[name].GetF().GetArgs()[1].Typ.Width)
	}

	ignored := map[string]*planpb.Expr{"txt": makeCol(tinyText, 4), "t": makeCol(timeType, 0),
		"m": makeCol(mediumType, 1), "u": makeCol(unsignedMediumType, 2), "n": plainInt}
	require.NoError(t, builder.applyLoadAssignmentCasts(tableDef, ignored, true))
	require.Equal(t, "cast_ignore", ignored["txt"].GetF().GetFunc().GetObjName())
	require.Equal(t, "cast_ignore", ignored["t"].GetF().GetFunc().GetObjName())
	for _, name := range []string{"m", "u"} {
		require.Equal(t, "cast_ignore", ignored[name].GetF().GetFunc().GetObjName())
		require.Equal(t, int32(24), ignored[name].GetF().GetArgs()[1].Typ.Width)
	}
	require.Same(t, plainInt, ignored["n"])
}

// MEDIUMINT assignment needs both the cluster gate and the persisted authoring
// floor. Keep fixture state explicit and restore it before the process closes.
func setLoadAssignmentProtocol(t *testing.T, proc *process.Process) {
	t.Helper()
	rt := moruntime.ServiceRuntime(proc.GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		old, existed := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if existed {
				rt.SetGlobalVariables(key, old)
			} else {
				rt.CompareAndDeleteGlobalVariables(key, defines.MORPCLatestVersion)
			}
		})
		rt.SetGlobalVariables(key, defines.MORPCLatestVersion)
	}
}

func TestParallelLoadTimeAndMediumIntKeepOriginalInput(t *testing.T) {
	typesToLoad := []planpb.Type{
		{Id: int32(types.T_time), Scale: 6},
		{Id: int32(types.T_int32), Width: 24, Scale: -1},
		{Id: int32(types.T_uint32), Width: 24, Scale: -1},
		{Id: int32(types.T_int32), Width: 32, Scale: -1},
	}
	names := []string{"t", "m", "u", "n"}
	table := &planpb.TableDef{}
	node := &planpb.Node{}
	supplied := make(map[string]int32)
	for i, typ := range typesToLoad {
		table.Cols = append(table.Cols, &planpb.ColDef{Name: names[i], Typ: typ})
		node.ProjectList = append(node.ProjectList, &planpb.Expr{Typ: typ,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: int32(i)}}})
		supplied[names[i]] = int32(i)
	}
	staged := makeCastExpr(&tree.Load{Param: &tree.ExternParam{}}, "rows.csv", table, node, supplied)
	for i := 0; i < 3; i++ {
		require.Equal(t, int32(types.T_varchar), staged[i].Typ.Id)
		require.Nil(t, staged[i].GetF(), names[i]+" must reach assignment as raw input")
		require.Equal(t, int32(i), staged[i].GetCol().GetColPos())
	}
	require.Equal(t, int32(types.T_int32), staged[3].Typ.Id)
	require.Equal(t, "cast", staged[3].GetF().GetFunc().GetObjName())
}

func TestLegacyLoadTimeAndMediumIntAssignmentBeforeDefaults(t *testing.T) {
	proc := newPlanTestProcess(t)
	setLoadAssignmentProtocol(t, proc)
	ctx := NewMockCompilerContext(true, proc)
	create, err := mysql.ParseOne(t.Context(),
		"create table mixed_load(t time(6), m mediumint, u mediumint unsigned, n int, d time(6) default (t))", 1)
	require.NoError(t, err)
	defer create.Free()
	ddl, err := BuildPlan(ctx, create, false)
	require.NoError(t, err)
	table := ddl.GetDdl().GetCreateTable().TableDef
	table.TblId = 990002
	table.Name2ColIndex = make(map[string]int32, len(table.Cols))
	for i, col := range table.Cols {
		table.Name2ColIndex[col.Name] = int32(i)
	}
	ctx.tables["mixed_load"] = table
	ctx.objects["mixed_load"] = &planpb.ObjectRef{Obj: int64(table.TblId), SchemaName: "tpch", ObjName: "mixed_load"}
	load, err := mysql.ParseOne(t.Context(),
		"load data inline format='csv', data='00:00:00,1,2,3' into table mixed_load fields terminated by ',' (t,m,u,n)", 1)
	require.NoError(t, err)
	defer load.Free()
	// Call the legacy planner directly: the modern binder's helper does not
	// exercise its separate assignment projection loop.
	result, err := buildLoad(load.(*tree.Load), ctx, false)
	require.NoError(t, err)
	query := result.GetQuery()
	var assigned *planpb.Node
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_PROJECT && len(node.Children) == 1 &&
			query.Nodes[node.Children[0]].NodeType == planpb.Node_EXTERNAL_SCAN {
			assigned = node
			break
		}
	}
	require.NotNil(t, assigned)
	require.Equal(t, int32(types.T_varchar), assigned.ProjectList[table.Name2ColIndex["t"]].GetF().Args[0].Typ.Id)
	for _, name := range []string{"t", "m", "u"} {
		pos := table.Name2ColIndex[name]
		require.Equal(t, "cast_assign", assigned.ProjectList[pos].GetF().GetFunc().GetObjName(), name)
	}
	require.Nil(t, assigned.ProjectList[table.Name2ColIndex["n"]].GetF())
	d := table.Name2ColIndex["d"]
	require.True(t, assigned.ProjectList[d].GetLit().Isnull, "dependent default is materialized later")
	var defaultUsesAssigned bool
	for _, node := range query.Nodes {
		if len(node.Children) == 1 && node.Children[0] == assigned.NodeId && len(node.ProjectList) > int(d) {
			defaultUsesAssigned = len(collectRefColPos(node.ProjectList[d])) > 0
		}
	}
	require.True(t, defaultUsesAssigned, "default must consume the assigned child projection")
}

func TestTextLoadTimeScanTypes(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy%v/columnlist%v", legacy, explicit), func(t *testing.T) {
				proc := newPlanTestProcess(t)
				setLoadAssignmentProtocol(t, proc)
				ctx := NewMockCompilerContext(true, proc)
				create, err := mysql.ParseOne(t.Context(), "create table text_time_load(t time(6), n int)", 1)
				require.NoError(t, err)
				defer create.Free()
				ddl, err := BuildPlan(ctx, create, false)
				require.NoError(t, err)
				table := ddl.GetDdl().GetCreateTable().TableDef
				table.TblId = 990003
				table.Name2ColIndex = make(map[string]int32, len(table.Cols))
				for i, col := range table.Cols {
					table.Name2ColIndex[col.Name] = int32(i)
				}
				ctx.tables["text_time_load"] = table
				ctx.objects["text_time_load"] = &planpb.ObjectRef{Obj: int64(table.TblId), SchemaName: "tpch", ObjName: "text_time_load"}
				sql := "load data inline format='csv', data='2562047788:00:00,1' into table text_time_load fields terminated by ','"
				if explicit {
					sql = "load data inline format='csv', data='1,2562047788:00:00' into table text_time_load fields terminated by ',' (n,t)"
				}
				load, err := mysql.ParseOne(t.Context(), sql, 1)
				require.NoError(t, err)
				defer load.Free()
				var result *planpb.Plan
				if legacy {
					result, err = buildLoad(load.(*tree.Load), ctx, false)
				} else {
					result, err = BuildPlan(ctx, load, false)
				}
				require.NoError(t, err)
				var found bool
				for _, node := range result.GetQuery().Nodes {
					if node.NodeType != planpb.Node_EXTERNAL_SCAN {
						continue
					}
					for _, expr := range node.ProjectList {
						col := expr.GetCol()
						if col != nil && node.TableDef.Cols[col.ColPos].Name == "t" {
							found = true
							require.Equal(t, int32(types.T_time), node.TableDef.Cols[col.ColPos].Typ.Id, "decoder retains target metadata")
							require.Equal(t, int32(types.T_varchar), expr.Typ.Id, "scan metadata matches its physical text vector")
						}
					}
				}
				require.True(t, found)
			})
		}
	}
	for _, format := range []string{tree.PARQUET, tree.ARROW} {
		typ := planpb.Type{Id: int32(types.T_time), Scale: 6}
		require.Equal(t, typ, loadTimeInputType(format, typ), "typed columnar sources retain their declared type")
	}
}

// Exercise the actual expression executor: metadata alone cannot prove that
// decoded TIME is assignable, and parallel staging must retain the raw text.
func TestLoadTimeAssignmentExecution(t *testing.T) {
	for _, staged := range []bool{false, true} {
		name := "typed"
		if staged {
			name = "parallel_text"
		}
		t.Run(name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_INSERT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, false)
			proc := builder.compCtx.GetProcess()
			mode := "STRICT_TRANS_TABLES"
			proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return mode, nil })
			sink := &loadAssignmentWarningSink{}
			proc.WarningSink = sink
			typ := planpb.Type{Id: int32(types.T_time), Scale: 6}
			table := &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "v", Typ: typ}}}
			expr := &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
			if staged {
				expr = makeCastExpr(&tree.Load{Param: &tree.ExternParam{}}, "rows.csv", table,
					&planpb.Node{ProjectList: []*planpb.Expr{expr}}, map[string]int32{"v": 0})[0]
				require.Equal(t, int32(types.T_varchar), expr.Typ.Id)
				require.Nil(t, expr.GetF(), "assignment must see the unclamped text")
			}
			exprs := map[string]*planpb.Expr{"v": expr}
			require.NoError(t, builder.applyLoadAssignmentCasts(table, exprs, false))
			executor, err := colexec.NewExpressionExecutor(proc, exprs["v"])
			require.NoError(t, err)
			defer executor.Free()
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewVec(types.T_time.ToTypeWithScale(6))
			if staged {
				input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			}
			defer input.Clean(proc.Mp())
			for _, value := range []types.Time{-3020399000001, 3020399000001, -3020400000000, 3020400000000} {
				input.Vecs[0].Reset(*input.Vecs[0].GetType())
				if staged {
					require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(value.String2(6)), false, proc.Mp()))
				} else {
					require.NoError(t, vector.AppendFixed(input.Vecs[0], value, false, proc.Mp()))
				}
				input.SetRowCount(1)
				mode = "STRICT_TRANS_TABLES"
				sink.codes = nil
				_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
				require.Empty(t, sink.codes)
				mode = ""
				result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				want := types.Time(3020399000000)
				if value < 0 {
					want = -want
				}
				require.Equal(t, []types.Time{want}, vector.MustFixedColWithTypeCheck[types.Time](result))
				require.Equal(t, []uint16{moerr.ER_WARN_DATA_OUT_OF_RANGE}, sink.codes)
			}
		})
	}
}
