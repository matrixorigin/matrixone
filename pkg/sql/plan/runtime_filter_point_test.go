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
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	statsinfo "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func pointProofScan() *planpb.Node {
	typ := planpb.Type{Id: int32(types.T_int64)}
	return &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{7}, ObjRef: &planpb.ObjectRef{Obj: 42},
		TableDef: &planpb.TableDef{
			Name: "t", TableType: catalog.SystemOrdinaryRel,
			Cols: []*planpb.ColDef{{Name: "id", Typ: typ}}, Name2ColIndex: map[string]int32{"id": 0},
			Pkey: &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		},
		FilterList: []*planpb.Expr{pointProofEquality(0, makePlan2Int64ConstExprWithType(42))},
	}
}

func pointProofEquality(pos int32, value *planpb.Expr) *planpb.Expr {
	typ := planpb.Type{Id: int32(types.T_int64)}
	expr := makeRuntimeFilterTestEq(typ, 7, 8, pos, 0)
	expr.GetF().Args[1] = value
	return expr
}

func compositePointProofScan() *planpb.Node {
	scan := pointProofScan()
	typ := scan.TableDef.Cols[0].Typ
	scan.TableDef.Cols = append(scan.TableDef.Cols, &planpb.ColDef{Name: "part", Typ: typ},
		&planpb.ColDef{Name: catalog.CPrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_varchar)}, Hidden: true})
	scan.TableDef.Name2ColIndex["part"] = 1
	scan.TableDef.Name2ColIndex[catalog.CPrimaryKeyColName] = 2
	scan.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Names: []string{"id", "part"}}
	scan.FilterList = append(scan.FilterList, pointProofEquality(1, makePlan2Int64ConstExprWithType(9)))
	return scan
}

func TestSingletonPointInputProof(t *testing.T) {
	intType := planpb.Type{Id: int32(types.T_int64)}
	mustCast := func(expr *planpb.Expr) *planpb.Expr {
		t.Helper()
		cast, err := appendCastBeforeExpr(context.Background(), expr, intType)
		require.NoError(t, err)
		return cast
	}
	param := func(typ types.T, pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: planpb.Type{Id: int32(typ)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
	}
	textCast := mustCast(makePlan2StringConstExprWithType("42"))
	paramCast := mustCast(param(types.T_varchar, 0))
	timestampCast := mustCast(param(types.T_timestamp, 0))
	columnCast := mustCast(GetColExpr(intType, 7, 0))
	andExpr, err := BindFuncExprImplByPlanExpr(context.Background(), "and", []*planpb.Expr{
		pointProofEquality(0, makePlan2Int64ConstExprWithType(42)),
		pointProofEquality(1, makePlan2Int64ConstExprWithType(9)),
	})
	require.NoError(t, err)
	orExpr, err := BindFuncExprImplByPlanExpr(context.Background(), "or", []*planpb.Expr{
		pointProofEquality(0, makePlan2Int64ConstExprWithType(42)),
		pointProofEquality(1, makePlan2Int64ConstExprWithType(9)),
	})
	require.NoError(t, err)
	tests := []struct {
		name   string
		scan   func() *planpb.Node
		mutate func(*planpb.Node)
		want   bool
	}{
		{name: "integer point", want: true},
		{name: "parameter", want: true, mutate: func(n *planpb.Node) {
			typ := n.TableDef.Cols[0].Typ
			n.FilterList[0] = pointProofEquality(0, &planpb.Expr{Typ: typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}})
		}},
		{name: "cast literal", want: true, mutate: func(n *planpb.Node) {
			n.FilterList[0] = pointProofEquality(0, DeepCopyExpr(textCast))
		}},
		{name: "cast parameter", want: true, mutate: func(n *planpb.Node) {
			n.FilterList[0] = pointProofEquality(0, DeepCopyExpr(paramCast))
		}},
		{name: "timezone dependent cast parameter", mutate: func(n *planpb.Node) {
			n.FilterList[0] = pointProofEquality(0, DeepCopyExpr(timestampCast))
		}},
		{name: "null", want: true, mutate: func(n *planpb.Node) {
			typ := n.TableDef.Cols[0].Typ
			n.FilterList[0] = pointProofEquality(0, &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}})
		}},
		{name: "commuted", want: true, mutate: func(n *planpb.Node) {
			args := n.FilterList[0].GetF().Args
			args[0], args[1] = args[1], args[0]
		}},
		{name: "complete composite integer key", scan: compositePointProofScan, want: true},
		{name: "composite key with constant cast", scan: compositePointProofScan, want: true, mutate: func(n *planpb.Node) {
			n.FilterList[1] = pointProofEquality(1, DeepCopyExpr(textCast))
		}},
		{name: "composite parameters with commuted equality", scan: compositePointProofScan, want: true, mutate: func(n *planpb.Node) {
			n.FilterList[0] = pointProofEquality(0, param(types.T_int64, 0))
			n.FilterList[1] = pointProofEquality(1, DeepCopyExpr(paramCast))
			args := n.FilterList[1].GetF().Args
			args[0], args[1] = args[1], args[0]
		}},
		{name: "composite timezone dependent cast", scan: compositePointProofScan, mutate: func(n *planpb.Node) {
			n.FilterList[1] = pointProofEquality(1, DeepCopyExpr(timestampCast))
		}},
		{name: "repeated partial composite key", scan: compositePointProofScan, mutate: func(n *planpb.Node) {
			n.FilterList[1] = pointProofEquality(0, makePlan2Int64ConstExprWithType(9))
		}},
		{name: "nested and conservatively rejected", scan: compositePointProofScan, mutate: func(n *planpb.Node) {
			n.FilterList = []*planpb.Expr{DeepCopyExpr(andExpr)}
		}},
		{name: "or cannot establish a composite point", scan: compositePointProofScan, mutate: func(n *planpb.Node) {
			n.FilterList = []*planpb.Expr{DeepCopyExpr(orExpr)}
		}},
		{name: "partial composite key", scan: compositePointProofScan, mutate: func(n *planpb.Node) { n.FilterList = n.FilterList[:1] }},
		{name: "duplicate composite key metadata", scan: compositePointProofScan, mutate: func(n *planpb.Node) { n.TableDef.Pkey.Names[1] = "id" }},
		{name: "invalid composite physical key", scan: compositePointProofScan, mutate: func(n *planpb.Node) { n.TableDef.Cols[2].Name = "other" }},
		{name: "visible composite physical key", scan: compositePointProofScan, mutate: func(n *planpb.Node) { n.TableDef.Cols[2].Hidden = false }},
		{name: "no predicate", mutate: func(n *planpb.Node) { n.FilterList = nil }},
		{name: "range", mutate: func(n *planpb.Node) { n.FilterList[0].GetF().Func.ObjName = ">" }},
		{name: "different binding", mutate: func(n *planpb.Node) { n.FilterList[0].GetF().Args[0].GetCol().RelPos = 8 }},
		{name: "different value type", mutate: func(n *planpb.Node) { n.FilterList[0].GetF().Args[1].Typ.Id = int32(types.T_int32) }},
		{name: "cast column", mutate: func(n *planpb.Node) {
			n.FilterList[0].GetF().Args[0] = DeepCopyExpr(columnCast)
		}},
		{name: "float key", mutate: func(n *planpb.Node) { n.TableDef.Cols[0].Typ.Id = int32(types.T_float64) }},
		{name: "variable", mutate: func(n *planpb.Node) {
			typ := n.TableDef.Cols[0].Typ
			n.FilterList[0].GetF().Args[1] = &planpb.Expr{Typ: typ, Expr: &planpb.Expr_V{V: &planpb.VarRef{}}}
		}},
		{name: "temporary", mutate: func(n *planpb.Node) { n.TableDef.IsTemporary = true }},
		{name: "partition", mutate: func(n *planpb.Node) { n.TableDef.Partition = &planpb.Partition{} }},
		{name: "cluster", mutate: func(n *planpb.Node) { n.TableDef.ClusterBy = &planpb.ClusterByDef{} }},
		{name: "missing object", mutate: func(n *planpb.Node) { n.ObjRef = nil }},
		{name: "malformed equality", mutate: func(n *planpb.Node) { n.FilterList[0].GetF().Args = []*planpb.Expr{nil} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scan := pointProofScan()
			if tc.scan != nil {
				scan = tc.scan()
			}
			if tc.mutate != nil {
				tc.mutate(scan)
			}
			before := proto.Clone(scan)
			builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{
				scan, {NodeType: planpb.Node_PROJECT, Children: []int32{0}},
			}}}
			require.Equal(t, tc.want, builder.singletonPointInput(1, -1))
			require.True(t, proto.Equal(before, scan), "proof must not rewrite scan predicates")
		})
	}

	builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{{NodeType: planpb.Node_PROJECT, Children: []int32{0}}}}}
	require.False(t, builder.singletonPointInput(0, -1), "cyclic input must fail closed")
	require.False(t, builder.singletonPointInput(1, -1), "invalid node must fail closed")
	builder.qry.Nodes = []*planpb.Node{pointProofScan(), {NodeType: planpb.Node_JOIN, Children: []int32{0, 0}}}
	require.False(t, builder.singletonPointInput(1, -1), "a join can multiply the point input")
}

func pointRuntimeFilterBuilder(t *testing.T) *QueryBuilder {
	t.Helper()
	builder := newRuntimeFilterSingleTestBuilder(false)
	ctx := builder.compCtx.(*MockCompilerContext)
	proc := ctx.GetProcess()
	ctx.GetProcessFunc = func() *process.Process { return proc }
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	build := pointProofScan()
	build.BindingTags = []int32{2}
	build.FilterList[0].GetF().Args[0].GetCol().RelPos = 2
	build.Stats = DefaultStats()
	builder.qry.Nodes[1] = build
	join := builder.qry.Nodes[2]
	join.JoinType = planpb.Node_INNER
	join.Stats.HashmapStats.HashmapSize = 1000
	return builder
}

func pointNestedJoinBuilder(t *testing.T) *QueryBuilder {
	t.Helper()
	builder := pointRuntimeFilterBuilder(t)
	probe := proto.Clone(builder.qry.Nodes[0]).(*planpb.Node)
	probe.BindingTags = []int32{3}
	probe.ObjRef = &planpb.ObjectRef{Obj: 43}
	probe.TableDef.TableType = catalog.SystemOrdinaryRel
	probeExpr := GetColExpr(probe.TableDef.Cols[0].Typ, 3, 0)
	buildExpr := GetColExpr(probe.TableDef.Cols[0].Typ, -1, 0)
	probeFilter, buildFilter, ok := builder.makeExactRuntimeFilterPair(99, false, 10, probeExpr, buildExpr, false)
	require.True(t, ok)
	probe.RuntimeFilterProbeList = []*planpb.RuntimeFilterSpec{probeFilter}
	join := &planpb.Node{
		NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER, Children: []int32{3, 1},
		OnList:                 []*planpb.Expr{makeRuntimeFilterTestEq(probe.TableDef.Cols[0].Typ, 3, 2, 0, 0)},
		Stats:                  &planpb.Stats{HashmapStats: &planpb.HashMapStats{}},
		RuntimeFilterBuildList: []*planpb.RuntimeFilterSpec{buildFilter},
	}
	builder.qry.Nodes = append(builder.qry.Nodes, probe, join)
	builder.qry.Nodes[2].Children[1] = 4
	return builder
}

func TestSingletonNestedJoinProof(t *testing.T) {
	intType := planpb.Type{Id: int32(types.T_int64)}
	serial, err := BindFuncExprImplByPlanExpr(context.Background(), function.SerialFullFunctionName,
		[]*planpb.Expr{GetColExpr(intType, 2, 0)})
	require.NoError(t, err)
	castColumn, err := appendCastBeforeExpr(context.Background(), GetColExpr(intType, 2, 0), intType)
	require.NoError(t, err)
	setSerial := func(b *QueryBuilder) {
		b.qry.Nodes[3].TableDef.Cols[0].Typ = serial.Typ
		b.qry.Nodes[4].OnList[0].GetF().Args[0].Typ = serial.Typ
		b.qry.Nodes[4].OnList[0].GetF().Args[1] = DeepCopyExpr(serial)
		b.qry.Nodes[3].RuntimeFilterProbeList = nil
		b.qry.Nodes[4].RuntimeFilterBuildList = nil
	}
	setIndexProbe := func(b *QueryBuilder) {
		probe := b.qry.Nodes[3]
		source := b.qry.Nodes[1]
		name := catalog.SecondaryIndexTableNamePrefix + "test"
		probe.TableDef.TableType = catalog.SystemIndexRel
		probe.TableDef.Name = name
		probe.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: catalog.IndexTableIndexColName, Names: []string{catalog.IndexTableIndexColName}}
		probe.TableDef.Cols[0].Name = catalog.IndexTableIndexColName
		probe.TableDef.Name2ColIndex = map[string]int32{catalog.IndexTableIndexColName: 0}
		probe.ObjRef.SchemaName, probe.ObjRef.ObjName = "testdb", name
		source.ObjRef.SchemaName, source.ObjRef.ObjName = "testdb", source.TableDef.Name
		source.TableDef.Indexes = []*planpb.IndexDef{{IndexTableName: name, TableExist: true, Parts: []string{"state", catalog.CreateAlias("id")}}}
	}
	tests := []struct {
		name   string
		mutate func(*QueryBuilder)
		want   bool
	}{
		{name: "unique probe with its own filter", want: true},
		{name: "worker invariant serial build key", want: true, mutate: setSerial},
		{name: "regular index of point source", want: true, mutate: setIndexProbe},
		{name: "independent table schema versions", want: true, mutate: func(b *QueryBuilder) {
			setIndexProbe(b)
			b.qry.Nodes[1].ObjRef.Server = 7
			b.qry.Nodes[3].ObjRef.Server = 2
		}},
		{name: "master index of point source", mutate: func(b *QueryBuilder) {
			setIndexProbe(b)
			b.qry.Nodes[1].TableDef.Indexes[0].IndexAlgo = catalog.MOIndexMasterAlgo.ToString()
		}},
		{name: "index of a different schema", mutate: func(b *QueryBuilder) {
			setIndexProbe(b)
			b.qry.Nodes[3].ObjRef.SchemaName = "otherdb"
		}},
		{name: "historical source scan", mutate: func(b *QueryBuilder) {
			setIndexProbe(b)
			b.qry.Nodes[1].ScanSnapshot = &planpb.Snapshot{}
		}},
		{name: "historical index scan", mutate: func(b *QueryBuilder) {
			setIndexProbe(b)
			b.qry.Nodes[3].ScanSnapshot = &planpb.Snapshot{}
		}},
		{name: "index without full source primary key", mutate: func(b *QueryBuilder) {
			setIndexProbe(b)
			b.qry.Nodes[1].TableDef.Indexes[0].Parts = []string{"state"}
		}},
		{name: "ambiguous index metadata", mutate: func(b *QueryBuilder) {
			setIndexProbe(b)
			b.qry.Nodes[1].TableDef.Indexes = append(b.qry.Nodes[1].TableDef.Indexes, b.qry.Nodes[1].TableDef.Indexes[0])
		}},
		{name: "unique probe without filter", want: true, mutate: func(b *QueryBuilder) {
			b.qry.Nodes[3].RuntimeFilterProbeList = nil
			b.qry.Nodes[4].RuntimeFilterBuildList = nil
		}},
		{name: "unrelated probe dependency", mutate: func(b *QueryBuilder) { b.qry.Nodes[3].RuntimeFilterProbeList[0].Tag = 98 }},
		{name: "invalid matching filter pair", mutate: func(b *QueryBuilder) { b.qry.Nodes[4].RuntimeFilterBuildList[0].BuildExpr = nil }},
		{name: "unrelated build dependency", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[4].RuntimeFilterBuildList = append(b.qry.Nodes[4].RuntimeFilterBuildList, &planpb.RuntimeFilterSpec{Tag: 98})
		}},
		{name: "source scan dependency", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[1].RuntimeFilterProbeList = []*planpb.RuntimeFilterSpec{{Tag: 98}}
		}},
		{name: "outer probe is reused", mutate: func(b *QueryBuilder) { b.qry.Nodes[4].Children[0] = 0 }},
		{name: "probe and build share a scan", mutate: func(b *QueryBuilder) { b.qry.Nodes[4].Children[1] = 3 }},
		{name: "nested outer join", mutate: func(b *QueryBuilder) { b.qry.Nodes[4].JoinType = planpb.Node_LEFT }},
		{name: "nested right join", mutate: func(b *QueryBuilder) { b.qry.Nodes[4].IsRightJoin = true }},
		{name: "nested shuffle", mutate: func(b *QueryBuilder) { b.qry.Nodes[4].Stats.HashmapStats.Shuffle = true }},
		{name: "partitioned probe", mutate: func(b *QueryBuilder) { b.qry.Nodes[3].TableDef.Partition = &planpb.Partition{} }},
		{name: "filtered probe", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[3].FilterList = []*planpb.Expr{makePlan2BoolConstExprWithType(true)}
		}},
		{name: "nonregular internal index", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[3].TableDef.TableType = catalog.SystemIndexRel
			b.qry.Nodes[3].TableDef.Name = catalog.FullTextIndexTableNamePrefix + "test"
		}},
		{name: "fake primary key", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[3].TableDef.Pkey.PkeyColName = catalog.FakePrimaryKeyColName
		}},
		{name: "incomplete probe primary key", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[3].TableDef.Pkey.Names = append(b.qry.Nodes[3].TableDef.Pkey.Names, "other")
		}},
		{name: "probe referenced on both sides", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[4].OnList[0].GetF().Args[1].GetCol().RelPos = 3
		}},
		{name: "build expression outside subtree", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[4].OnList[0].GetF().Args[1].GetCol().RelPos = 42
		}},
		{name: "mismatched equality type", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[4].OnList[0].GetF().Args[1].Typ.Id = int32(types.T_float64)
		}},
		{name: "unproved cast on build key", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[4].OnList[0].GetF().Args[1] = DeepCopyExpr(castColumn)
		}},
		{name: "serial contains probe reference", mutate: func(b *QueryBuilder) {
			setSerial(b)
			b.qry.Nodes[4].OnList[0].GetF().Args[1].GetF().Args[0].GetCol().RelPos = 3
		}},
		{name: "serial contains a variable", mutate: func(b *QueryBuilder) {
			setSerial(b)
			b.qry.Nodes[4].OnList[0].GetF().Args[1].GetF().Args[0] = &planpb.Expr{
				Typ: intType, Expr: &planpb.Expr_V{V: &planpb.VarRef{}},
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder := pointNestedJoinBuilder(t)
			if tc.mutate != nil {
				tc.mutate(builder)
			}
			require.Equal(t, tc.want, builder.singletonPointInput(4, 0))
		})
	}
}

func TestSingletonRuntimeFilterAdmission(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*QueryBuilder)
		want   bool
	}{
		{name: "default stats", want: true},
		{name: "stale high build estimate", want: true, mutate: func(b *QueryBuilder) {
			b.qry.Nodes[1].Stats.Outcnt = 6_000_000
			b.qry.Nodes[2].Stats.HashmapStats.HashmapSize = 6_000_000
		}},
		{name: "stale primary key estimate", want: true, mutate: func(b *QueryBuilder) {
			b.qry.Nodes[1].Stats.Outcnt = 321_000
			b.qry.Nodes[2].Stats.HashmapStats.HashOnPK = true
		}},
		{name: "nonpoint build", mutate: func(b *QueryBuilder) { b.qry.Nodes[1].FilterList = nil }},
		{name: "outer join", mutate: func(b *QueryBuilder) { b.qry.Nodes[2].JoinType = planpb.Node_LEFT }},
		{name: "right inner join", mutate: func(b *QueryBuilder) { b.qry.Nodes[2].IsRightJoin = true }},
		{name: "probe predicate", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[0].FilterList = []*planpb.Expr{makePlan2BoolConstExprWithType(true)}
		}},
		{name: "probe block filter", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[0].BlockFilterList = []*planpb.Expr{makePlan2BoolConstExprWithType(true)}
		}},
		{name: "computed probe projection", mutate: func(b *QueryBuilder) {
			b.qry.Nodes[0].ProjectList = []*planpb.Expr{makePlan2BoolConstExprWithType(true)}
		}},
		{name: "probe has filter dependency", mutate: func(b *QueryBuilder) { b.qry.Nodes[0].RuntimeFilterProbeList = []*planpb.RuntimeFilterSpec{{Tag: 99}} }},
		{name: "runtime filter disabled", mutate: func(b *QueryBuilder) { b.optimizerHints = &OptimizerHints{runtimeFilter: 1} }},
		{name: "not full probe primary key", mutate: func(b *QueryBuilder) { b.qry.Nodes[0].TableDef.Pkey.PkeyColName = "other" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder := pointRuntimeFilterBuilder(t)
			if tc.mutate != nil {
				tc.mutate(builder)
			}
			probe := builder.qry.Nodes[0]
			join := builder.qry.Nodes[2]
			before := DeepCopyStats(probe.Stats)
			builder.generateRuntimeFilters(2)
			if tc.want {
				require.Len(t, join.RuntimeFilterBuildList, 1)
				require.Len(t, probe.RuntimeFilterProbeList, 1)
				require.Equal(t, join.RuntimeFilterBuildList[0].Tag, probe.RuntimeFilterProbeList[0].Tag)
				require.Equal(t, before, probe.Stats, "singleton admission must not fabricate scan statistics")
				builder.forceJoinOnOneCN(2, false)
				require.False(t, probe.Stats.ForceOneCN)
			} else {
				require.Empty(t, join.RuntimeFilterBuildList)
			}
		})
	}

	t.Run("ordinary cost admission is unchanged", func(t *testing.T) {
		builder := pointRuntimeFilterBuilder(t)
		builder.qry.Nodes[0].FilterList = []*planpb.Expr{makePlan2BoolConstExprWithType(true)}
		builder.qry.Nodes[2].Stats.HashmapStats.HashmapSize = 1
		builder.generateRuntimeFilters(2)
		require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
	})
	t.Run("shuffle retains its existing local behavior", func(t *testing.T) {
		builder := pointRuntimeFilterBuilder(t)
		builder.qry.Nodes[2].Stats.HashmapStats.Shuffle = true
		builder.generateRuntimeFilters(2)
		require.Empty(t, builder.qry.Nodes[0].RuntimeFilterProbeList)
		require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
		require.Nil(t, builder.qry.Nodes[2].RuntimeFilterBuildList[0].Expr)
	})
}

type pointUpdateStatsContext struct{ CompilerContext }

func (c pointUpdateStatsContext) Stats(obj *ObjectRef, snapshot *Snapshot) (*statsinfo.StatsInfo, error) {
	if obj != nil && (obj.ObjName == catalog.SecondaryIndexTableNamePrefix+"idx_hint_a" ||
		obj.ObjName == catalog.SecondaryIndexTableNamePrefix+"idx_hint_ab") {
		return &statsinfo.StatsInfo{TableCnt: 50000, BlockNumber: 7}, nil
	}
	return c.CompilerContext.Stats(obj, snapshot)
}

func TestRuntimeFilterForPrimaryKeyPointUpdate(t *testing.T) {
	mock := NewMockOptimizer(true)
	proc := mock.ctxt.GetProcess()
	mock.ctxt.GetProcessFunc = func() *process.Process { return proc }
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	addIndexHintChoiceTableForTest(mock)
	table := mock.ctxt.tables["index_hint_t"]
	table.Indexes = table.Indexes[:2]
	for _, index := range table.Indexes {
		oldName := index.IndexTableName
		name := catalog.SecondaryIndexTableNamePrefix + oldName
		index.IndexTableName = name
		indexTable := mock.ctxt.tables[oldName]
		indexTable.Name = name
		mock.ctxt.tables[name] = indexTable
		object := *mock.ctxt.objects[oldName]
		object.ObjName = name
		mock.ctxt.objects[name] = &object
		mock.ctxt.id2name[indexTable.TblId] = name
		mock.ctxt.pks[name] = mock.ctxt.pks[oldName]
	}

	ctx := pointUpdateStatsContext{CompilerContext: &mock.ctxt}
	stmts, err := mysql.Parse(ctx.GetContext(), "UPDATE index_hint_t SET a = 9 WHERE id = 42", 1)
	require.NoError(t, err)
	built, err := BuildPlan(ctx, stmts[0], false)
	require.NoError(t, err)
	query := built.GetQuery()
	require.NotNil(t, query)
	probes := make(map[string]*planpb.Node)
	buildTags := make(map[int32]bool)
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil &&
			(node.TableDef.Name == catalog.SecondaryIndexTableNamePrefix+"idx_hint_a" ||
				node.TableDef.Name == catalog.SecondaryIndexTableNamePrefix+"idx_hint_ab") {
			probes[node.TableDef.Name] = node
		}
		for _, spec := range node.RuntimeFilterBuildList {
			buildTags[spec.Tag] = true
		}
	}
	for _, name := range []string{catalog.SecondaryIndexTableNamePrefix + "idx_hint_a", catalog.SecondaryIndexTableNamePrefix + "idx_hint_ab"} {
		probe := probes[name]
		require.NotNil(t, probe, "the maintained index scan must be reachable: %s", name)
		require.Len(t, probe.RuntimeFilterProbeList, 1, "the singleton maintenance join should filter %s", name)
		require.True(t, buildTags[probe.RuntimeFilterProbeList[0].Tag], "probe must have a matching build")
	}
}
