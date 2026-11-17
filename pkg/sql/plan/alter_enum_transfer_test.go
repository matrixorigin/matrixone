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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func alterEnumTransferTestSpec() *executor.AlterEnumTransferSpec {
	return &executor.AlterEnumTransferSpec{
		AccountID: 7, Database: "db", SourceID: 10, TargetID: 20,
		Source: &planpb.TableDef{Name: "src", Cols: []*planpb.ColDef{
			{Name: "e", Seqnum: 3, Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "A,a"}},
			{Name: "id", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_int64)}},
		}},
		Target: &planpb.TableDef{Name: "tmp", Cols: []*planpb.ColDef{
			{Name: "renamed", Seqnum: 3, Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "A,a", EnumCollation: types.EnumCollationGeneralCI}},
			{Name: "id", Seqnum: 0, Typ: planpb.Type{Id: int32(types.T_int64)}},
		}},
	}
}

func TestAlterEnumTransferScopedProof(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 7)
	spec := alterEnumTransferTestSpec()
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "insert into db.tmp (renamed,id) select e,id from db.src", 1)
	require.NoError(t, err)
	defer stmt.Free()
	marked, err := WithAlterEnumTransfer(ctx, stmt, spec)
	require.NoError(t, err)
	state := marked.Value(alterEnumTransferKey{}).(*alterEnumTransferBinding)
	require.Len(t, state.columns, 1)
	source := DeepCopyTableDef(spec.Source, true)
	source.TblId = spec.SourceID
	target := DeepCopyTableDef(spec.Target, true)
	target.TblId = spec.TargetID
	require.NoError(t, validateAlterEnumSource(marked, state.sourceNode, source, &planpb.ObjectRef{Obj: int64(spec.SourceID)}))
	require.NoError(t, validateAlterEnumTarget(marked, stmt.(*tree.Insert), target, &planpb.ObjectRef{Obj: int64(spec.TargetID)}))
	raw := &planpb.Expr{Typ: source.Cols[0].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 0}}}
	result, transferred, err := alterEnumTransferExpr(marked, state.rows, "renamed", raw, target.Cols[0].Typ)
	require.NoError(t, err)
	require.True(t, transferred)
	require.Equal(t, "cast", result.GetF().Func.ObjName)
	require.Equal(t, types.EnumCollationGeneralCI, result.Typ.EnumCollation)
	require.Equal(t, types.EnumCollationLegacy, raw.Typ.EnumCollation)
	_, transferred, err = alterEnumTransferExpr(WithoutAlterEnumTransfer(marked), state.rows, "renamed", raw, target.Cols[0].Typ)
	require.NoError(t, err)
	require.False(t, transferred)
	source.Cols[0].Seqnum++
	require.Error(t, validateAlterEnumSource(marked, state.sourceNode, source, &planpb.ObjectRef{Obj: 10}))
	target.Cols[0].Typ.Enumvalues = "a,A"
	require.Error(t, validateAlterEnumTarget(marked, stmt.(*tree.Insert), target, &planpb.ObjectRef{Obj: 20}))
}

func TestAlterEnumTransferRejectsWrongStatementScope(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 7)
	for _, sql := range []string{
		"insert into db.other (renamed) select e from db.src",
		"insert into db.tmp (renamed) select e from other.src",
		"insert into db.tmp (renamed) select e from db.src where id=1",
		"insert into db.tmp (renamed) select e from db.src join db.other on 1=1",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			_, err = WithAlterEnumTransfer(ctx, stmt, alterEnumTransferTestSpec())
			require.Error(t, err)
		})
	}
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "insert into db.tmp (renamed) select e from db.src", 1)
	require.NoError(t, err)
	defer stmt.Free()
	_, err = WithAlterEnumTransfer(defines.AttachAccountId(ctx, 8), stmt, alterEnumTransferTestSpec())
	require.Error(t, err)
}

func TestAlterEnumTransferEligibility(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 7)
	for _, tc := range []struct {
		name          string
		sql           string
		change        func(*executor.AlterEnumTransferSpec)
		wantTransfers int
		wantError     bool
	}{
		{name: "same members in framed encoding", sql: "insert into db.tmp (renamed) select e from db.src", change: func(s *executor.AlterEnumTransferSpec) {
			s.Target.Cols[0].Typ.Enumvalues = "2#1:A1:a"
			s.Target.Cols[0].Typ.EnumEncoding = types.EnumEncodingFramed
		}, wantTransfers: 1},
		{name: "reordered domain", sql: "insert into db.tmp (renamed) select e from db.src", change: func(s *executor.AlterEnumTransferSpec) { s.Target.Cols[0].Typ.Enumvalues = "a,A" }},
		{name: "new default constant", sql: "insert into db.tmp (renamed) select 1 from db.src"},
		{name: "non enum source", sql: "insert into db.tmp (renamed) select id from db.src"},
		{name: "generated target", sql: "insert into db.tmp (renamed) select e from db.src", change: func(s *executor.AlterEnumTransferSpec) { s.Target.Cols[0].GeneratedCol = &planpb.GeneratedCol{} }},
		{name: "stored generated source", sql: "insert into db.tmp (renamed) select e from db.src", change: func(s *executor.AlterEnumTransferSpec) {
			s.Source.Cols[0].GeneratedCol = &planpb.GeneratedCol{IsStored: true}
		}, wantTransfers: 1},
		{name: "virtual generated source", sql: "insert into db.tmp (renamed) select e from db.src", change: func(s *executor.AlterEnumTransferSpec) { s.Source.Cols[0].GeneratedCol = &planpb.GeneratedCol{} }, wantTransfers: 1},
		{name: "changed sequence", sql: "insert into db.tmp (renamed) select e from db.src", change: func(s *executor.AlterEnumTransferSpec) { s.Target.Cols[0].Seqnum++ }, wantError: true},
		{name: "duplicate source names", sql: "insert into db.tmp (renamed) select e from db.src", change: func(s *executor.AlterEnumTransferSpec) {
			s.Source.Cols = append(s.Source.Cols, &planpb.ColDef{Name: "E"})
		}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := alterEnumTransferTestSpec()
			if tc.change != nil {
				tc.change(spec)
			}
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			marked, err := WithAlterEnumTransfer(ctx, stmt, spec)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			state := marked.Value(alterEnumTransferKey{}).(*alterEnumTransferBinding)
			require.Len(t, state.columns, tc.wantTransfers)
		})
	}
}

func TestAlterEnumTransferPlanRequiresCastAndBaseScan(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 7)
	spec := alterEnumTransferTestSpec()
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "insert into db.tmp (renamed) select e from db.src", 1)
	require.NoError(t, err)
	defer stmt.Free()
	marked, err := WithAlterEnumTransfer(ctx, stmt, spec)
	require.NoError(t, err)
	state := marked.Value(alterEnumTransferKey{}).(*alterEnumTransferBinding)
	source := DeepCopyTableDef(spec.Source, true)
	source.TblId = spec.SourceID
	target := DeepCopyTableDef(spec.Target, true)
	target.TblId = spec.TargetID
	// A newly created replacement table may renumber columns after FIRST/AFTER
	// or earlier DROP operations; its identity is table ID, name and write slot.
	target.Cols[0].Seqnum = 0
	require.NoError(t, validateAlterEnumSource(marked, state.sourceNode, source, &planpb.ObjectRef{Obj: 10}))
	require.NoError(t, validateAlterEnumTarget(marked, stmt.(*tree.Insert), target, &planpb.ObjectRef{Obj: 20}))
	raw := &planpb.Expr{Typ: source.Cols[0].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}}
	cast, transferred, err := alterEnumTransferExpr(marked, state.rows, "renamed", raw, target.Cols[0].Typ)
	require.NoError(t, err)
	require.True(t, transferred)
	scan := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{Obj: 10}, TableDef: source, ProjectList: []*planpb.Expr{raw}}
	project := &planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: []*planpb.Expr{cast}}
	writer := &planpb.Node{NodeType: planpb.Node_MULTI_UPDATE, Children: []int32{1}, UpdateCtxList: []*planpb.UpdateCtx{{ObjRef: &planpb.ObjectRef{Obj: 20}, TableDef: target, InsertCols: []planpb.ColRef{{}}}}}
	query := &planpb.Query{Steps: []int32{2}, Nodes: []*planpb.Node{scan, project, writer}}
	p := &planpb.Plan{Plan: &planpb.Plan_Query{Query: query}}
	require.NoError(t, ValidateAlterEnumTransferPlan(marked, p))
	pre := &planpb.Node{NodeType: planpb.Node_PRE_INSERT, Children: []int32{1}, PreInsertCtx: &planpb.PreInsertCtx{Ref: &planpb.ObjectRef{Obj: 20}, TableDef: target}, ProjectList: []*planpb.Expr{{Typ: target.Cols[0].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}}}
	query.Nodes = append(query.Nodes, pre)
	writer.Children[0] = 3
	require.NoError(t, ValidateAlterEnumTransferPlan(marked, p), "ordinary columns pass through PRE_INSERT")
	pre.ProjectList[0].GetCol().RelPos = -1
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "a synthetic key is not the transferred column")
	pre.ProjectList[0].GetCol().RelPos = 0
	pre.ProjectList[0].GetCol().ColPos = 1
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "PRE_INSERT does not execute its projection metadata")
	pre.ProjectList[0].GetCol().ColPos = 0
	pre.PreInsertCtx = nil
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p))
	join := &planpb.Node{NodeType: planpb.Node_JOIN, JoinType: planpb.Node_DEDUP, OnDuplicateAction: planpb.Node_FAIL, Children: []int32{1, 1}, ProjectList: []*planpb.Expr{{Typ: target.Cols[0].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1}}}}}
	query.Nodes = append(query.Nodes, join)
	writer.Children[0] = 4
	require.NoError(t, ValidateAlterEnumTransferPlan(marked, p))
	join.ProjectList[0].GetCol().RelPos = 0
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "normal dedup fills the probe side with NULL")
	join.IsRightJoin = true
	require.NoError(t, ValidateAlterEnumTransferPlan(marked, p))
	join.ProjectList[0].GetCol().RelPos = 1
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "right dedup fills the build side with NULL")
	join.ProjectList[0].GetCol().RelPos = 0
	join.OnDuplicateAction = planpb.Node_UPDATE
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p))
	join.OnDuplicateAction = planpb.Node_FAIL
	join.ProjectList[0] = cast
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "JOIN cannot execute a CAST in its projection")
	writer.Children = []int32{0, 1}
	writer.UpdateCtxList[0].InsertCols[0].RelPos = 1
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "MULTI_UPDATE ignores RelPos and executes only child 0")
	writer.UpdateCtxList[0].InsertCols[0].RelPos = 0
	writer.Children = []int32{1}
	query.Steps[0] = 1
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "a cast and scan without a target write do not prove a transfer")
	query.Steps[0] = 2
	project.ProjectList[0] = raw
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "a removed cast must not silently fall back")
	project.ProjectList[0] = cast
	project.Children = []int32{0, 0}
	cast.GetF().Args[0].GetCol().RelPos = 1
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "PROJECT executes only child 0")
	cast.GetF().Args[0].GetCol().RelPos = 0
	project.Children = []int32{0}
	originalArg := cast.GetF().Args[0]
	cast.GetF().Args[0] = &planpb.Expr{Typ: source.Cols[0].Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "a same-typed constant is not the source column")
	cast.GetF().Args[0] = originalArg
	scan.IndexScanInfo.IsIndexScan = true
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p))
	scan.IndexScanInfo.IsIndexScan = false
	scan.ObjRef.Obj = 11
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p))
	require.NoError(t, ValidateAlterEnumTransferPlan(WithoutAlterEnumTransfer(marked), p))
}

func TestAlterEnumTransferPlanRejectsSwappedColumns(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 7)
	spec := alterEnumTransferTestSpec()
	source2 := *spec.Source.Cols[0]
	source2.Name, source2.Seqnum = "f", 4
	target2 := *spec.Target.Cols[0]
	target2.Name, target2.Seqnum = "other", 4
	spec.Source.Cols = append(spec.Source.Cols[:1], &source2)
	spec.Target.Cols = append(spec.Target.Cols[:1], &target2)
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "insert into db.tmp (renamed,other) select e,f from db.src", 1)
	require.NoError(t, err)
	defer stmt.Free()
	marked, err := WithAlterEnumTransfer(ctx, stmt, spec)
	require.NoError(t, err)
	state := marked.Value(alterEnumTransferKey{}).(*alterEnumTransferBinding)
	source := DeepCopyTableDef(spec.Source, true)
	source.TblId = spec.SourceID
	target := DeepCopyTableDef(spec.Target, true)
	target.TblId = spec.TargetID
	require.NoError(t, validateAlterEnumSource(marked, state.sourceNode, source, &planpb.ObjectRef{Obj: 10}))
	require.NoError(t, validateAlterEnumTarget(marked, stmt.(*tree.Insert), target, &planpb.ObjectRef{Obj: 20}))
	raw0 := &planpb.Expr{Typ: source.Cols[0].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	raw1 := &planpb.Expr{Typ: source.Cols[1].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}}
	cast0, _, err := alterEnumTransferExpr(marked, state.rows, "renamed", raw0, target.Cols[0].Typ)
	require.NoError(t, err)
	cast1, _, err := alterEnumTransferExpr(marked, state.rows, "other", raw1, target.Cols[1].Typ)
	require.NoError(t, err)
	scan := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{Obj: 10}, TableDef: source, ProjectList: []*planpb.Expr{raw0, raw1}}
	project := &planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: []*planpb.Expr{cast0, cast1}}
	writer := &planpb.Node{NodeType: planpb.Node_MULTI_UPDATE, Children: []int32{1}, UpdateCtxList: []*planpb.UpdateCtx{{ObjRef: &planpb.ObjectRef{Obj: 20}, TableDef: target, InsertCols: []planpb.ColRef{{ColPos: 0}, {ColPos: 1}}}}}
	p := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{Steps: []int32{2}, Nodes: []*planpb.Node{scan, project, writer}}}}
	require.NoError(t, ValidateAlterEnumTransferPlan(marked, p))
	writer.UpdateCtxList[0].InsertCols[0].ColPos = 1
	writer.UpdateCtxList[0].InsertCols[1].ColPos = 0
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "swapped same-domain target write slots must be rejected")
	writer.UpdateCtxList[0].InsertCols[0].ColPos = 0
	writer.UpdateCtxList[0].InsertCols[1].ColPos = 1
	cast0.GetF().Args[0] = raw1
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "same-domain cast from a different source must be rejected")
	cast0.GetF().Args[0] = raw0
	col := func(typ planpb.Type, pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos}}}
	}
	sink := &planpb.Node{NodeType: planpb.Node_SINK, Children: []int32{1}, ProjectList: []*planpb.Expr{col(target.Cols[0].Typ, 0), col(target.Cols[1].Typ, 1)}}
	sinkScan := &planpb.Node{NodeType: planpb.Node_SINK_SCAN, SourceStep: []int32{0}, ProjectList: []*planpb.Expr{col(target.Cols[0].Typ, 0), col(target.Cols[1].Typ, 1)}}
	query := p.GetQuery()
	query.Nodes = append(query.Nodes, sink, sinkScan)
	query.Steps = []int32{3, 2}
	writer.Children[0] = 4
	require.NoError(t, ValidateAlterEnumTransferPlan(marked, p))
	sink.ProjectList[0].GetCol().ColPos = 1
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "SINK projection metadata is not executed")
	sink.ProjectList[0].GetCol().ColPos = 0
	sinkScan.ProjectList[0].GetCol().ColPos = 1
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "sink scan must retain the physical column slot")
	sinkScan.ProjectList[0].GetCol().ColPos = 0
	sinkScan.SourceStep[0] = 1
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "a sink scan cannot use its own or a later step")
	sinkScan.SourceStep = nil
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p))
	sinkScan.SourceStep = []int32{0, 0}
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p))
	sinkScan.SourceStep = []int32{0}
	sink.RecursiveSink = true
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p))
	sink.RecursiveSink = false
	// A real sink scan may reorder the producer's output slots. The writer
	// must then use the matching reordered scan slots for each target column.
	sinkScan.ProjectList[0].GetCol().ColPos = 1
	sinkScan.ProjectList[1].GetCol().ColPos = 0
	writer.UpdateCtxList[0].InsertCols[0].ColPos = 1
	writer.UpdateCtxList[0].InsertCols[1].ColPos = 0
	require.NoError(t, ValidateAlterEnumTransferPlan(marked, p))
	query.Steps = append(query.Steps, 2)
	require.Error(t, ValidateAlterEnumTransferPlan(marked, p), "the target writer cannot be a duplicated step")
}

func TestNeedsAlterEnumTransferOrderedDomains(t *testing.T) {
	legacy := func(values string, collation uint32, seq uint32) *planpb.ColDef {
		return &planpb.ColDef{Name: "e", Seqnum: seq, Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: values, EnumCollation: collation}}
	}
	old := legacy("A,a", types.EnumCollationLegacy, 3)
	unchanged := legacy("A,a", types.EnumCollationLegacy, 3)
	changed := legacy("A,a", types.EnumCollationGeneralCI, 3)
	reordered := legacy("a,A", types.EnumCollationGeneralCI, 3)
	framed := legacy("2#1:A1:a", types.EnumCollationLegacy, 3)
	framed.Typ.EnumEncoding = types.EnumEncodingFramed
	malformed := legacy("2#1:A", types.EnumCollationGeneralCI, 3)
	malformed.Typ.EnumEncoding = types.EnumEncodingFramed
	hidden := *changed
	hidden.Hidden = true
	generated := *changed
	generated.GeneratedCol = &planpb.GeneratedCol{}
	wrongSequence := *changed
	wrongSequence.Seqnum = 9
	for _, tc := range []struct {
		name           string
		source, target []*planpb.ColDef
		want           bool
	}{
		{"unchanged", []*planpb.ColDef{old}, []*planpb.ColDef{unchanged}, false},
		{"collation changed", []*planpb.ColDef{old}, []*planpb.ColDef{changed}, true},
		{"encoding changed", []*planpb.ColDef{old}, []*planpb.ColDef{framed}, true},
		{"reordered domain", []*planpb.ColDef{old}, []*planpb.ColDef{reordered}, false},
		{"malformed domain", []*planpb.ColDef{old}, []*planpb.ColDef{malformed}, false},
		{"hidden", []*planpb.ColDef{old}, []*planpb.ColDef{&hidden}, false},
		{"generated target", []*planpb.ColDef{old}, []*planpb.ColDef{&generated}, false},
		{"wrong sequence still requires scoped proof", []*planpb.ColDef{old}, []*planpb.ColDef{&wrongSequence}, true},
		{"same domain with several identities", []*planpb.ColDef{old, changed}, []*planpb.ColDef{unchanged}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, NeedsAlterEnumTransfer(&planpb.TableDef{Cols: tc.source}, &planpb.TableDef{Cols: tc.target}))
		})
	}
}
