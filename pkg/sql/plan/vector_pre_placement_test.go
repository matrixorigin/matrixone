// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
package plan

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func requiredIVFPlacementPlan() *pb.Query {
	typ := pb.Type{Id: int32(types.T_int64)}
	col := func(rel, pos int32) *pb.Expr {
		return &pb.Expr{Typ: typ, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: rel, ColPos: pos}}}
	}
	equality := func() []*pb.Expr {
		return []*pb.Expr{{Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: "="}, Args: []*pb.Expr{col(0, 0), col(1, 0)}}}}}
	}
	obj := &pb.ObjectRef{Obj: 10, Db: 1}
	table := &pb.TableDef{Cols: []*pb.ColDef{{Name: "id", Typ: typ}, {Name: "other", Typ: typ}}, Name2ColIndex: map[string]int32{"id": 0, "other": 1}, Pkey: &pb.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"}}
	scan := func() *pb.Node {
		return &pb.Node{NodeType: pb.Node_TABLE_SCAN, ObjRef: obj, TableDef: table, ProjectList: []*pb.Expr{col(0, 0), col(0, 1)}, Stats: &pb.Stats{}}
	}
	spec := &pb.RuntimeFilterSpec{Tag: 7, MustApply: true, UseMembershipFilter: true, Expr: col(4, 0)}
	vector := &pb.Node{NodeType: pb.Node_VECTOR_INDEX_SCAN, TableDef: &pb.TableDef{Cols: []*pb.ColDef{{Name: "pkid", Typ: typ}}}, ProjectList: []*pb.Expr{col(0, 0)}, Stats: &pb.Stats{ForceOneCN: true}, RuntimeFilterProbeList: []*pb.RuntimeFilterSpec{spec}, VectorIndexScan: &pb.VectorIndexScan{SourceTable: obj, SourceTableDef: table, Index: &pb.IndexDef{IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(), Parts: []string{"v"}, IndexAlgoParams: `{"async":"false"}`}, QueryVector: &pb.Expr{Typ: pb.Type{Id: int32(types.T_array_float32)}, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Sval{Sval: "[0]"}}}}, ScanWork: &pb.VectorIndexScanWork{Objects: 2, Rows: 10, Blocks: 2, VectorBytesPerRow: 512}}}
	nodes := []*pb.Node{scan(), vector, scan(), {NodeType: pb.Node_JOIN, JoinType: pb.Node_SEMI, Children: []int32{1, 2}, OnList: equality(), ProjectList: []*pb.Expr{col(0, 0)}, RuntimeFilterBuildList: []*pb.RuntimeFilterSpec{spec}, Stats: &pb.Stats{}}, {NodeType: pb.Node_JOIN, JoinType: pb.Node_INNER, Children: []int32{0, 3}, OnList: equality(), ProjectList: []*pb.Expr{col(0, 0)}, Stats: &pb.Stats{}}}
	for id, n := range nodes {
		n.NodeId = int32(id)
	}
	return &pb.Query{StmtType: pb.Query_SELECT, Steps: []int32{4}, Nodes: nodes}
}

func TestRequiredIVFPlacementProvesPrimaryKeyAndCompleteTree(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*pb.Query)
		want   bool
	}{
		{"scalar PRE", func(q *pb.Query) {}, true},
		{"row dependent query vector", func(q *pb.Query) { q.Nodes[1].VectorIndexScan.QueryVector = q.Nodes[0].ProjectList[0] }, false},
		{"same typed non PK", func(q *pb.Query) { q.Nodes[3].OnList[0].GetF().Args[1].GetCol().ColPos = 1 }, false},
		{"computed PK", func(q *pb.Query) {
			q.Nodes[2].ProjectList[0] = &pb.Expr{Typ: q.Nodes[2].ProjectList[0].Typ, Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: "abs"}}}}
		}, false},
		{"shared access", func(q *pb.Query) { q.Nodes[3].Children[1] = 0 }, false},
		{"one object", func(q *pb.Query) { q.Nodes[1].VectorIndexScan.ScanWork.Objects = 1 }, false},
		{"missing stats", func(q *pb.Query) { q.Nodes[1].VectorIndexScan.ScanWork = nil }, false},
		{"foreign source", func(q *pb.Query) { q.Nodes[2].ObjRef = &pb.ObjectRef{Obj: 11, Db: 1} }, false},
		{"partial build", func(q *pb.Query) { q.Nodes[2].Limit = makePlan2Uint64ConstExprWithType(1) }, false},
		{"shuffle", func(q *pb.Query) { q.Nodes[3].Stats.HashmapStats = &pb.HashMapStats{Shuffle: true} }, false},
		{"optional filter", func(q *pb.Query) {
			q.Nodes[3].RuntimeFilterBuildList[0] = &pb.RuntimeFilterSpec{Tag: 7, UseMembershipFilter: true}
		}, false},
		{"unexplained force local", func(q *pb.Query) { q.Nodes[2].Stats.ForceOneCN = true }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := requiredIVFPlacementPlan()
			tc.mutate(q)
			_, _, _, ok := RequiredIVFPlacement(q)
			require.Equal(t, tc.want, ok)
			want := ExecTypeAP_ONECN
			if tc.want {
				want = ExecTypeAP_MULTICN
			}
			require.Equal(t, want, GetExecType(q, false, false))
		})
	}
}
