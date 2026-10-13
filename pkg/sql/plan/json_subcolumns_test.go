// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package plan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJSONSubcolumnProjectionProof(t *testing.T) {
	col := &pb.Expr{Typ: pb.Type{Id: int32(types.T_json)}, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 7, ColPos: 0}}}
	for _, tc := range []struct {
		name   string
		exprs  []*pb.Expr
		accept bool
	}{
		{"fixed", []*pb.Expr{jpCallExpr("json_extract", col, jpStrLit("$.trace.id"))}, true},
		{"multiple", []*pb.Expr{jpCallExpr("json_extract_string", col, jpStrLit("$.trace.id")), jpCallExpr("json_extract_float64", col, jpStrLit("$.latency"))}, true},
		{"full", []*pb.Expr{col, jpCallExpr("json_extract", col, jpStrLit("$.trace.id"))}, false},
		{"root", []*pb.Expr{jpCallExpr("json_extract", col, jpStrLit("$"))}, false},
		{"wildcard", []*pb.Expr{jpCallExpr("json_extract", col, jpStrLit("$.*"))}, false},
		{"array", []*pb.Expr{jpCallExpr("json_extract", col, jpStrLit("$.tags[0]"))}, false},
		{"dynamic", []*pb.Expr{jpCallExpr("json_extract", col, col)}, false},
		{"unknown", []*pb.Expr{jpCallExpr("json_type", col)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, BindingTags: []int32{7}, TableDef: &pb.TableDef{Cols: []*pb.ColDef{{Seqnum: 9, Typ: pb.Type{Id: int32(types.T_json)}}}}}
			project := &pb.Node{NodeType: pb.Node_PROJECT, Children: []int32{0}, ProjectList: tc.exprs}
			q := &pb.Query{Steps: []int32{1}, Nodes: []*pb.Node{scan, project}}
			proveJSONPathProjection(q)
			require.Equal(t, tc.accept, scan.ExtraOptions != "")
			if tc.accept {
				p, e := objectio.DecodeJSONPathProjection(scan.ExtraOptions)
				require.NoError(t, e)
				require.NotEmpty(t, p[9])
			}
			scan.ExtraOptions = ""
			scan.FilterList = []*pb.Expr{jpCallExpr("json_type", col)}
			proveJSONPathProjection(q)
			require.Empty(t, scan.ExtraOptions)
		})
	}
}
