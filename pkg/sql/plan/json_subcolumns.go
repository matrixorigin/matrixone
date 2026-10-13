// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package plan

import (
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"sort"
	"strings"
)

// proveJSONPathProjection runs before column remapping. Every consumption of a
// scan's JSON value must be a fixed object-key extraction. Forwarded bare values,
// unsupported expression/node forms and multi-step queries decline projection.
func proveJSONPathProjection(q *pb.Query) {
	if len(q.Steps) != 1 {
		return
	}
	nodes := make([]*pb.Node, 0)
	var collect func(int32) bool
	collect = func(id int32) bool {
		n := q.Nodes[id]
		switch n.NodeType {
		case pb.Node_TABLE_SCAN, pb.Node_PROJECT, pb.Node_FILTER, pb.Node_SORT, pb.Node_AGG:
		default:
			return false
		}
		if n.NodeType == pb.Node_TABLE_SCAN {
			if len(n.Children) != 0 || len(n.BindingTags) != 1 || n.TableDef == nil {
				return false
			}
		} else if len(n.Children) != 1 {
			return false
		}
		nodes = append(nodes, n)
		for _, child := range n.Children {
			if !collect(child) {
				return false
			}
		}
		return true
	}
	if !collect(q.Steps[0]) {
		return
	}
	scans := make(map[[2]int32]*pb.ColDef)
	var scan *pb.Node
	for _, n := range nodes {
		if n.NodeType == pb.Node_TABLE_SCAN {
			if scan != nil {
				return
			}
			scan = n
			for i, c := range n.TableDef.Cols {
				if c.Typ.Id == int32(types.T_json) {
					scans[[2]int32{n.BindingTags[0], int32(i)}] = c
				}
			}
		}
	}
	if scan == nil || scan.ExtraOptions != "" {
		return
	}
	unsafe := make(map[[2]int32]bool)
	paths := make(map[[2]int32]map[string]bool)
	unsupported := false
	var visit func(*pb.Expr)
	visit = func(e *pb.Expr) {
		if e == nil {
			return
		}
		switch x := e.Expr.(type) {
		case *pb.Expr_Col:
			k := [2]int32{x.Col.RelPos, x.Col.ColPos}
			if scans[k] != nil {
				unsafe[k] = true
			}
		case *pb.Expr_F:
			f := x.F
			name := strings.ToLower(f.Func.ObjName)
			if (name == "json_extract" || name == "json_extract_string" || name == "json_extract_float64") && len(f.Args) == 2 && f.Args[0].GetCol() != nil && f.Args[1].GetLit() != nil {
				col := f.Args[0].GetCol()
				k := [2]int32{col.RelPos, col.ColPos}
				if scans[k] != nil && !f.Args[1].GetLit().Isnull {
					if s, ok := f.Args[1].GetLit().Value.(*pb.Literal_Sval); ok {
						p, err := bytejson.ParseJsonPath(s.Sval)
						if err == nil {
							if _, ok = p.ObjectKeys(); ok {
								if paths[k] == nil {
									paths[k] = make(map[string]bool)
								}
								paths[k][s.Sval] = true
								return
							}
						}
					}
				}
			}
			for _, arg := range f.Args {
				visit(arg)
			}
		case *pb.Expr_List:
			for _, arg := range x.List.List {
				visit(arg)
			}
		case *pb.Expr_Lit, *pb.Expr_P, *pb.Expr_V, *pb.Expr_T, *pb.Expr_Max, *pb.Expr_Vec, *pb.Expr_Fold:
		default:
			unsupported = true
		}
	}
	for _, n := range nodes {
		if n.NodeType != pb.Node_TABLE_SCAN {
			for _, e := range n.ProjectList {
				visit(e)
			}
		}
		for _, es := range [][]*pb.Expr{n.FilterList, n.BlockFilterList, n.GroupBy, n.AggList, n.OnList} {
			for _, e := range es {
				visit(e)
			}
		}
		for _, o := range n.OrderBy {
			visit(o.Expr)
		}
		visit(n.Limit)
		visit(n.Offset)
	}
	if unsupported {
		return
	}
	projection := make(objectio.JSONPathProjection)
	for k, ps := range paths {
		if !unsafe[k] && len(ps) <= 32 && scans[k].Seqnum < uint32(objectio.SEQNUM_UPPER) {
			seq := uint16(scans[k].Seqnum)
			for p := range ps {
				projection[seq] = append(projection[seq], p)
			}
		}
	}
	if len(projection) > 0 {
		for seq := range projection {
			sort.Strings(projection[seq])
		}
		scan.ExtraOptions = objectio.EncodeJSONPathProjection(projection)
	}
}
