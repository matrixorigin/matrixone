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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
)

// RequiredIVFPlacement recognizes the generated scalar PRE access tree. The
// returned scans retain ForceOneCN for scan scheduling; only its query-wide
// aggregation may be omitted. No proof or placement state survives compilation.
func RequiredIVFPlacement(q *plan.Query) (vectorID, membershipID int32, localScans map[int32]struct{}, eligible bool) {
	if q == nil || q.StmtType != plan.Query_SELECT || len(q.Steps) != 1 {
		return
	}
	hasRequired := false
	for _, n := range q.Nodes {
		if n.GetNodeType() != plan.Node_VECTOR_INDEX_SCAN {
			continue
		}
		for _, rf := range n.RuntimeFilterProbeList {
			hasRequired = hasRequired || (rf != nil && rf.MustApply && rf.UseMembershipFilter)
		}
	}
	if !hasRequired || QueryContainsSequenceFunction(q) {
		return
	}
	nodes := make(map[int32]*plan.Node)
	var walk func(int32) bool
	walk = func(id int32) bool {
		if id < 0 || int(id) >= len(q.Nodes) || q.Nodes[id] == nil {
			return false
		}
		if _, duplicate := nodes[id]; duplicate {
			return false
		}
		n := q.Nodes[id]
		nodes[id] = n
		for _, child := range n.Children {
			if !walk(child) {
				return false
			}
		}
		return true
	}
	if !walk(q.Steps[0]) {
		return
	}
	unwrap := func(id int32, allowSort bool) int32 {
		for {
			n := nodes[id]
			if n == nil || (n.NodeType != plan.Node_PROJECT && n.NodeType != plan.Node_FILTER && (!allowSort || n.NodeType != plan.Node_SORT)) || len(n.Children) != 1 {
				return id
			}
			id = n.Children[0]
		}
	}
	root := nodes[unwrap(q.Steps[0], true)]
	if root == nil || root.NodeType != plan.Node_JOIN || root.JoinType != plan.Node_INNER || root.IsRightJoin || len(root.Children) != 2 || vectorPREShuffled(root) {
		return
	}
	membershipID = unwrap(root.Children[1], false)
	member := nodes[membershipID]
	if member == nil || member.NodeType != plan.Node_JOIN || member.JoinType != plan.Node_SEMI || member.IsRightJoin || len(member.Children) != 2 || vectorPREShuffled(member) {
		return
	}
	vectorID = member.Children[0]
	v := nodes[vectorID]
	if v == nil || v.NodeType != plan.Node_VECTOR_INDEX_SCAN || len(v.Children) != 0 || v.VectorIndexScan == nil {
		return
	}
	spec := v.VectorIndexScan
	if len(spec.Index.GetParts()) != 1 || spec.QueryVector == nil || !rule.IsConstant(spec.QueryVector, true) || spec.Index == nil || !catalog.IsIvfIndexAlgo(spec.Index.IndexAlgo) || spec.SourceTable == nil || spec.SourceTableDef == nil || spec.FirstRoundLimit != nil || spec.BucketExpandStep != 0 ||
		spec.ScanWork == nil || spec.ScanWork.Objects < 2 || !positiveScanWork(spec.ScanWork.Rows) || !positiveScanWork(spec.ScanWork.VectorBytesPerRow) || spec.ScanWork.Blocks <= 0 {
		return
	}
	async, err := catalog.IndexParamAsync(spec.Index.IndexAlgoParams)
	if err != nil || async {
		return
	}
	pk := spec.SourceTableDef.Pkey
	if pk == nil || len(pk.Names) != 1 {
		return
	}
	pkPos, ok := spec.SourceTableDef.Name2ColIndex[pk.PkeyColName]
	if !ok || pkPos < 0 || int(pkPos) >= len(spec.SourceTableDef.Cols) {
		return
	}
	pkType := spec.SourceTableDef.Cols[pkPos].Typ.Id
	pkOutputs := make(map[int32]map[int32]bool)
	var outputs func(int32) map[int32]bool
	outputs = func(id int32) map[int32]bool {
		if known, ok := pkOutputs[id]; ok {
			return known
		}
		n := nodes[id]
		result := make(map[int32]bool)
		pkOutputs[id] = result
		if n == nil {
			return result
		}
		for i, expr := range n.ProjectList {
			col := expr.GetCol()
			if col == nil || expr.Typ.Id != pkType {
				continue
			}
			if n.NodeType == plan.Node_TABLE_SCAN || n.NodeType == plan.Node_VECTOR_INDEX_SCAN {
				if col.RelPos != 0 || col.ColPos < 0 || int(col.ColPos) >= len(n.GetTableDef().GetCols()) {
					continue
				}
				name := n.TableDef.Cols[col.ColPos].Name
				expected := pk.PkeyColName
				if n.NodeType == plan.Node_VECTOR_INDEX_SCAN {
					expected = "pkid"
				} else if n.IndexScanInfo.IsIndexScan {
					expected = catalog.IndexTablePrimaryColName
				}
				result[int32(i)] = name == expected
			} else if col.RelPos >= 0 && int(col.RelPos) < len(n.Children) {
				result[int32(i)] = outputs(n.Children[col.RelPos])[col.ColPos]
			}
		}
		return result
	}
	pkJoin := func(n *plan.Node) bool {
		if !vectorPREPKEquality(n, pkType) || len(n.Children) != 2 {
			return false
		}
		for _, arg := range n.OnList[0].GetF().Args {
			col := arg.GetCol()
			if col.RelPos < 0 || col.RelPos > 1 || !outputs(n.Children[col.RelPos])[col.ColPos] {
				return false
			}
		}
		return true
	}
	if !types.T(pkType).IsInteger() || !pkJoin(root) || !pkJoin(member) {
		return
	}
	if len(member.RuntimeFilterBuildList) != 1 || len(v.RuntimeFilterProbeList) != 1 {
		return
	}
	build, probe := member.RuntimeFilterBuildList[0], v.RuntimeFilterProbeList[0]
	if build == nil || probe == nil || build.Tag <= 0 || build.Tag != probe.Tag || !build.MustApply || !probe.MustApply || !build.UseMembershipFilter || !probe.UseMembershipFilter || build.Expr.GetCol() == nil || probe.Expr.GetCol() == nil || build.Expr.Typ.Id != pkType || probe.Expr.Typ.Id != pkType {
		return
	}
	builders, consumers := 0, 0
	for _, n := range nodes {
		if n != v && n.NodeType == plan.Node_VECTOR_INDEX_SCAN {
			return
		}
		for _, rf := range n.RuntimeFilterBuildList {
			if rf != nil && rf.Tag == build.Tag {
				builders++
			}
		}
		for _, rf := range n.RuntimeFilterProbeList {
			if rf != nil && rf.Tag == build.Tag {
				consumers++
			}
		}
	}
	if builders != 1 || consumers != 1 {
		return
	}
	localScans = make(map[int32]struct{})
	indexTags := make(map[int32]bool)
	var access func(int32, bool) bool
	access = func(id int32, inIndex bool) bool {
		n := nodes[id]
		if n == nil || n.Limit != nil || n.Offset != nil {
			return false
		}
		switch n.NodeType {
		case plan.Node_PROJECT, plan.Node_FILTER:
			return len(n.Children) == 1 && access(n.Children[0], inIndex)
		case plan.Node_JOIN:
			if n.JoinType != plan.Node_INDEX || n.IsRightJoin || len(n.Children) != 2 || vectorPREShuffled(n) || !pkJoin(n) {
				return false
			}
			// INDEX-specific filters must remain within this local access subtree.
			for _, rf := range n.RuntimeFilterBuildList {
				if rf == nil || rf.Tag <= 0 || rf.Tag == build.Tag {
					return false
				}
				inTree := make(map[int32]struct{})
				var subtree func(int32)
				subtree = func(child int32) {
					inTree[child] = struct{}{}
					for _, next := range nodes[child].Children {
						subtree(next)
					}
				}
				subtree(id)
				found := false
				for nid, other := range nodes {
					for _, endpoint := range other.RuntimeFilterProbeList {
						if endpoint != nil && endpoint.Tag == rf.Tag {
							if _, inside := inTree[nid]; !inside {
								return false
							}
							found = true
						}
					}
					if nid != id {
						for _, endpoint := range other.RuntimeFilterBuildList {
							if endpoint != nil && endpoint.Tag == rf.Tag {
								return false
							}
						}
					}
				}
				if !found {
					return false
				}
				indexTags[rf.Tag] = true
			}
			return access(n.Children[0], true) && access(n.Children[1], true)
		case plan.Node_TABLE_SCAN:
			if len(n.Children) != 0 || n.TableDef == nil || n.TableDef.TblFunc != nil {
				return false
			}
			sameSource := sameVectorPREObject(n.ObjRef, spec.SourceTable)
			if !sameSource {
				if !n.IndexScanInfo.IsIndexScan || !sameVectorPREObject(n.ParentObjRef, spec.SourceTable) || n.ObjRef == nil {
					return false
				}
				known := false
				for _, index := range spec.SourceTableDef.Indexes {
					if index != nil && index.TableExist && catalog.IsRegularIndexAlgo(index.IndexAlgo) && index.IndexName == n.IndexScanInfo.IndexName &&
						index.IndexTableName == n.IndexScanInfo.IndexTableName && index.IndexTableName == n.ObjRef.ObjName {
						known = true
						break
					}
				}
				if !known {
					return false
				}
			}
			if n.GetStats().GetForceOneCN() {
				if !inIndex {
					return false
				}
				localScans[id] = struct{}{}
			}
			return true
		default:
			return false
		}
	}
	if !access(root.Children[0], false) || !access(member.Children[1], false) {
		return vectorID, membershipID, nil, false
	}
	// A plain row fetch receives the outer INNER's optional PK filter. Prove
	// its source and unique endpoints; encoding remains the existing RF owner's.
	var outerProbe *plan.Node
	var outerTag int32
	if len(root.RuntimeFilterBuildList) > 1 {
		return vectorID, membershipID, nil, false
	}
	if len(root.RuntimeFilterBuildList) == 1 {
		outerBuild := root.RuntimeFilterBuildList[0]
		rowFetch := nodes[root.Children[0]]
		optionalPK := func(rf *plan.RuntimeFilterSpec) bool {
			return rf != nil && !rf.MustApply && !rf.UseMembershipFilter && !rf.ScalarPredicate && !rf.MatchPrefix && !rf.NotOnPk
		}
		pkSlot := func(expr *plan.Expr) bool {
			col := expr.GetCol()
			return col != nil && col.RelPos == -1 && col.ColPos == 0 && expr.Typ.Id == pkType
		}
		if !optionalPK(outerBuild) || outerBuild.Tag <= 0 || outerBuild.Tag == build.Tag || indexTags[outerBuild.Tag] || rowFetch.NodeType != plan.Node_TABLE_SCAN ||
			(outerBuild.BuildExpr == nil && outerBuild.Expr == nil) || (outerBuild.BuildExpr != nil && !pkSlot(outerBuild.BuildExpr)) || (outerBuild.Expr != nil && !pkSlot(outerBuild.Expr)) {
			return vectorID, membershipID, nil, false
		}
		outerTag = outerBuild.Tag
		buildCount, probeCount := 0, 0
		for _, n := range q.Nodes {
			if n == nil {
				continue
			}
			for _, rf := range n.RuntimeFilterBuildList {
				if rf != nil && rf.Tag == outerTag {
					if n != root {
						return vectorID, membershipID, nil, false
					}
					buildCount++
				}
			}
			for _, rf := range n.RuntimeFilterProbeList {
				if rf == nil || rf.Tag != outerTag {
					continue
				}
				col := rf.Expr.GetCol()
				if n != rowFetch || !optionalPK(rf) || col == nil || col.RelPos != 0 || col.ColPos < 0 || int(col.ColPos) >= len(n.TableDef.Cols) || rf.Expr.Typ.Id != pkType {
					return vectorID, membershipID, nil, false
				}
				expected := pk.PkeyColName
				if n.IndexScanInfo.IsIndexScan {
					expected = catalog.IndexTablePrimaryColName
				}
				column := n.TableDef.Cols[col.ColPos]
				if column == nil || column.Name != expected || column.Typ.Id != pkType {
					return vectorID, membershipID, nil, false
				}
				probeCount++
			}
		}
		if buildCount != 1 || probeCount != 1 {
			return vectorID, membershipID, nil, false
		}
		outerProbe = rowFetch
	}
	for _, n := range nodes {
		if n.NodeType != plan.Node_TABLE_SCAN {
			continue
		}
		for _, rf := range n.RuntimeFilterProbeList {
			if rf == nil || (!indexTags[rf.Tag] && !(n == outerProbe && rf.Tag == outerTag)) {
				return vectorID, membershipID, nil, false
			}
		}
	}
	// Never turn an unexplained whole-query restriction into a local scan hint.
	for id, n := range q.Nodes {
		if n == nil || !n.GetStats().GetForceOneCN() || int32(id) == vectorID {
			continue
		}
		if _, proved := localScans[int32(id)]; !proved {
			return vectorID, membershipID, nil, false
		}
	}
	return vectorID, membershipID, localScans, true
}

func vectorPREShuffled(n *plan.Node) bool {
	return n.GetStats().GetHashmapStats().GetShuffle()
}

func vectorPREPKEquality(n *plan.Node, pkType int32) bool {
	if len(n.OnList) != 1 {
		return false
	}
	f := n.OnList[0].GetF()
	if f == nil || f.Func == nil || f.Func.ObjName != "=" || len(f.Args) != 2 {
		return false
	}
	a, b := f.Args[0], f.Args[1]
	return a != nil && b != nil && a.GetCol() != nil && b.GetCol() != nil && a.Typ.Id == pkType && b.Typ.Id == pkType && a.GetCol().RelPos != b.GetCol().RelPos
}

func sameVectorPREObject(a, b *plan.ObjectRef) bool {
	return a != nil && b != nil && a.Obj != 0 && a.Obj == b.Obj && a.Db == b.Db && a.Server == b.Server && a.GetPubInfo().GetTenantId() == b.GetPubInfo().GetTenantId()
}
