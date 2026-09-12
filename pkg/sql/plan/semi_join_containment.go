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
	"github.com/gogo/protobuf/proto"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

const maxSemiContainmentScans = 6

type semiContainmentRelation struct {
	scans      []*planpb.Node
	tagToScan  map[int32]int
	predicates []*planpb.Expr
}

type semiContainmentKey struct {
	outer *planpb.Expr
	build *planpb.Expr
	fn    *planpb.ObjectRef
}

// removeImpliedSemiJoins applies the set identity
//
//	(X SEMI B) SEMI A = X SEMI B, when keys(B) is a subset of keys(A).
//
// The proof below is deliberately limited to conjunctive inner-join relations.
// B must contain an injective copy of every base scan and every total predicate
// in A, and its membership key must be equal to A's key inside B. Extra scans
// and predicates can only reduce B's key set. The rule does not use cardinality
// estimates, table names, or query-specific constants.
func (builder *QueryBuilder) removeImpliedSemiJoins(nodeID int32) int32 {
	if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return nodeID
	}
	node := builder.qry.Nodes[nodeID]
	if node == nil {
		return nodeID
	}
	for i, childID := range node.Children {
		node.Children[i] = builder.removeImpliedSemiJoins(childID)
	}
	for builder.impliedSemiJoin(node) {
		nodeID = node.Children[0]
		node = builder.qry.Nodes[nodeID]
	}
	return nodeID
}

func (builder *QueryBuilder) impliedSemiJoin(outer *planpb.Node) bool {
	if !plainLogicalSemiJoin(outer) {
		return false
	}
	inner := builder.qry.Nodes[outer.Children[0]]
	if !plainLogicalSemiJoin(inner) {
		return false
	}

	subset, ok := builder.readSemiContainmentRelation(outer.Children[1])
	if !ok {
		return false
	}
	superset, ok := builder.readSemiContainmentRelation(inner.Children[1])
	if !ok || len(subset.scans) > len(superset.scans) {
		return false
	}
	outerKeys, ok := readSemiContainmentKeys(outer.OnList, subset.tagToScan)
	if !ok {
		return false
	}
	innerKeys, ok := readSemiContainmentKeys(inner.OnList, superset.tagToScan)
	if !ok {
		return false
	}

	mapping := make(map[int32]int32, len(subset.scans))
	used := make([]bool, len(superset.scans))
	var search func(int) bool
	search = func(pos int) bool {
		if pos == len(subset.scans) {
			return semiContainmentMappingProvesSubset(
				subset, superset, outerKeys, innerKeys, mapping,
			)
		}
		left := subset.scans[pos]
		leftTag := left.BindingTags[0]
		for rightPos, right := range superset.scans {
			if used[rightPos] || !sameSemiContainmentScan(left, right) {
				continue
			}
			used[rightPos] = true
			mapping[leftTag] = right.BindingTags[0]
			if search(pos + 1) {
				return true
			}
			delete(mapping, leftTag)
			used[rightPos] = false
		}
		return false
	}
	return search(0)
}

func plainLogicalSemiJoin(node *planpb.Node) bool {
	return node != nil && node.NodeType == planpb.Node_JOIN &&
		node.JoinType == planpb.Node_SEMI &&
		len(node.Children) == 2 && len(node.OnList) > 0 &&
		len(node.ProjectList) == 0 && len(node.FilterList) == 0 &&
		len(node.OrderBy) == 0 && node.Limit == nil && node.Offset == nil &&
		len(node.BindingTags) == 0 && len(node.RuntimeFilterProbeList) == 0 &&
		len(node.RuntimeFilterBuildList) == 0 && len(node.SendMsgList) == 0 &&
		len(node.RecvMsgList) == 0
}

func (builder *QueryBuilder) readSemiContainmentRelation(
	nodeID int32,
) (*semiContainmentRelation, bool) {
	relation := &semiContainmentRelation{tagToScan: make(map[int32]int)}
	seen := make(map[int32]bool)
	var visit func(int32) bool
	visit = func(id int32) bool {
		if id < 0 || int(id) >= len(builder.qry.Nodes) || seen[id] {
			return false
		}
		seen[id] = true
		node := builder.qry.Nodes[id]
		if node == nil || node.Limit != nil || node.Offset != nil ||
			len(node.OrderBy) != 0 || len(node.RuntimeFilterProbeList) != 0 ||
			len(node.RuntimeFilterBuildList) != 0 {
			return false
		}
		switch node.NodeType {
		case planpb.Node_FILTER:
			if len(node.Children) != 1 || node.FilterIsBarrier ||
				len(node.ProjectList) != 0 {
				return false
			}
			relation.predicates = append(relation.predicates,
				splitPlanConjunctions(node.FilterList)...)
			return visit(node.Children[0])
		case planpb.Node_JOIN:
			if node.JoinType != planpb.Node_INNER ||
				len(node.Children) != 2 || len(node.ProjectList) != 0 ||
				len(node.FilterList) != 0 {
				return false
			}
			relation.predicates = append(relation.predicates,
				splitPlanConjunctions(node.OnList)...)
			return visit(node.Children[0]) && visit(node.Children[1])
		case planpb.Node_TABLE_SCAN:
			if len(node.Children) != 0 || len(node.BindingTags) != 1 ||
				node.ObjRef == nil || node.TableDef == nil ||
				len(relation.scans) >= maxSemiContainmentScans {
				return false
			}
			tag := node.BindingTags[0]
			if _, exists := relation.tagToScan[tag]; exists {
				return false
			}
			relation.tagToScan[tag] = len(relation.scans)
			relation.scans = append(relation.scans, node)
			relation.predicates = append(relation.predicates,
				splitPlanConjunctions(node.FilterList)...)
			return true
		default:
			return false
		}
	}
	if !visit(nodeID) || len(relation.scans) == 0 {
		return nil, false
	}
	return relation, true
}

func sameSemiContainmentScan(left, right *planpb.Node) bool {
	return left != nil && right != nil && objectRefEqual(left.ObjRef, right.ObjRef) &&
		proto.Equal(left.ScanSnapshot, right.ScanSnapshot)
}

func readSemiContainmentKeys(
	predicates []*planpb.Expr,
	buildTags map[int32]int,
) ([]semiContainmentKey, bool) {
	conjuncts := splitPlanConjunctions(predicates)
	keys := make([]semiContainmentKey, 0, len(conjuncts))
	for _, predicate := range conjuncts {
		fn := predicate.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) != 2 ||
			!IsEqualFunc(fn.Func.Obj) || !isTruncationSafePredicateExpr(predicate) {
			return nil, false
		}
		left, right := fn.Args[0], fn.Args[1]
		leftCol, rightCol := left.GetCol(), right.GetCol()
		if leftCol == nil || rightCol == nil {
			return nil, false
		}
		_, leftBuild := buildTags[leftCol.RelPos]
		_, rightBuild := buildTags[rightCol.RelPos]
		if leftBuild == rightBuild {
			return nil, false
		}
		key := semiContainmentKey{outer: left, build: right, fn: fn.Func}
		if leftBuild {
			key.outer, key.build = right, left
		}
		keys = append(keys, key)
	}
	return keys, len(keys) > 0
}

func semiContainmentMappingProvesSubset(
	subset, superset *semiContainmentRelation,
	outerKeys, innerKeys []semiContainmentKey,
	mapping map[int32]int32,
) bool {
	for _, predicate := range subset.predicates {
		if !isTruncationSafePredicateExpr(predicate) {
			return false
		}
		mapped, ok := remapSemiContainmentExpr(predicate, mapping, true)
		if !ok || !semiContainmentExprListContains(superset.predicates, mapped) {
			return false
		}
	}

	equalities := newSemiContainmentEqualities(superset.predicates)
	for _, outerKey := range outerKeys {
		mappedBuild, ok := remapSemiContainmentExpr(outerKey.build, mapping, true)
		if !ok {
			return false
		}
		matched := false
		for _, innerKey := range innerKeys {
			if !objectRefEqual(outerKey.fn, innerKey.fn) ||
				!exprStructuralEqual(outerKey.outer, innerKey.outer) {
				continue
			}
			if equalities.equivalent(mappedBuild, innerKey.build) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func remapSemiContainmentExpr(
	expr *planpb.Expr,
	mapping map[int32]int32,
	requireMapped bool,
) (*planpb.Expr, bool) {
	result := DeepCopyExpr(expr)
	ok := true
	var visit func(*planpb.Expr)
	visit = func(current *planpb.Expr) {
		if current == nil || !ok {
			return
		}
		if col := current.GetCol(); col != nil {
			mapped, exists := mapping[col.RelPos]
			if !exists {
				if requireMapped {
					ok = false
				}
				return
			}
			col.RelPos = mapped
			return
		}
		if fn := current.GetF(); fn != nil {
			for _, arg := range fn.Args {
				visit(arg)
			}
			return
		}
		if list := current.GetList(); list != nil {
			for _, item := range list.List {
				visit(item)
			}
		}
	}
	visit(result)
	return result, ok
}

func semiContainmentExprListContains(list []*planpb.Expr, target *planpb.Expr) bool {
	for _, candidate := range list {
		if exprStructuralEqual(candidate, target) ||
			semiContainmentCommutedEqual(candidate, target) {
			return true
		}
	}
	return false
}

func semiContainmentCommutedEqual(left, right *planpb.Expr) bool {
	lf, rf := left.GetF(), right.GetF()
	if lf == nil || rf == nil || lf.Func == nil || rf.Func == nil ||
		len(lf.Args) != 2 || len(rf.Args) != 2 ||
		!objectRefEqual(lf.Func, rf.Func) {
		return false
	}
	functionID, _ := function.DecodeOverloadID(lf.Func.Obj)
	if functionID != function.EQUAL && functionID != function.NOT_EQUAL {
		return false
	}
	return exprStructuralEqual(lf.Args[0], rf.Args[1]) &&
		exprStructuralEqual(lf.Args[1], rf.Args[0])
}

type semiContainmentEqualities struct {
	parent map[[2]int32][2]int32
}

func newSemiContainmentEqualities(predicates []*planpb.Expr) *semiContainmentEqualities {
	equalities := &semiContainmentEqualities{parent: make(map[[2]int32][2]int32)}
	for _, predicate := range predicates {
		fn := predicate.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) != 2 ||
			!IsEqualFunc(fn.Func.Obj) || !isTruncationSafePredicateExpr(predicate) {
			continue
		}
		left, right := fn.Args[0], fn.Args[1]
		leftCol, rightCol := left.GetCol(), right.GetCol()
		if leftCol == nil || rightCol == nil || !sameSemiContainmentType(left.Typ, right.Typ) {
			continue
		}
		equalities.union(
			[2]int32{leftCol.RelPos, leftCol.ColPos},
			[2]int32{rightCol.RelPos, rightCol.ColPos},
		)
	}
	return equalities
}

func (equalities *semiContainmentEqualities) find(key [2]int32) [2]int32 {
	parent, ok := equalities.parent[key]
	if !ok {
		equalities.parent[key] = key
		return key
	}
	if parent != key {
		equalities.parent[key] = equalities.find(parent)
	}
	return equalities.parent[key]
}

func (equalities *semiContainmentEqualities) union(left, right [2]int32) {
	left = equalities.find(left)
	right = equalities.find(right)
	if left != right {
		equalities.parent[left] = right
	}
}

func (equalities *semiContainmentEqualities) equivalent(left, right *planpb.Expr) bool {
	leftCol, rightCol := left.GetCol(), right.GetCol()
	if leftCol == nil || rightCol == nil || !sameSemiContainmentType(left.Typ, right.Typ) {
		return false
	}
	leftKey := [2]int32{leftCol.RelPos, leftCol.ColPos}
	rightKey := [2]int32{rightCol.RelPos, rightCol.ColPos}
	return leftKey == rightKey || equalities.find(leftKey) == equalities.find(rightKey)
}

// Type.Table and nullability are lineage properties, not part of the resolved
// comparison representation. Keep coercing equalities out of the closure, but
// allow the same VARCHAR key carried by two different base tables.
func sameSemiContainmentType(left, right planpb.Type) bool {
	return left.Id == right.Id && left.Width == right.Width &&
		left.Scale == right.Scale && left.Charset == right.Charset &&
		left.Enumvalues == right.Enumvalues && left.PadSpace == right.PadSpace
}
