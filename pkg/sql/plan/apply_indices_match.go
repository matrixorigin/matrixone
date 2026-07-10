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

// This file holds the small, domain-neutral helpers shared by BOTH the classic
// fulltext MATCH path (apply_indices_fulltext.go) and the bm25 MATCH path
// (apply_indices_bm25.go). They operate purely on the shared fulltext_match()
// function ABI (Args[0]=pattern, Args[1]=mode, Args[2:]=index columns) and the
// generic node-context table — no classic-fulltext- or bm25-specific behavior —
// so sharing them avoids duplication without coupling the two index semantics.
package plan

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// equalsMatchFunc reports whether two MATCH functions are equivalent: same
// pattern, same mode, and the same set of index column names.
func (builder *QueryBuilder) equalsMatchFunc(fn1 *plan.Function, fn2 *plan.Function) bool {

	nargs1 := len(fn1.Args)
	nargs2 := len(fn2.Args)

	if nargs1 != nargs2 {
		return false
	}

	// check search pattern and mode
	var pattern1, pattern2 string
	var mode1, mode2 int64

	pattern1 = fn1.Args[0].GetLit().GetSval()
	mode1 = fn1.Args[1].GetLit().GetI64Val()

	pattern2 = fn2.Args[0].GetLit().GetSval()
	mode2 = fn2.Args[1].GetLit().GetI64Val()

	if pattern1 != pattern2 || mode1 != mode2 {
		return false
	}

	// check index parts
	for i := 2; i < nargs1; i++ {
		if !strings.EqualFold(fn1.Args[i].GetCol().GetName(), fn2.Args[i].GetCol().GetName()) {
			return false
		}
	}

	return true
}

// findEqualMatchFunc returns map[projid]filter_id -- position of the projids and
// filterids but NOT position of ProjectList and FilterList
func (builder *QueryBuilder) findEqualMatchFunc(projNode *plan.Node, scanNode *plan.Node, projids, filterids []int32) map[int32]int32 {

	eqmap := make(map[int32]int32)

	for i, projid := range projids {
		prexpr := projNode.ProjectList[projid]
		for j, fid := range filterids {
			fexpr := scanNode.FilterList[fid]
			eq := builder.equalsMatchFunc(prexpr.GetF(), fexpr.GetF())
			if eq {
				eqmap[int32(i)] = int32(j)
			}
		}
	}

	return eqmap
}

// matchRewriteContextNodeID picks a valid node-context id for a MATCH rewrite,
// preferring preferredNodeID and falling back to the scan node's id.
func (builder *QueryBuilder) matchRewriteContextNodeID(preferredNodeID int32, scanNode *plan.Node) int32 {
	if preferredNodeID >= 0 && int(preferredNodeID) < len(builder.ctxByNode) && builder.ctxByNode[preferredNodeID] != nil {
		return preferredNodeID
	}
	if scanNode != nil && scanNode.NodeId >= 0 && int(scanNode.NodeId) < len(builder.ctxByNode) && builder.ctxByNode[scanNode.NodeId] != nil {
		return scanNode.NodeId
	}
	return preferredNodeID
}
