// Copyright 2026 MatrixOne
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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ClusterHasLockingCurrentRead reports whether every live service negotiates
// at least MORPCVersion135, the locking-read current-read gate (design D-7;
// the ClusterHasIndexProvenance pattern mirrored verbatim; the allocated
// number sits strictly above the verified upstream high-water 134 per D-7's
// revalidation rule — never the colliding 130–134). MOProtocolVersion
// tracks the OLDEST live service and is read live at each decision rather
// than memoized; an unavailable runtime answers false, which keeps below-gate
// clusters on today's semantics uniformly.
func ClusterHasLockingCurrentRead(proc *process.Process) bool {
	if proc == nil {
		return false
	}
	rt := runtime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return false
	}
	value, ok := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	if !ok {
		return false
	}
	version, ok := value.(int64)
	return ok && version >= defines.MORPCVersion135
}

// PlanLockingReadQuery returns the SELECT query of a locking-current-read
// presence plan (any LockTarget carries bind-time locking_read presence,
// design D-8 layer 1). Nil for plans without presence. The summary is
// derived from the plan object itself and never changes after bind.
func PlanLockingReadQuery(pn any) (*plan.Query, error) {
	switch p := pn.(type) {
	case *plan.Plan:
		if p == nil || p.GetQuery() == nil {
			return nil, nil
		}
		return p.GetQuery(), nil
	case *plan.Query:
		return p, nil
	}
	return nil, nil
}

// PlanHasLockingCurrentRead reports bind-time presence: at least one
// LockTarget of the plan marks a locking current-read occurrence.
func PlanHasLockingCurrentRead(qry *plan.Query) bool {
	if qry == nil {
		return false
	}
	for _, node := range qry.GetNodes() {
		for _, t := range node.GetLockTargets() {
			if t.GetLockingRead() {
				return true
			}
		}
	}
	return false
}

// LockingReadClosureWalk is the rev-14 R14-F2 closure check over a presence
// plan: construction never emits the zero scan role for a completed plan, so
// a zero-role TABLE_SCAN surviving optimization can only be a mark lost
// through an unknown rewrite; search-generated scans plus the covered-plan
// strip stay outside the stage-1 admission set, so their presence in a
// locking statement declines the whole statement to conserving base
// semantics; and an OUTER-locked occurrence on a relation kind the stage-1
// admission set does not cover (a cluster table, an external relation, an
// index surface without a marked owning parent) cannot be left behind while
// its ordinary locked peers current-read — retaining that mix would violate
// D-10's whole-statement conservation, so it declines the whole statement
// too. Intentional snapshot occurrences (explicit-nested roles, covered-strip
// scans inside subqueries) are distinguished by representation and never
// trip these members. Both outcomes are computed from the plan alone — no
// structural inference over join shapes or table identity, so same-table
// scalar subqueries lowered to LEFT joins stay decidable (design D-10.3).
//
// The returned absent value is a sentinel for "no closure violation"; plan
// nodes already carry the per-occurrence roles that survive every approved
// rewrite site's propagation (apply_indices fabrication, DeepCopyNode, the
// fulltext2 covered-plan strip value).
type LockingReadClosure struct {
	// LostMark is true when a zero-role scan survived in a presence plan.
	LostMark bool
	// OutsideAdmission is true when a search-served surface (INDEX_SEARCH_SCAN
	// or the covered-plan strip role) participates in a presence plan.
	OutsideAdmission bool
	// UnsupportedOuterKind is true when an outer-locking occurrence sits on
	// a relation kind outside the admission set (compile's kind switch
	// mirrored here: ordinary tables, and index surfaces only through a
	// marked owning parent of this plan).
	UnsupportedOuterKind bool
}

func WalkLockingReadClosure(qry *plan.Query) LockingReadClosure {
	var out LockingReadClosure
	// The closure is a presence-plan check (design D-10.3): without bind-time
	// presence there is no marking to lose and no admission set to mix with —
	// an unmarked plan's scan roles are today's legacy values and mean
	// nothing for the current-read contract. Gate here, at the walk itself,
	// so every consumer inherits the same exemption-free semantics whether or
	// not it re-checks presence.
	if !PlanHasLockingCurrentRead(qry) {
		return out
	}
	for _, node := range qry.GetNodes() {
		if node == nil {
			continue
		}
		if node.NodeType == plan.Node_INDEX_SEARCH_SCAN {
			out.OutsideAdmission = true
			continue
		}
		if node.NodeType != plan.Node_TABLE_SCAN {
			continue
		}
		switch node.GetScanReadRole() {
		case plan.ScanReadRole_ScanNestedBlock:
			out.LostMark = true
		case plan.ScanReadRole_ScanCoveredPlanStrip:
			out.OutsideAdmission = true
		case plan.ScanReadRole_ScanOuterLocking,
			plan.ScanReadRole_ScanOuterLockingWithExplicitSnapshot:
			if !outerLockedScanKindAdmissible(qry, node) {
				out.UnsupportedOuterKind = true
			}
		}
	}
	return out
}

// outerLockedScanKindAdmissible mirrors compile's admission kind switch at
// the plan layer: an outer-locking occurrence is admissible on an ordinary
// user table, or on a covered secondary-index surface whose fabricated scan
// names a marked owning base table of this plan; anything else (cluster
// tables, external relations, orphan index surfaces) must decline the whole
// statement instead of reading current on some locked peers and snapshot on
// the others.
func outerLockedScanKindAdmissible(qry *plan.Query, node *plan.Node) bool {
	tbl := node.GetTableDef()
	if tbl == nil {
		return false
	}
	switch tbl.GetTableType() {
	case catalog.SystemOrdinaryRel:
		return true
	case catalog.SystemIndexRel:
		info := node.GetIndexScanInfo()
		parentID := info.GetParentTableId()
		if parentID <= 0 {
			return false
		}
		for _, n := range qry.GetNodes() {
			for _, t := range n.GetLockTargets() {
				if t.GetLockingRead() && t.GetTableId() == uint64(parentID) {
					return true
				}
			}
		}
	}
	return false
}
