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

package databranchutils

import (
	"sort"
	"strconv"
	"strings"
)

// BranchSnapshotKind is the value stored in mo_snapshots.kind for rows that
// are managed by the data-branch protect-snapshot mechanism. The `kind`
// column is the single source of truth for "is this snapshot managed by
// branch".
const BranchSnapshotKind = "branch"

// BranchSnapshotSnamePrefix is the sname prefix used by branch-owned snapshot
// rows. The suffix is the decimal child table id. Keep this in sync with the
// design doc §4.3.
const BranchSnapshotSnamePrefix = "__mo_branch_"

// BranchSnapshotName returns the sname used in mo_snapshots for the branch
// protect snapshot of a child table. Child table ids are cluster-unique, so
// the name is globally unique without any additional qualifier.
func BranchSnapshotName(childTableID uint64) string {
	return BranchSnapshotSnamePrefix + strconv.FormatUint(childTableID, 10)
}

// BranchReclaimDag is an in-memory picture of mo_branch_metadata suitable for
// running the reclaim DAG walk. `Children` is an adjacency list keyed on
// parent table id; `Info` maps every known table id to its metadata row.
//
// It is a distinct, slimmer structure from the LCA-oriented DataBranchDAG
// defined in branch_dag.go: the reclaim walk only cares about
// (parent, deleted) and would waste work computing depths or LCA pointers.
type BranchReclaimDag struct {
	Children map[uint64][]uint64
	Info     map[uint64]BranchReclaimNode
}

// BranchReclaimNode is the per-tid metadata needed by the reclaim walk.
type BranchReclaimNode struct {
	ParentTableID uint64
	CloneTS       int64
	Deleted       bool
}

// NewBranchReclaimDag builds the reclaim DAG from a flat list of metadata
// rows (shape shared with NewDAG).
func NewBranchReclaimDag(rows []DataBranchMetadata) BranchReclaimDag {
	dag := BranchReclaimDag{
		Children: make(map[uint64][]uint64, len(rows)),
		Info:     make(map[uint64]BranchReclaimNode, len(rows)),
	}
	for _, r := range rows {
		dag.Info[r.TableID] = BranchReclaimNode{
			ParentTableID: r.PTableID,
			CloneTS:       r.CloneTS,
			Deleted:       r.TableDeleted,
		}
		if r.PTableID != 0 {
			dag.Children[r.PTableID] = append(dag.Children[r.PTableID], r.TableID)
		}
	}
	return dag
}

// SubtreeAllDeleted returns true iff `root` and every descendant reachable
// through the DAG have `Deleted == true`. A root that is not in `Info` is
// treated as "deleted" (i.e. already reclaimable), which matches the
// dangling-metadata case in the design doc (§9.3.1 UT-U7).
//
// Implementation notes:
//   - The walk is cycle-safe: a `visited` set prevents infinite recursion if
//     `mo_branch_metadata` is corrupted into a cycle (e.g. A.parent=B,
//     B.parent=A). A revisited node is treated as "still deleted" so the
//     cycle does not starve an otherwise-reclaimable subtree.
//   - A per-invocation `memo` cache turns the amortised cost from O(N²) to
//     O(N) when the same subtree is evaluated for multiple candidates, which
//     is the common case during cascaded drops.
func (d BranchReclaimDag) SubtreeAllDeleted(root uint64) bool {
	memo := make(map[uint64]bool, len(d.Info))
	visited := make(map[uint64]struct{}, len(d.Info))
	return d.subtreeAllDeletedMemo(root, memo, visited)
}

func (d BranchReclaimDag) subtreeAllDeletedMemo(
	root uint64,
	memo map[uint64]bool,
	visited map[uint64]struct{},
) bool {
	if v, ok := memo[root]; ok {
		return v
	}
	if _, seen := visited[root]; seen {
		// Cycle: assume deleted so the cycle does not hold up the rest of
		// the subtree. The enclosing caller's visited bookkeeping prevents
		// an infinite loop regardless of what the true `Deleted` bit says.
		return true
	}
	visited[root] = struct{}{}
	meta, ok := d.Info[root]
	if !ok {
		memo[root] = true
		return true
	}
	if !meta.Deleted {
		memo[root] = false
		return false
	}
	for _, child := range d.Children[root] {
		if !d.subtreeAllDeletedMemo(child, memo, visited) {
			memo[root] = false
			return false
		}
	}
	memo[root] = true
	return true
}

// ComputeBranchReclaimDropList walks the DAG starting from `deadTIDs`,
// climbing to every ancestor and re-checking subtree-all-deleted. The return
// value is the (sorted, deduplicated) list of snames that must be removed
// from mo_snapshots to release protection (§5.3).
//
// Both the ancestor walk (this function) and the subtree check
// (SubtreeAllDeleted) are cycle-safe — a corrupt `mo_branch_metadata` row
// that produces a parent-cycle must never hang the drop path.
func ComputeBranchReclaimDropList(dag BranchReclaimDag, deadTIDs []uint64) []string {
	candidates := make(map[uint64]struct{}, len(deadTIDs)*2)
	for _, tid := range deadTIDs {
		cursor := tid
		for cursor != 0 {
			if _, seen := candidates[cursor]; seen {
				// Already walked from a previous dead tid or hit a cycle —
				// either way there is nothing new above this cursor.
				break
			}
			candidates[cursor] = struct{}{}
			meta, ok := dag.Info[cursor]
			if !ok {
				break
			}
			cursor = meta.ParentTableID
		}
	}

	// Memoise subtree results so `O(candidates)` × `O(subtree)` does not
	// become quadratic when many candidates share ancestors (cascaded drop
	// of a wide subtree).
	memo := make(map[uint64]bool, len(dag.Info))
	visited := make(map[uint64]struct{}, len(dag.Info))
	var drops []string
	for tid := range candidates {
		if _, ok := dag.Info[tid]; !ok {
			continue
		}
		if dag.subtreeAllDeletedMemo(tid, memo, visited) {
			drops = append(drops, BranchSnapshotName(tid))
		}
	}
	sort.Strings(drops)
	return drops
}

// BuildBranchSnapshotDeleteSQL returns the DELETE statement that reclaims
// the given snames from mo_snapshots, or the empty string if there is
// nothing to drop. The caller is responsible for executing it as sys.
//
// Branch snames are synthesised internally as `__mo_branch_<decimal>` so
// they cannot contain quote characters in practice. The only "foreign"
// value in this SQL is thus a known-safe synthesised identifier.
func BuildBranchSnapshotDeleteSQL(snames []string) string {
	if len(snames) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(80 + len(snames)*24)
	b.WriteString("delete from mo_catalog.mo_snapshots where kind = '")
	b.WriteString(BranchSnapshotKind)
	b.WriteString("' and sname in (")
	for i, s := range snames {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('\'')
		b.WriteString(s)
		b.WriteByte('\'')
	}
	b.WriteByte(')')
	return b.String()
}

// ReclaimBranchSnapshotsCore runs the shared reclaim algorithm. It is the
// single source of truth for the "flip table_deleted → compute drop list →
// delete mo_snapshots rows" pipeline. Both the frontend path (data branch
// delete) and the compile path (plain DROP TABLE) route through it via the
// wrapper in their respective packages. Test code can drive it directly by
// passing mock closures, which is what UT-U5/UT-U6/UT-U7 rely on.
func ReclaimBranchSnapshotsCore(
	deadTIDs []uint64,
	loadDAG func() (BranchReclaimDag, error),
	execDelete func(snames []string) error,
) error {
	if len(deadTIDs) == 0 {
		return nil
	}
	dag, err := loadDAG()
	if err != nil {
		return err
	}
	drops := ComputeBranchReclaimDropList(dag, deadTIDs)
	if len(drops) == 0 {
		return nil
	}
	return execDelete(drops)
}
