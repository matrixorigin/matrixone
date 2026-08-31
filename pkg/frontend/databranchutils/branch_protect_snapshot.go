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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// BranchSnapshotKind is the value stored in mo_snapshots.kind for rows that
// are managed by the data-branch protect-snapshot mechanism. The `kind`
// column is the single source of truth for "is this snapshot managed by
// branch".
const BranchSnapshotKind = "branch"

// AlterLineageLevel marks an internal mo_branch_metadata edge created by
// ALTER's copy-and-swap. It preserves physical history but is not a logical
// branch fork.
const AlterLineageLevel = "alter"

func IsAlterLineageLevel(level string) bool {
	return level == AlterLineageLevel || strings.HasPrefix(level, AlterLineageLevel+":")
}

func NextAlterLineageLevel(parentLevel string) string {
	if IsAlterLineageLevel(parentLevel) {
		return parentLevel
	}
	if parentLevel == "" {
		return AlterLineageLevel
	}
	return AlterLineageLevel + ":" + parentLevel
}

// isLogicalBranchOwnerLevel reports whether a live metadata row represents a
// logical branch. A plain "alter" row exists only for a historical source,
// while "alter:<level>" carries the logical ownership inherited across
// ALTER's copy-and-swap.
func isLogicalBranchOwnerLevel(level string) bool {
	return !IsAlterLineageLevel(level) || strings.HasPrefix(level, AlterLineageLevel+":")
}

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

// ParseBranchSnapshotName extracts the child table id from a branch-managed
// snapshot name. It rejects user snapshots and malformed internal names.
func ParseBranchSnapshotName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, BranchSnapshotSnamePrefix) {
		return 0, false
	}
	id, err := strconv.ParseUint(strings.TrimPrefix(name, BranchSnapshotSnamePrefix), 10, 64)
	return id, err == nil
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
	Creator       uint64
	Level         string
	Deleted       bool
}

// HistoricalLineageEdge identifies the physical table edge protected by an
// ALTER-owned branch snapshot. The catalog identity fields come from the
// matching kind='branch' snapshot row.
type HistoricalLineageEdge struct {
	ChildTableID  uint64
	ParentTableID uint64
	CloneTS       int64
	AccountName   string
	DatabaseName  string
	TableName     string
}

// HistoricalSource is a normalized user snapshot or active PITR retention
// window. OldestTS is a fixed snapshot timestamp or the rolling PITR lower
// bound calculated at the owning statement's timestamp.
type HistoricalSource struct {
	Level        string
	AccountName  string
	DatabaseName string
	TableName    string
	ObjectID     uint64
	OldestTS     int64
}

// AlterLineageCompactionPlan lists the paired catalog rows that can be
// removed without disconnecting a live logical branch or active historical
// source.
type AlterLineageCompactionPlan struct {
	TableIDs      []uint64
	SnapshotNames []string
}

// BranchReclaimPlan contains the branch-owned rows that can be reclaimed
// after an account drop. Metadata rows are retained while any descendant is
// live because their parent links are needed to protect that descendant's
// ancestor snapshots.
type BranchReclaimPlan struct {
	MetadataTableIDs []uint64
	SnapshotNames    []string
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
			Creator:       r.Creator,
			Level:         r.Level,
			Deleted:       r.TableDeleted,
		}
		if r.PTableID != 0 {
			dag.Children[r.PTableID] = append(dag.Children[r.PTableID], r.TableID)
		}
	}
	return dag
}

// PitrRetentionLowerBound returns the oldest timestamp retained by a PITR at
// the supplied statement time. It never reads the wall clock itself, keeping
// frontend and compile-layer decisions on the same boundary.
func PitrRetentionLowerBound(now time.Time, length int, unit string) (int64, error) {
	if length <= 0 || length > 100 {
		return 0, moerr.NewInvalidInputNoCtxf("invalid PITR length %d", length)
	}
	now = now.UTC()
	var lower time.Time
	switch unit {
	case "h":
		lower = now.Add(-time.Duration(length) * time.Hour)
	case "d":
		lower = now.AddDate(0, 0, -length)
	case "mo":
		lower = addDateClamped(now, 0, -length)
	case "y":
		lower = addDateClamped(now, -length, 0)
	default:
		return 0, moerr.NewInvalidInputNoCtxf("unknown PITR unit %q", unit)
	}
	return lower.UnixNano(), nil
}

// PitrRetentionRangeDoesNotExpand reports whether replacing one PITR range
// with another can never ask GC to retain older history. A point-in-time
// comparison is insufficient for mixed fixed and calendar units: for example,
// one month can be shorter than 30 days in February and longer in March.
func PitrRetentionRangeDoesNotExpand(
	currentLength int,
	currentUnit string,
	nextLength int,
	nextUnit string,
) (bool, error) {
	current, err := newPitrDuration(currentLength, currentUnit)
	if err != nil {
		return false, err
	}
	next, err := newPitrDuration(nextLength, nextUnit)
	if err != nil {
		return false, err
	}
	return pitrRetentionRangeDoesNotExpand(current, next), nil
}

// MergePitrRetentionRanges returns one persistable PITR range that covers both
// inputs for every statement time. This is used by sys_mo_catalog_pitr, whose
// range must remain an upper bound for all user PITRs as month lengths change.
//
// If neither input permanently covers the other (for example, one month and
// thirty days), the result is their fixed-day upper envelope. With PITR lengths
// limited to 100, an incomparable calendar range is at most three months, so
// the envelope is always representable as at most 100 days.
func MergePitrRetentionRanges(
	currentLength int,
	currentUnit string,
	nextLength int,
	nextUnit string,
) (int, string, error) {
	current, err := newPitrDuration(currentLength, currentUnit)
	if err != nil {
		return 0, "", err
	}
	next, err := newPitrDuration(nextLength, nextUnit)
	if err != nil {
		return 0, "", err
	}

	if pitrRetentionRangeDoesNotExpand(current, next) {
		return currentLength, currentUnit, nil
	}
	if pitrRetentionRangeDoesNotExpand(next, current) {
		return nextLength, nextUnit, nil
	}

	maxHours := current.maxHours
	if next.maxHours > maxHours {
		maxHours = next.maxHours
	}
	days := (maxHours + 24 - 1) / 24
	if days > 100 {
		return 0, "", moerr.NewInternalErrorNoCtxf(
			"cannot represent merged PITR retention range %dh", maxHours,
		)
	}
	return days, "d", nil
}

func pitrRetentionRangeDoesNotExpand(current, next pitrDuration) bool {
	// Hours and days are fixed durations in UTC. Months and years share the
	// same clamped calendar semantics, so twelve months exactly equal one year.
	if current.kind == next.kind {
		return next.normalized <= current.normalized
	}

	// Across fixed and calendar units, require the longest possible next
	// duration to fit inside the shortest possible current duration. The
	// conservative bound makes the decision independent of month ends, leap
	// years, and an unbounded delay before every GC consumer observes ALTER.
	return next.maxHours <= current.minHours
}

type pitrDuration struct {
	kind       byte
	normalized int
	minHours   int
	maxHours   int
}

func newPitrDuration(length int, unit string) (pitrDuration, error) {
	if length <= 0 || length > 100 {
		return pitrDuration{}, moerr.NewInvalidInputNoCtxf("invalid PITR length %d", length)
	}

	switch unit {
	case "h":
		return pitrDuration{kind: 'f', normalized: length, minHours: length, maxHours: length}, nil
	case "d":
		hours := length * 24
		return pitrDuration{kind: 'f', normalized: hours, minHours: hours, maxHours: hours}, nil
	case "mo":
		return pitrDuration{
			kind:       'c',
			normalized: length,
			minHours:   length * 28 * 24,
			maxHours:   length * 31 * 24,
		}, nil
	case "y":
		return pitrDuration{
			kind:       'c',
			normalized: length * 12,
			minHours:   length * 365 * 24,
			maxHours:   length * 366 * 24,
		}, nil
	default:
		return pitrDuration{}, moerr.NewInvalidInputNoCtxf("unknown PITR unit %q", unit)
	}
}

// addDateClamped applies a calendar offset while clamping the day to the last
// valid day in the target month. time.Time.AddDate normalizes February 31 into
// March, which can move a rolling retention boundary backwards at month end.
func addDateClamped(t time.Time, years int, months int) time.Time {
	targetMonth := time.Date(
		t.Year()+years,
		t.Month()+time.Month(months),
		1,
		t.Hour(),
		t.Minute(),
		t.Second(),
		t.Nanosecond(),
		t.Location(),
	)
	lastDay := targetMonth.AddDate(0, 1, -1).Day()
	day := t.Day()
	if day > lastDay {
		day = lastDay
	}
	return targetMonth.AddDate(0, 0, day-1)
}

func historicalSourceOwnsComponent(
	source HistoricalSource,
	nodeIDs map[uint64]struct{},
	edges []HistoricalLineageEdge,
) bool {
	switch strings.ToLower(source.Level) {
	case "cluster":
		return true
	case "account":
		for _, edge := range edges {
			if source.AccountName == edge.AccountName {
				return true
			}
		}
	case "database":
		for _, edge := range edges {
			if source.AccountName == edge.AccountName &&
				source.DatabaseName == edge.DatabaseName {
				return true
			}
		}
	case "table":
		if source.ObjectID != 0 {
			if _, ok := nodeIDs[source.ObjectID]; ok {
				return true
			}
		}
		for _, edge := range edges {
			if source.AccountName == edge.AccountName &&
				source.DatabaseName == edge.DatabaseName &&
				source.TableName == edge.TableName {
				return true
			}
		}
	}
	return false
}

// ComputeAlterLineageCompactionPlan finds ALTER-only generations that no
// longer have an owner. A live logical branch conservatively owns its entire
// connected component so sibling/ancestor LCA paths are never disconnected.
func ComputeAlterLineageCompactionPlan(
	dag BranchReclaimDag,
	edges map[uint64]HistoricalLineageEdge,
	sources []HistoricalSource,
) AlterLineageCompactionPlan {
	visited := make(map[uint64]struct{}, len(dag.Info))
	plan := AlterLineageCompactionPlan{}

	for start := range dag.Info {
		if _, ok := visited[start]; ok {
			continue
		}
		component, logicalOwner := dag.connectedComponent(start, visited)

		if logicalOwner {
			continue
		}
		componentNodeIDs := make(map[uint64]struct{}, len(component))
		componentEdges := make([]HistoricalLineageEdge, 0, len(component))
		for _, tableID := range component {
			componentNodeIDs[tableID] = struct{}{}
			if edge, ok := edges[tableID]; ok {
				componentEdges = append(componentEdges, edge)
			}
		}
		componentSources := make([]HistoricalSource, 0, len(sources))
		for _, source := range sources {
			if historicalSourceOwnsComponent(source, componentNodeIDs, componentEdges) {
				componentSources = append(componentSources, source)
			}
		}
		for _, tableID := range component {
			meta, ok := dag.Info[tableID]
			if !ok || !IsAlterLineageLevel(meta.Level) {
				continue
			}
			edge, ok := edges[tableID]
			if ok && (edge.ChildTableID != tableID ||
				edge.ParentTableID != meta.ParentTableID || edge.CloneTS != meta.CloneTS) {
				continue
			}
			// A live row without its identity-matched snapshot is retained
			// conservatively. A deleted row may legitimately have no snapshot:
			// plain DROP reclaims branch snapshots before the last historical
			// owner disappears. Once that owner is gone, deleting the orphaned
			// metadata row (and the deterministic snapshot name, idempotently)
			// closes the lifecycle instead of leaking one row per ALTER.
			if !ok && !meta.Deleted {
				continue
			}
			covered := false
			for _, source := range componentSources {
				if source.OldestTS <= meta.CloneTS {
					covered = true
					break
				}
			}
			if covered {
				continue
			}
			plan.TableIDs = append(plan.TableIDs, tableID)
			plan.SnapshotNames = append(plan.SnapshotNames, BranchSnapshotName(tableID))
		}
	}

	sort.Slice(plan.TableIDs, func(i, j int) bool { return plan.TableIDs[i] < plan.TableIDs[j] })
	sort.Strings(plan.SnapshotNames)
	return plan
}

func (d BranchReclaimDag) connectedComponent(
	start uint64,
	visited map[uint64]struct{},
) (component []uint64, hasLiveLogicalBranch bool) {
	stack := []uint64{start}
	for len(stack) > 0 {
		last := len(stack) - 1
		tableID := stack[last]
		stack = stack[:last]
		if tableID == 0 {
			continue
		}
		if _, ok := visited[tableID]; ok {
			continue
		}
		visited[tableID] = struct{}{}
		component = append(component, tableID)

		if meta, ok := d.Info[tableID]; ok {
			if !meta.Deleted && isLogicalBranchOwnerLevel(meta.Level) {
				hasLiveLogicalBranch = true
			}
			if meta.ParentTableID != 0 {
				stack = append(stack, meta.ParentTableID)
			}
		}
		stack = append(stack, d.Children[tableID]...)
	}
	return component, hasLiveLogicalBranch
}

// ComponentHasLiveLogicalBranch reports whether the lineage component that
// contains start has a live logical branch owner. Plain "alter" rows only pin
// historical physical generations and do not represent logical branches.
func (d BranchReclaimDag) ComponentHasLiveLogicalBranch(start uint64) bool {
	_, hasLiveLogicalBranch := d.connectedComponent(
		start,
		make(map[uint64]struct{}, len(d.Info)),
	)
	return hasLiveLogicalBranch
}

// BuildAlterLineageSnapshotDeleteSQL deletes only branch-managed snapshots.
func BuildAlterLineageSnapshotDeleteSQL(snames []string) string {
	return BuildBranchSnapshotDeleteSQL(snames)
}

// BuildAlterLineageMetadataDeleteSQL re-checks ALTER ownership at execution
// time so a stale compaction plan cannot delete a logical branch row.
func BuildAlterLineageMetadataDeleteSQL(tableIDs []uint64) string {
	if len(tableIDs) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(128 + len(tableIDs)*20)
	b.WriteString("delete from mo_catalog.mo_branch_metadata where table_id in (")
	for i, tableID := range tableIDs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatUint(tableID, 10))
	}
	b.WriteString(") and (level = 'alter' or level like 'alter:%')")
	return b.String()
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

// SubtreeHasLiveNode reports whether root or any descendant still represents
// a live physical generation. Unlike SubtreeAllDeleted, it intentionally
// walks children even when root has no metadata row; root tables commonly
// appear only as p_table_id. The walk is cycle-safe for corrupted metadata.
func (d BranchReclaimDag) SubtreeHasLiveNode(root uint64) bool {
	visited := make(map[uint64]struct{}, len(d.Info))
	var walk func(uint64) bool
	walk = func(tableID uint64) bool {
		if _, ok := visited[tableID]; ok {
			return false
		}
		visited[tableID] = struct{}{}
		if meta, ok := d.Info[tableID]; ok && !meta.Deleted {
			return true
		}
		for _, childID := range d.Children[tableID] {
			if walk(childID) {
				return true
			}
		}
		return false
	}
	return walk(root)
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
		//
		// mo_branch_metadata is supposed to be acyclic (table_id is PK,
		// the row is only ever written once by updateBranchMetaTable).
		// Hitting this branch therefore indicates catalog corruption —
		// surface a WARN so the operator can investigate.
		logutil.Warn(
			"DataBranch-ProtectSnapshot-CycleDetected",
			zap.Uint64("table-id", root),
			zap.String("reason", "mo_branch_metadata ancestor chain forms a cycle; treating node as deleted to avoid infinite recursion"),
		)
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

func branchReclaimCandidates(dag BranchReclaimDag, roots []uint64) map[uint64]struct{} {
	candidates := make(map[uint64]struct{}, len(roots)*2)
	for _, tid := range roots {
		cursor := tid
		// `walkVisited` is scoped to a single root walk so we can
		// distinguish "hit a sibling's already-seen ancestor" (benign)
		// from "hit a node in our own walk's history" (cycle).
		walkVisited := make(map[uint64]struct{})
		for cursor != 0 {
			if _, inThisWalk := walkVisited[cursor]; inThisWalk {
				logutil.Warn(
					"DataBranch-ProtectSnapshot-CycleDetected",
					zap.Uint64("table-id", cursor),
					zap.Uint64("walk-origin-tid", tid),
					zap.String("reason", "mo_branch_metadata ancestor chain forms a cycle; breaking walk"),
				)
				break
			}
			walkVisited[cursor] = struct{}{}
			if _, seen := candidates[cursor]; seen {
				// Already covered by a previous root's walk (normal
				// fan-out): nothing new above this cursor.
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
	return candidates
}

func branchReclaimableTableIDs(dag BranchReclaimDag, roots []uint64) []uint64 {
	candidates := branchReclaimCandidates(dag, roots)

	// Memoise subtree results so `O(candidates)` × `O(subtree)` does not
	// become quadratic when many candidates share ancestors (cascaded drop
	// of a wide subtree).
	memo := make(map[uint64]bool, len(dag.Info))
	visited := make(map[uint64]struct{}, len(dag.Info))
	var tableIDs []uint64
	for tid := range candidates {
		meta, ok := dag.Info[tid]
		if !ok {
			continue
		}
		// ALTER generations are reclaimed by ComputeAlterLineageCompactionPlan,
		// which also accounts for user snapshots and PITRs. Deleting their
		// identity snapshots here would discard the account/database scope
		// evidence before that historical-owner check runs.
		if IsAlterLineageLevel(meta.Level) {
			continue
		}
		if dag.subtreeAllDeletedMemo(tid, memo, visited) {
			tableIDs = append(tableIDs, tid)
		}
	}
	sort.Slice(tableIDs, func(i, j int) bool { return tableIDs[i] < tableIDs[j] })
	return tableIDs
}

// ComputeAccountBranchReclaimPlan returns the safe branch metadata and
// protect-snapshot rows to remove while dropping accountID. It starts at all
// metadata rows created in that account and walks through their ancestors, so
// a later child-account drop can reclaim a retained ancestor row. A row is
// returned only when its complete descendant subtree is deleted; this keeps
// the DAG connected for live cross-account descendants.
func ComputeAccountBranchReclaimPlan(
	dag BranchReclaimDag,
	accountID uint64,
) BranchReclaimPlan {
	var roots []uint64
	for tableID, meta := range dag.Info {
		if meta.Creator == accountID {
			roots = append(roots, tableID)
		}
	}
	tableIDs := branchReclaimableTableIDs(dag, roots)
	plan := BranchReclaimPlan{MetadataTableIDs: tableIDs}
	for _, tableID := range tableIDs {
		plan.SnapshotNames = append(plan.SnapshotNames, BranchSnapshotName(tableID))
	}
	sort.Strings(plan.SnapshotNames)
	return plan
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
	tableIDs := branchReclaimableTableIDs(dag, deadTIDs)
	drops := make([]string, 0, len(tableIDs))
	for _, tableID := range tableIDs {
		drops = append(drops, BranchSnapshotName(tableID))
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

// BuildBranchMetadataDeleteSQL returns a guarded DELETE for reclaimable
// normal branch metadata. The table_deleted and level predicates are repeated
// at execution time so a stale caller cannot remove a live or ALTER lineage
// row even if its table-id list was computed earlier.
func BuildBranchMetadataDeleteSQL(tableIDs []uint64) string {
	if len(tableIDs) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(160 + len(tableIDs)*20)
	b.WriteString("delete from mo_catalog.mo_branch_metadata where table_id in (")
	for i, tableID := range tableIDs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatUint(tableID, 10))
	}
	b.WriteString(") and table_deleted = true and (level != 'alter' and level not like 'alter:%')")
	return b.String()
}

// MarkAndReclaimBranchSnapshotsCore runs the complete shared reclaim pipeline.
// loadDAG must acquire the mo_branch_metadata coordination lock. Keeping the
// lock before markDeleted gives every deletion path the same lock order and
// prevents concurrent sibling drops from each updating one row before trying
// to lock the complete relation.
func MarkAndReclaimBranchSnapshotsCore(
	deadTIDs []uint64,
	loadDAG func() (BranchReclaimDag, error),
	markDeleted func() error,
	execDelete func(snames []string) error,
) error {
	if len(deadTIDs) == 0 {
		return nil
	}
	dag, err := loadDAG()
	if err != nil {
		return err
	}
	if err = markDeleted(); err != nil {
		return err
	}
	for _, tid := range deadTIDs {
		if node, ok := dag.Info[tid]; ok {
			node.Deleted = true
			dag.Info[tid] = node
		}
	}
	return reclaimBranchSnapshotsFromDAG(deadTIDs, dag, execDelete)
}

// ReclaimBranchSnapshotsCore computes and executes reclaim after the caller
// has already persisted table_deleted. It remains available for callers and
// tests that provide an already-updated DAG.
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
	return reclaimBranchSnapshotsFromDAG(deadTIDs, dag, execDelete)
}

func reclaimBranchSnapshotsFromDAG(
	deadTIDs []uint64,
	dag BranchReclaimDag,
	execDelete func(snames []string) error,
) error {
	drops := ComputeBranchReclaimDropList(dag, deadTIDs)
	if len(drops) == 0 {
		return nil
	}
	return execDelete(drops)
}
