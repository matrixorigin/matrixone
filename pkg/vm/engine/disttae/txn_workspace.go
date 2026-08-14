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

package disttae

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/tidwall/btree"
)

type workspaceMutationID uint64
type workspaceObjectDeleteID uint64

func workspaceMutationIDs(
	set map[workspaceMutationID]struct{},
) []workspaceMutationID {
	ids := make([]workspaceMutationID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// workspaceCommitOrder is an immutable logical position in the TN precommit
// stream. A physical rewrite inherits its source position. When one logical
// mutation is split into several replacements, child positions keep every
// replacement adjacent to the source's former neighbors even after nested
// rewrites. This decouples protocol order from mutation allocation time.
type workspaceCommitOrder []uint64

func childWorkspaceCommitOrder(
	parent workspaceCommitOrder,
	ordinal uint64,
) workspaceCommitOrder {
	child := make(workspaceCommitOrder, len(parent)+1)
	copy(child, parent)
	child[len(parent)] = ordinal
	return child
}

func compareWorkspaceCommitOrder(a, b workspaceCommitOrder) int {
	return slices.Compare(a, b)
}

// workspaceUsage is the logical usage of active mutations. It deliberately
// lives beside logical mutation publication rather than in Transaction or
// PayloadStore: every COW, spill, compaction and rollback changes the active
// mutation set here atomically.
type workspaceUsage struct {
	totalBytes               uint64
	inMemoryInsertBytes      uint64
	spillEligibleInsertBytes uint64
	inMemoryInsertRows       int
	inMemoryDeleteRows       int
}

func usageOfWorkspaceEntry(entry Entry, bat *batch.Batch) workspaceUsage {
	if bat == nil {
		return workspaceUsage{}
	}
	usage := workspaceUsage{totalBytes: uint64(bat.Size())}
	if entry.fileName != "" {
		return usage
	}
	if entry.typ == INSERT && !entry.isCatalog() {
		// Keep the spill-selection accounting identical to the historical
		// scope scan. MO_TABLES_LOGICAL_ID_INDEX is excluded from ordinary
		// transaction-size accounting by IsSystemTable, but it is not one of
		// the three catalog entries that dumpInsertBatchLocked skips.
		usage.spillEligibleInsertBytes = usage.totalBytes
	}
	if catalog.IsSystemTable(entry.tableId) {
		return usage
	}
	switch entry.typ {
	case INSERT:
		usage.inMemoryInsertBytes = usage.totalBytes
		usage.inMemoryInsertRows = bat.RowCount()
	case DELETE:
		usage.inMemoryDeleteRows = bat.RowCount()
	}
	return usage
}

func (u *workspaceUsage) add(other workspaceUsage) {
	u.totalBytes += other.totalBytes
	u.inMemoryInsertBytes += other.inMemoryInsertBytes
	u.spillEligibleInsertBytes += other.spillEligibleInsertBytes
	u.inMemoryInsertRows += other.inMemoryInsertRows
	u.inMemoryDeleteRows += other.inMemoryDeleteRows
}

func (u *workspaceUsage) sub(other workspaceUsage) {
	if u.totalBytes < other.totalBytes ||
		u.inMemoryInsertBytes < other.inMemoryInsertBytes ||
		u.spillEligibleInsertBytes < other.spillEligibleInsertBytes ||
		u.inMemoryInsertRows < other.inMemoryInsertRows ||
		u.inMemoryDeleteRows < other.inMemoryDeleteRows {
		panic("workspace usage underflow")
	}
	u.totalBytes -= other.totalBytes
	u.inMemoryInsertBytes -= other.inMemoryInsertBytes
	u.spillEligibleInsertBytes -= other.spillEligibleInsertBytes
	u.inMemoryInsertRows -= other.inMemoryInsertRows
	u.inMemoryDeleteRows -= other.inMemoryDeleteRows
}

// workspaceMutation is the logical visibility record for one published write.
// Its stable ID and commit order survive payload merge, spill and rewrite.
// The global mutation registry is the authority for active and revision
// visibility; tableOverlay contains only derived, table-local indexes.
type workspaceMutation struct {
	id                        workspaceMutationID
	statementID               uint64
	attemptID                 uint64
	createdRevision           uint64
	retiredRevision           uint64
	compactionInputGeneration uint64
	commitOrder               workspaceCommitOrder
	active                    bool
	entry                     Entry
	payloadID                 workspacePayloadID
	objectIDs                 []types.Objectid
	objectNames               []string
	blockMeta                 bool
	// pkKeys and rowIDs are immutable row-level access facts derived from the
	// current payload generation. indexedRows records which rows are present in
	// the current TableOverlay indexes after applying payload selections.
	pkKeys             []string
	rowIDs             []objectio.Rowid
	indexedRows        []bool
	memoryInsert       bool
	pkIndexComplete    bool
	rowIDIndexComplete bool
	// accessIndexRevision is the first revision described by pkKeys, rowIDs,
	// indexedRows and the completeness flags above. Physical generation
	// replacement advances it while retaining the preceding access facts in
	// the statement-history indexes.
	accessIndexRevision uint64
}

type workspaceAccessIndexInterval struct {
	mutationID workspaceMutationID
	start      uint64
	end        uint64
}

func (i workspaceAccessIndexInterval) contains(revision uint64) bool {
	return i.start <= revision && revision < i.end
}

// workspaceMutationOwners stores the overwhelmingly common single-owner
// point-index entry inline. A secondary map is allocated only when one key is
// genuinely owned by multiple logical mutations. This keeps TableOverlay's
// exact duplicate/rollback semantics without paying for one Go map per unique
// primary key or RowID in large transactions.
type workspaceMutationOwners struct {
	singleID    workspaceMutationID
	singleCount uint32
	multiple    map[workspaceMutationID]uint32
}

func (o workspaceMutationOwners) len() int {
	if o.multiple != nil {
		return len(o.multiple)
	}
	if o.singleCount != 0 {
		return 1
	}
	return 0
}

func (o *workspaceMutationOwners) add(id workspaceMutationID) {
	if o.multiple != nil {
		o.multiple[id]++
		return
	}
	if o.singleCount == 0 || o.singleID == id {
		o.singleID = id
		o.singleCount++
		return
	}
	o.multiple = map[workspaceMutationID]uint32{
		o.singleID: o.singleCount,
		id:         1,
	}
	o.singleID = 0
	o.singleCount = 0
}

func (o *workspaceMutationOwners) remove(id workspaceMutationID) bool {
	if o.multiple == nil {
		if o.singleID != id || o.singleCount == 0 {
			return false
		}
		o.singleCount--
		if o.singleCount == 0 {
			o.singleID = 0
		}
		return true
	}
	count := o.multiple[id]
	if count == 0 {
		return false
	}
	if count > 1 {
		o.multiple[id] = count - 1
		return true
	}
	delete(o.multiple, id)
	if len(o.multiple) == 1 {
		for remainingID, remainingCount := range o.multiple {
			o.singleID = remainingID
			o.singleCount = remainingCount
		}
		o.multiple = nil
	}
	return true
}

func (o workspaceMutationOwners) forEach(fn func(workspaceMutationID, uint32)) {
	if o.multiple != nil {
		for id, count := range o.multiple {
			fn(id, count)
		}
		return
	}
	if o.singleCount != 0 {
		fn(o.singleID, o.singleCount)
	}
}

// orderedMutationSet is the sole mutable owner of one active mutation index.
// Membership is keyed by the stable mutation ID, while iteration follows the
// immutable TN commit order. Publishing and retiring a mutation update both
// representations together under txnWorkspace.mu; readers therefore never
// rebuild or sort the active write set.
type orderedMutationSet struct {
	byID    map[workspaceMutationID]*workspaceMutation
	byOrder *btree.BTreeG[*workspaceMutation]
}

// workspaceCompactionPlan pins the not-yet-evaluated active mutations that
// existed when planning started. newInputs is the number of physical-input
// changes published since the last successfully completed plan. It is a
// scheduling watermark, not an active-mutation count: a long transaction must
// not re-plan old tails at every later statement boundary merely because its
// total write set once crossed the compaction threshold.
type workspaceCompactionPlan struct {
	entries          *workspaceEntrySet
	mutationIDs      []workspaceMutationID
	inputGenerations []uint64
	inputGeneration  uint64
	newInputs        uint64
	currentAttempt   statementAttemptKey
}

func (p *workspaceCompactionPlan) Close() {
	if p == nil || p.entries == nil {
		return
	}
	p.entries.Close()
}

func newOrderedMutationSet() *orderedMutationSet {
	return &orderedMutationSet{
		byID: make(map[workspaceMutationID]*workspaceMutation),
		byOrder: btree.NewBTreeGOptions(
			func(a, b *workspaceMutation) bool {
				if cmp := compareWorkspaceCommitOrder(a.commitOrder, b.commitOrder); cmp != 0 {
					return cmp < 0
				}
				return a.id < b.id
			},
			btree.Options{NoLocks: true},
		),
	}
}

func (s *orderedMutationSet) add(mutation *workspaceMutation) {
	if mutation == nil {
		panic("cannot index a nil workspace mutation")
	}
	if _, exists := s.byID[mutation.id]; exists {
		panic("workspace mutation index contains a duplicate ID")
	}
	if _, replaced := s.byOrder.Set(mutation); replaced {
		panic("workspace mutation commit order is not unique")
	}
	s.byID[mutation.id] = mutation
}

func (s *orderedMutationSet) remove(mutation *workspaceMutation) {
	if mutation == nil || s.byID[mutation.id] != mutation {
		panic("workspace mutation index is inconsistent")
	}
	if removed, ok := s.byOrder.Delete(mutation); !ok || removed != mutation {
		panic("workspace mutation order index is inconsistent")
	}
	delete(s.byID, mutation.id)
}

func (s *orderedMutationSet) contains(id workspaceMutationID) bool {
	_, exists := s.byID[id]
	return exists
}

func (s *orderedMutationSet) len() int {
	return len(s.byID)
}

func (s *orderedMutationSet) ids() []workspaceMutationID {
	ids := make([]workspaceMutationID, 0, len(s.byID))
	s.byOrder.Scan(func(mutation *workspaceMutation) bool {
		ids = append(ids, mutation.id)
		return true
	})
	return ids
}

func (s *orderedMutationSet) equalIDs(expected map[workspaceMutationID]struct{}) bool {
	if len(s.byID) != len(expected) {
		return false
	}
	for id := range s.byID {
		if _, exists := expected[id]; !exists {
			return false
		}
	}
	return true
}

// workspaceMutationIndexData contains immutable secondary-index facts derived
// from the physical payload at publication time. Callers hand these facts to
// appendMutationLocked before transferring Batch ownership to PayloadStore;
// overlay indexes therefore never need to reopen or rescan payloads.
type workspaceMutationIndexData struct {
	objectIDs []types.Objectid
	// objectNames retains the exact ObjectName strings referenced by an
	// ObjectMeta payload. Clone protection is keyed by this physical name, not
	// by Objectid, so both indexes are derived once from the same publication.
	objectNames []string
	blockMeta   bool
	pkKeys      []string
	rowIDs      []objectio.Rowid
	// memoryInsert identifies payloads whose rows participate in local
	// read-your-writes. The completeness flags prove that every row in that
	// payload has the corresponding immutable point-access fact.
	memoryInsert       bool
	pkIndexComplete    bool
	rowIDIndexComplete bool
}

type workspaceOverlayKey struct {
	accountID  uint32
	databaseID uint64
	tableID    uint64
}

// tableOverlay owns the derived current-state and statement-history indexes for
// one exact account/database/table key. It stores stable mutation IDs rather
// than payload positions. All fields are protected by txnWorkspace.mu; logical
// visibility remains authoritative in txnWorkspace.mutations.
type tableOverlay struct {
	// retiredMutations is the history needed only by ReadViews published by
	// the currently executing statement. EndStatement expires those views and
	// clears this journal; history therefore never grows with transaction age.
	retiredMutations            []workspaceMutationID
	retiredPKCandidateMutations []workspaceMutationID
	retiredObjectDeletes        []workspaceObjectDeleteID
	activeMutations             *orderedMutationSet
	activeMemoryDeleteMutations map[workspaceMutationID]struct{}
	activeObjectDeleteMutations map[workspaceMutationID]struct{}
	// activeInsertPKMutations resolves an encoded primary key directly to the
	// active in-memory INSERT mutations that can contain it. The nested count
	// preserves correctness when one mutation contains duplicate encoded keys.
	activeInsertPKMutations map[string]workspaceMutationOwners
	// historicalInsertPKMutations retains point-access facts removed from the
	// current index while a statement ReadView can still observe their payload
	// generation. It is statement-scoped and cleared by EndStatement, so exact
	// lookups never have to scan transaction-wide mutations merely because the
	// workspace advanced after the ReadView was published.
	historicalInsertPKMutations map[string][]workspaceAccessIndexInterval
	// activeUnindexedMemoryInserts is the explicit completeness boundary of the
	// point index. A key miss is authoritative only while this set is empty.
	activeUnindexedMemoryInserts     map[workspaceMutationID]struct{}
	historicalUnindexedMemoryInserts []workspaceAccessIndexInterval
	// activeInsertRowIDMutations resolves DELETEs of transaction-local rows
	// directly to their owning INSERT mutations. This prevents each UPDATE
	// from reopening every earlier INSERT payload in the transaction.
	activeInsertRowIDMutations     map[objectio.Rowid]workspaceMutationOwners
	historicalInsertRowIDMutations map[objectio.Rowid][]workspaceAccessIndexInterval
	// activeUnindexedMemoryInsertRowIDs is the completeness proof for the
	// RowID point index. A current-view lookup is authoritative only when this
	// set is empty.
	activeUnindexedMemoryInsertRowIDs     map[workspaceMutationID]struct{}
	historicalUnindexedMemoryInsertRowIDs []workspaceAccessIndexInterval
	// activeMemoryDeletes resolves a committed block row directly to the active
	// in-memory DELETE mutations. Counts preserve overlapping logical deletes
	// and make mutation retirement and selection rollback exact.
	activeMemoryDeletes        map[objectio.Blockid]map[uint32]uint32
	activePendingObjectDeletes map[workspaceObjectDeleteID]struct{}
	// activeUncommittedObjects is the current logical owner of each
	// transaction-local object. Immutable mutation records still retain the
	// publication history needed by read views; object-delete compaction only
	// asks for the current owner and must not walk that history.
	activeUncommittedObjects map[types.Objectid]map[workspaceMutationID]struct{}
	// droppedAt is the statement attempt that made this exact table obsolete.
	// Keeping this state on the overlay prevents a bare table-ID side map from
	// confusing tables across accounts or databases. Statement rollback clears
	// it through StatementJournal ownership.
	droppedAt *statementAttemptKey
}

// workspaceTableOp is one name-binding change published by a statement
// attempt. DDL name visibility belongs to the transaction workspace just like
// row mutations: readers must either observe the latest active binding or the
// deletion that shadows committed catalog state, and statement rollback must
// remove exactly the publishing attempt.
type workspaceTableOp struct {
	kind              int
	tableID           uint64
	payload           *txnTable
	owner             statementAttemptKey
	previousActiveKey *tableKey
}

type workspaceDatabaseOp struct {
	kind       int
	databaseID uint64
	payload    *txnDatabase
	owner      statementAttemptKey
}

// workspaceDDLCatalog is the transaction-local catalog overlay. Name indexes
// serve transactional database/table lookup, while tableKeysByID avoids the
// former full scan when resolving a table ID. createdTables remains true for
// the transaction lifetime after a successful create statement, even if a
// later statement drops the table; rolling back the create attempt removes it.
type workspaceDDLCatalog struct {
	databases          map[databaseKey][]workspaceDatabaseOp
	tables             map[tableKey][]workspaceTableOp
	tableKeysByID      map[uint64]map[tableKey]struct{}
	activeTableKeyByID map[uint64]tableKey
	createdTables      map[uint64]statementAttemptKey
}

func newWorkspaceDDLCatalog() workspaceDDLCatalog {
	return workspaceDDLCatalog{
		databases:          make(map[databaseKey][]workspaceDatabaseOp),
		tables:             make(map[tableKey][]workspaceTableOp),
		tableKeysByID:      make(map[uint64]map[tableKey]struct{}),
		activeTableKeyByID: make(map[uint64]tableKey),
		createdTables:      make(map[uint64]statementAttemptKey),
	}
}

// workspaceDroppedTables is an immutable commit/compaction view of tables
// made obsolete by active statement attempts. Exact overlay keys drive DML
// filtering; the derived table-ID set exists only for decoding legacy ALTER
// notes, whose wire format carries no account or database identity.
type workspaceDroppedTables struct {
	keys     map[workspaceOverlayKey]struct{}
	tableIDs map[uint64]struct{}
}

func (s workspaceDroppedTables) empty() bool {
	return len(s.keys) == 0
}

func (s workspaceDroppedTables) containsEntry(entry Entry) bool {
	_, ok := s.keys[workspaceOverlayKey{
		accountID:  entry.accountId,
		databaseID: entry.databaseId,
		tableID:    entry.tableId,
	}]
	return ok
}

func (s workspaceDroppedTables) containsTableID(tableID uint64) bool {
	_, ok := s.tableIDs[tableID]
	return ok
}

func newTableOverlay() *tableOverlay {
	return &tableOverlay{
		activeMutations: newOrderedMutationSet(),
		activeUncommittedObjects: make(
			map[types.Objectid]map[workspaceMutationID]struct{}),
		activeMemoryDeleteMutations:       make(map[workspaceMutationID]struct{}),
		activeObjectDeleteMutations:       make(map[workspaceMutationID]struct{}),
		activeInsertPKMutations:           make(map[string]workspaceMutationOwners),
		historicalInsertPKMutations:       make(map[string][]workspaceAccessIndexInterval),
		activeUnindexedMemoryInserts:      make(map[workspaceMutationID]struct{}),
		activeInsertRowIDMutations:        make(map[objectio.Rowid]workspaceMutationOwners),
		historicalInsertRowIDMutations:    make(map[objectio.Rowid][]workspaceAccessIndexInterval),
		activeUnindexedMemoryInsertRowIDs: make(map[workspaceMutationID]struct{}),
		activeMemoryDeletes:               make(map[objectio.Blockid]map[uint32]uint32),
		activePendingObjectDeletes:        make(map[workspaceObjectDeleteID]struct{}),
	}
}

// workspaceObjectDelete is a logical delete against a row in a transaction-
// local object. The source object cannot carry a normal tombstone mutation:
// it must be rewritten before commit. These pending deletes nevertheless
// participate in statement ownership and read-view visibility exactly like
// ordinary mutations.
type workspaceObjectDelete struct {
	id              workspaceObjectDeleteID
	statementID     uint64
	attemptID       uint64
	createdRevision uint64
	retiredRevision uint64
	active          bool
	key             workspaceOverlayKey
	blockID         types.Blockid
	offsets         []int64
}

type workspaceObjectDeleteSnapshot struct {
	ids     []workspaceObjectDeleteID
	blocks  map[types.Blockid][]int64
	objects map[types.Objectid]workspaceObjectMetadata
}

type workspaceObjectMetadata struct {
	accountID          uint32
	databaseID         uint64
	tableID            uint64
	databaseName       string
	tableName          string
	autoIncrEpoch      uint32
	autoIncrEpochKnown bool
}

type workspaceSpillSource struct {
	mutationID  workspaceMutationID
	statementID uint64
	attemptID   uint64
	entry       Entry
	payload     workspaceSpillPayload
}

type workspaceSpillObject struct {
	statementID       uint64
	attemptID         uint64
	sourceMutationIDs []workspaceMutationID
	entry             Entry
}

type workspaceMutationCompaction struct {
	dstMutationID  workspaceMutationID
	dstOldBat      *batch.Batch
	dstNewBat      *batch.Batch
	srcMutationIDs []workspaceMutationID
	srcOldBats     []*batch.Batch
}

type workspaceMutationRewrite struct {
	mutationID workspaceMutationID
	oldBat     *batch.Batch
	selections []int64
	entry      Entry
}

type workspaceMutationRewriteResult struct {
	sourceID workspaceMutationID
	targetID workspaceMutationID
	entry    Entry
}

// workspaceMutationTransition is the generic logical publication primitive
// used by statement finalization. A transition retires an arbitrary source
// set and publishes an arbitrary target set at one workspace revision. The
// source payload generations are pinned by the caller until publication.
type workspaceMutationTransitionSource struct {
	mutationID workspaceMutationID
	oldBat     *batch.Batch
	selections []int64
}

type workspaceMutationTransitionTarget struct {
	entry Entry
	// replacementOf identifies the logical mutation position inherited by
	// this target. Several targets may replace one source; they are ordered as
	// stable children of that source. Zero means this is a genuinely new
	// mutation and therefore belongs at the end of the commit stream.
	replacementOf workspaceMutationID
	selections    []int64
}

type workspaceMutationTransitionResult struct {
	targetIDs []workspaceMutationID
}

// workspaceRollback owns the last visible payload generation of every active
// mutation in one rolled-back statement attempt. It also records all logical
// mutation identities created by the attempt, including sources already
// replaced by spill or rewrite. The payload leases remain valid until Close.
type workspaceRollback struct {
	statementID uint64
	attemptID   uint64
	mutationIDs []workspaceMutationID
	entries     *workspaceEntrySet
	loadFiles   []string
	actions     []func()
}

// RunActions restores transaction-local state changed outside the workspace.
// Actions execute in reverse registration order, matching nested mutation
// semantics. They are detached from StatementJournal before this value is
// returned, so callers must run them after the workspace lock is released.
func (r *workspaceRollback) RunActions() {
	if r == nil {
		return
	}
	for idx := len(r.actions) - 1; idx >= 0; idx-- {
		r.actions[idx]()
	}
	r.actions = nil
}

func (r *workspaceRollback) Close() {
	if r == nil {
		return
	}
	r.actions = nil
	if r.entries != nil {
		r.entries.Close()
	}
}

// workspaceSpillAttempt owns all source leases from selection until either
// atomic publication or abort. Close must be called on every path.
type workspaceSpillAttempt struct {
	workspace *txnWorkspace
	sources   []workspaceSpillSource
	owner     statementAttemptKey
	terminal  bool
	committed bool
	closed    bool
}

func (a *workspaceSpillAttempt) sourceIDs() []workspaceMutationID {
	ids := make([]workspaceMutationID, len(a.sources))
	for idx := range a.sources {
		ids[idx] = a.sources[idx].mutationID
	}
	return ids
}

func (a *workspaceSpillAttempt) Close() {
	if a == nil || a.closed {
		return
	}
	a.closed = true
	if !a.committed {
		spills := make([]workspaceSpillPayload, len(a.sources))
		for idx := range a.sources {
			spills[idx] = a.sources[idx].payload
		}
		a.workspace.payloads.abortSpills(spills)
	}
	for idx := range a.sources {
		a.sources[idx].payload.lease.Close()
	}
}

// statementAttempt is the sole statement-rollback owner for one
// StatementID/AttemptID pair. Physical replacements transfer mutation
// membership within this owner; a retry creates a new AttemptID instead of
// reopening or reusing the rolled-back attempt.
type statementAttempt struct {
	statementID      uint64
	attemptID        uint64
	nextWriteScopeID uint64
	// activeWriteScopes owns every Compile execution currently contributing to
	// this statement attempt. Compile branches (for example UNION branches and
	// internal Data Branch SQL) may finish in a different order from the one in
	// which they started, so this is an ownership set rather than a nesting
	// stack. A statement boundary is publishable only after the set is empty.
	activeWriteScopes map[uint64]struct{}
	// mutations owns the active physical representatives of logical writes
	// created by this attempt. Physical rewrite/spill publication transfers
	// membership from retired source IDs to their replacement IDs, so rollback
	// never scans stale representatives and the journal size is bounded by the
	// attempt's current logical state rather than its rewrite history.
	mutations     map[workspaceMutationID]struct{}
	objectDeletes []workspaceObjectDeleteID
	droppedTables []workspaceOverlayKey
	databaseOps   []databaseKey
	tableOps      []tableKey
	createdTables []uint64
	// loadFiles are physical objects created by LOAD TABLE during this exact
	// attempt. Statement rollback removes only this attempt's objects, while
	// transaction rollback uses the journal's transaction-level ordered list.
	loadFiles []string
	// rollbackActions restore transaction-local objects mutated by this
	// attempt but owned outside txnWorkspace, such as cached txnTable schema
	// state changed by ALTER TABLE. They are executed outside the workspace
	// lock and discarded only after a successful statement boundary.
	rollbackActions []func()
	// selectionUndo records the selection set that was visible before this
	// attempt first changed an older mutation. Selection changes are logical
	// writes and therefore belong to statement rollback just like newly
	// appended mutations.
	selectionUndo map[workspaceMutationID][]int64
	// rewriteUndo records mutations from completed attempts that this attempt
	// replaced. Rollback publishes a new mutation carrying the preceding
	// metadata and payload generation; it never rewinds visibility bounds, so
	// read views captured before, during and after the rewrite remain valid.
	rewriteUndo map[workspaceMutationID]workspaceMutationRewriteUndo
	state       statementAttemptState
}

type workspaceMutationRewriteUndo struct {
	mutationID  workspaceMutationID
	payloadID   workspacePayloadID
	statementID uint64
	attemptID   uint64
	commitOrder workspaceCommitOrder
	entry       Entry
	bat         *batch.Batch
	selections  []int64
}

type statementAttemptState uint8

const (
	statementAttemptOpen statementAttemptState = iota + 1
	statementAttemptCompleted
	statementAttemptRolledBack
)

type statementAttemptKey struct {
	statementID uint64
	attemptID   uint64
}

// statementJournal owns statement and retry-attempt identity. A retry keeps
// the logical StatementID and advances AttemptID; mutation identifiers are
// never rewound or reused.
type statementJournal struct {
	current      *statementAttempt
	retryPending bool
	// loadFiles preserves transaction publication order while loadFileCounts
	// answers transaction ownership without scanning statement history.
	// The current attempt keeps its own list for statement rollback; completed
	// attempts need no retained journal object because transaction rollback
	// deletes the transaction-level list in full.
	loadFiles      []string
	loadFileCounts map[string]int
	// executionOpen and boundaryAdvanced describe the frontend statement
	// lifecycle around the logical attempt owned by this journal. Keeping the
	// guards here prevents Transaction from maintaining a second statement
	// state machine beside StatementJournal.
	executionOpen    bool
	boundaryAdvanced bool
	rc               rcStatementJournal
}

// rcStatementJournal is the only owner of RC statement snapshot history and
// the CN tombstone-transfer cursor. These values describe statement-boundary
// visibility, so publishing them outside StatementJournal would let the
// logical statement and its transfer boundary advance independently.
type rcStatementJournal struct {
	lastTransferred types.TS
	snapshots       []timestamp.Timestamp
	pendingTransfer bool
}

type rcStatementState struct {
	lastTransferred types.TS
	snapshots       []timestamp.Timestamp
	pendingTransfer bool
}

// rcBoundaryState is the constant-size state needed by ordinary statement
// and tombstone-transfer execution. Snapshot history remains private to the
// journal and is copied only for diagnostics: exposing it through this hot
// path would make every RC statement boundary proportional to transaction
// history.
type rcBoundaryState struct {
	lastTransferred types.TS
	pendingTransfer bool
}

// rcBoundaryPublication is applied under the same workspace lock as the
// logical mutation transition. recordStatement distinguishes a completed RC
// statement from AdvanceSnapshot/commit cursor movement.
type rcBoundaryPublication struct {
	recordStatement   bool
	statementSnapshot timestamp.Timestamp
	lastTransferred   types.TS
	pendingTransfer   bool
}

// workspaceBoundaryPublication is an all-or-nothing logical boundary plan.
// Validation covers both statement identity and the optional RC snapshot
// transition before either component is applied under txnWorkspace.mu.
type workspaceBoundaryPublication struct {
	advanceStatement bool
	rc               *rcBoundaryPublication
}

func newStatementJournal() statementJournal {
	current := &statementAttempt{
		attemptID: 1,
		state:     statementAttemptOpen,
		mutations: make(map[workspaceMutationID]struct{}),
	}
	journal := statementJournal{
		current:        current,
		loadFileCounts: make(map[string]int),
	}
	return journal
}

func (a statementAttempt) key() statementAttemptKey {
	return statementAttemptKey{
		statementID: a.statementID,
		attemptID:   a.attemptID,
	}
}

func (j *statementJournal) beginExecution() error {
	if j.executionOpen {
		return moerr.NewInternalErrorNoCtx("BUG: StartStatement called twice")
	}
	j.executionOpen = true
	j.boundaryAdvanced = false
	return nil
}

func (j *statementJournal) endExecution() error {
	if !j.executionOpen {
		return moerr.NewInternalErrorNoCtx("BUG: StartStatement not called")
	}
	j.executionOpen = false
	j.boundaryAdvanced = false
	return nil
}

func (j *statementJournal) markBoundaryAdvanced() error {
	if !j.executionOpen {
		return moerr.NewInternalErrorNoCtx("BUG: StartStatement not called")
	}
	if j.boundaryAdvanced {
		return moerr.NewInternalErrorNoCtx("BUG: IncrStatementID called twice")
	}
	j.boundaryAdvanced = true
	return nil
}

func (j *statementJournal) reopenBoundary() {
	j.boundaryAdvanced = false
}

func (j *statementJournal) appendMutation(
	statementID uint64,
	attemptID uint64,
	mutationID workspaceMutationID,
) error {
	if j.current == nil || j.current.statementID != statementID ||
		j.current.attemptID != attemptID {
		return moerr.NewInternalErrorNoCtx(
			"workspace mutation does not belong to the current statement attempt")
	}
	if j.current.state != statementAttemptOpen {
		return moerr.NewInternalErrorNoCtx(
			"workspace mutation belongs to a closed statement attempt")
	}
	if j.current.mutations == nil {
		j.current.mutations = make(map[workspaceMutationID]struct{})
	}
	if _, duplicate := j.current.mutations[mutationID]; duplicate {
		return moerr.NewInternalErrorNoCtx(
			"workspace mutation is already owned by the current statement attempt")
	}
	j.current.mutations[mutationID] = struct{}{}
	return nil
}

func (j *statementJournal) validateMutationReplacement(
	owner statementAttemptKey,
	sourceIDs []workspaceMutationID,
) error {
	if j.current == nil || j.current.state != statementAttemptOpen {
		return moerr.NewInternalErrorNoCtx(
			"workspace mutation replacement belongs to an invalid statement attempt")
	}
	return j.validateMutationOwnership(owner, sourceIDs)
}

// validatePhysicalMutationReplacement permits an owner-preserving payload
// replacement while a statement retry is between attempts. IncrStatementID
// compacts and spills transaction-level workspace payload before it advances
// a rolled-back attempt to its retry attempt; completed statements remain
// transaction-visible during that preparation. Their physical representation
// may therefore change without becoming a write of either the failed attempt
// or its successor.
//
// The exception is deliberately narrower than an ordinary rewrite: the owner
// must be an earlier completed attempt and every source must remain outside the
// rolled-back attempt's ownership set. Logical rewrites still require an open
// attempt through validateMutationReplacement, so retry rollback cannot
// resurrect failed writes or absorb completed writes into the retry attempt.
func (j *statementJournal) validatePhysicalMutationReplacement(
	owner statementAttemptKey,
	sourceIDs []workspaceMutationID,
) error {
	if j.current == nil {
		return moerr.NewInternalErrorNoCtx(
			"workspace physical mutation replacement has no statement attempt")
	}
	if j.current.state == statementAttemptOpen {
		return j.validateMutationOwnership(owner, sourceIDs)
	}
	if j.current.state == statementAttemptRolledBack && j.retryPending &&
		owner != j.current.key() {
		return j.validateMutationOwnership(owner, sourceIDs)
	}
	return moerr.NewInternalErrorNoCtx(
		"workspace physical mutation replacement belongs to an invalid statement attempt")
}

// validateBoundaryMutationReplacement validates source ownership before an
// atomic statement-boundary transition publishes its target attempt. A retry
// boundary is the one legal transition whose source journal is already
// rolled back: advance() will create the replacement attempt while the same
// workspace lock is held. Ordinary rewrites remain restricted to an open
// attempt by validateMutationReplacement.
func (j *statementJournal) validateBoundaryMutationReplacement(
	owner statementAttemptKey,
	sourceIDs []workspaceMutationID,
	advanceStatement bool,
) error {
	if j.current == nil {
		return moerr.NewInternalErrorNoCtx(
			"workspace mutation replacement has no statement attempt")
	}
	if j.current.state == statementAttemptOpen {
		return j.validateMutationOwnership(owner, sourceIDs)
	}
	if advanceStatement && j.retryPending &&
		j.current.state == statementAttemptRolledBack {
		return j.validateMutationOwnership(owner, sourceIDs)
	}
	return moerr.NewInternalErrorNoCtx(
		"workspace mutation replacement belongs to an invalid statement boundary")
}

func (j *statementJournal) validateMutationOwnership(
	owner statementAttemptKey,
	sourceIDs []workspaceMutationID,
) error {
	currentOwner := owner == j.current.key()
	for _, sourceID := range sourceIDs {
		_, owned := j.current.mutations[sourceID]
		if owned != currentOwner {
			return moerr.NewInternalErrorNoCtx(
				"workspace mutation replacement has inconsistent statement ownership")
		}
	}
	return nil
}

// replaceMutationsValidated transfers statement rollback ownership across a
// physical mutation replacement. Targets may already belong to the current
// attempt (for example, transition publication uses the ordinary append path),
// so ownership is a set union rather than an append-only event stream. The
// caller validates every source before changing payload or mutation state,
// making the ownership publication part of the same atomic workspace
// transition.
func (j *statementJournal) replaceMutationsValidated(
	owner statementAttemptKey,
	sourceIDs []workspaceMutationID,
	targetIDs []workspaceMutationID,
) {
	if owner != j.current.key() {
		return
	}
	for _, sourceID := range sourceIDs {
		delete(j.current.mutations, sourceID)
	}
	for _, targetID := range targetIDs {
		j.current.mutations[targetID] = struct{}{}
	}
}

// replaceTerminalMutationsValidated attaches the materialized commit
// representation to the final open attempt. Sources may belong to completed
// attempts, which deliberately no longer own statement rollback state. The
// caller must validate that the final boundary is still open before invoking
// this helper; adding targets to the final attempt keeps rollback ownership
// internally consistent until transaction commit completes.
func (j *statementJournal) replaceTerminalMutationsValidated(
	sourceIDs []workspaceMutationID,
	targetIDs []workspaceMutationID,
) {
	for _, sourceID := range sourceIDs {
		delete(j.current.mutations, sourceID)
	}
	for _, targetID := range targetIDs {
		j.current.mutations[targetID] = struct{}{}
	}
}

func (j *statementJournal) appendObjectDelete(
	statementID uint64,
	attemptID uint64,
	deleteID workspaceObjectDeleteID,
) error {
	if j.current == nil || j.current.statementID != statementID ||
		j.current.attemptID != attemptID {
		return moerr.NewInternalErrorNoCtx(
			"workspace object delete does not belong to the current statement attempt")
	}
	if j.current.state != statementAttemptOpen {
		return moerr.NewInternalErrorNoCtx(
			"workspace object delete belongs to a closed statement attempt")
	}
	j.current.objectDeletes = append(j.current.objectDeletes, deleteID)
	return nil
}

func (j *statementJournal) appendDroppedTable(
	statementID uint64,
	attemptID uint64,
	key workspaceOverlayKey,
) error {
	if j.current == nil || j.current.statementID != statementID ||
		j.current.attemptID != attemptID {
		return moerr.NewInternalErrorNoCtx(
			"workspace dropped table does not belong to the current statement attempt")
	}
	if j.current.state != statementAttemptOpen {
		return moerr.NewInternalErrorNoCtx(
			"workspace dropped table belongs to a closed statement attempt")
	}
	j.current.droppedTables = append(j.current.droppedTables, key)
	return nil
}

func (j *statementJournal) appendDatabaseOp(
	owner statementAttemptKey,
	key databaseKey,
) error {
	if j.current == nil || j.current.key() != owner ||
		j.current.state != statementAttemptOpen {
		return moerr.NewInternalErrorNoCtx(
			"workspace database operation belongs to an invalid statement attempt")
	}
	j.current.databaseOps = append(j.current.databaseOps, key)
	return nil
}

func (j *statementJournal) appendTableOp(
	owner statementAttemptKey,
	key tableKey,
) error {
	if j.current == nil || j.current.key() != owner ||
		j.current.state != statementAttemptOpen {
		return moerr.NewInternalErrorNoCtx(
			"workspace table operation belongs to an invalid statement attempt")
	}
	j.current.tableOps = append(j.current.tableOps, key)
	return nil
}

func (j *statementJournal) appendCreatedTable(
	owner statementAttemptKey,
	tableID uint64,
) error {
	if j.current == nil || j.current.key() != owner ||
		j.current.state != statementAttemptOpen {
		return moerr.NewInternalErrorNoCtx(
			"workspace created table belongs to an invalid statement attempt")
	}
	j.current.createdTables = append(j.current.createdTables, tableID)
	return nil
}

func (j *statementJournal) appendRollbackAction(action func()) error {
	if action == nil {
		return moerr.NewInternalErrorNoCtx(
			"workspace statement rollback action is nil")
	}
	if j.current == nil || j.current.state != statementAttemptOpen {
		return moerr.NewInternalErrorNoCtx(
			"workspace statement rollback action belongs to an invalid attempt")
	}
	j.current.rollbackActions = append(j.current.rollbackActions, action)
	return nil
}

func (j *statementJournal) appendLoadFiles(names ...string) error {
	if j.current == nil || j.current.state != statementAttemptOpen {
		return moerr.NewInternalErrorNoCtx(
			"workspace LOAD file belongs to an invalid statement attempt")
	}
	j.current.loadFiles = append(j.current.loadFiles, names...)
	j.loadFiles = append(j.loadFiles, names...)
	for _, name := range names {
		j.loadFileCounts[name]++
	}
	return nil
}

func (j *statementJournal) allLoadFiles() []string {
	return slices.Clone(j.loadFiles)
}

func (j *statementJournal) hasLoadFile(name string) bool {
	return j.loadFileCounts[name] != 0
}

// releaseLoadFiles removes exactly the supplied ownership occurrences. The
// same physical name can be referenced by multiple statement attempts, so a
// name-based delete-all would incorrectly discard completed-statement
// ownership while rolling back a later attempt.
func (j *statementJournal) releaseLoadFiles(names ...string) error {
	if len(names) == 0 {
		return nil
	}
	releaseCounts := make(map[string]int, len(names))
	for _, name := range names {
		releaseCounts[name]++
	}
	for name, count := range releaseCounts {
		if j.loadFileCounts[name] < count {
			return moerr.NewInternalErrorNoCtxf(
				"workspace LOAD file ownership mismatch for %s: have %d, release %d",
				name, j.loadFileCounts[name], count)
		}
	}
	j.loadFiles = removeWorkspaceLoadFileOccurrencesFromEnd(
		j.loadFiles, releaseCounts)
	j.current.loadFiles = removeWorkspaceLoadFileOccurrencesFromEnd(
		j.current.loadFiles, releaseCounts)
	for name, count := range releaseCounts {
		remaining := j.loadFileCounts[name] - count
		if remaining == 0 {
			delete(j.loadFileCounts, name)
		} else {
			j.loadFileCounts[name] = remaining
		}
	}
	return nil
}

// removeWorkspaceLoadFileOccurrencesFromEnd preserves the publication order
// of earlier statements when a later attempt reused the same physical name.
func removeWorkspaceLoadFileOccurrencesFromEnd(
	names []string,
	counts map[string]int,
) []string {
	remaining := maps.Clone(counts)
	remove := make([]bool, len(names))
	for idx := len(names) - 1; idx >= 0; idx-- {
		name := names[idx]
		if remaining[name] != 0 {
			remaining[name]--
			remove[idx] = true
		}
	}
	kept := names[:0]
	for idx, name := range names {
		if !remove[idx] {
			kept = append(kept, name)
		}
	}
	clear(names[len(kept):])
	return kept
}

func (j *statementJournal) recordSelectionUndo(
	mutationID workspaceMutationID,
	selections []int64,
) {
	if j.current.selectionUndo == nil {
		j.current.selectionUndo = make(map[workspaceMutationID][]int64)
	}
	if _, recorded := j.current.selectionUndo[mutationID]; recorded {
		return
	}
	j.current.selectionUndo[mutationID] = slices.Clone(selections)
}

func (j *statementJournal) recordRewriteUndo(
	undo workspaceMutationRewriteUndo,
) {
	// A mutation created by this attempt disappears with the attempt. It must
	// not be restored as an older logical write.
	if undo.statementID == j.current.statementID &&
		undo.attemptID == j.current.attemptID {
		return
	}
	if j.current.rewriteUndo == nil {
		j.current.rewriteUndo = make(
			map[workspaceMutationID]workspaceMutationRewriteUndo)
	}
	if _, recorded := j.current.rewriteUndo[undo.mutationID]; recorded {
		return
	}
	undo.entry.bat = nil
	undo.selections = slices.Clone(undo.selections)
	j.current.rewriteUndo[undo.mutationID] = undo
}

func (j *statementJournal) validateAdvance() error {
	if !j.retryPending && len(j.current.activeWriteScopes) != 0 {
		return moerr.NewInternalErrorNoCtx(
			"workspace statement has unfinished write scopes")
	}
	return nil
}

func (j *statementJournal) advance() (statementAttempt, error) {
	if err := j.validateAdvance(); err != nil {
		return statementAttempt{}, err
	}
	return j.advanceValidated(), nil
}

// advanceValidated publishes a statement transition after validateAdvance has
// succeeded while the workspace lock is continuously held. Keeping this phase
// infallible is required by compound boundary publication: payload generations
// may already have changed, so the journal transition must not introduce a
// later error that could expose a partially published workspace revision.
func (j *statementJournal) advanceValidated() statementAttempt {
	if j.retryPending {
		j.current = &statementAttempt{
			statementID: j.current.statementID,
			attemptID:   j.current.attemptID + 1,
			state:       statementAttemptOpen,
			mutations:   make(map[workspaceMutationID]struct{}),
		}
		j.retryPending = false
		return *j.current
	}

	j.current.state = statementAttemptCompleted
	// A completed statement can no longer be rolled back through
	// RollbackLastStatement. Release captured table state at the exact
	// boundary where the statement becomes durable within this transaction.
	j.current.rollbackActions = nil
	j.current = &statementAttempt{
		statementID: j.current.statementID + 1,
		attemptID:   1,
		state:       statementAttemptOpen,
		mutations:   make(map[workspaceMutationID]struct{}),
	}
	return *j.current
}

func (j *statementJournal) publishRCBoundary(boundary rcBoundaryPublication) {
	if boundary.recordStatement {
		j.rc.snapshots = append(j.rc.snapshots, boundary.statementSnapshot)
	}
	j.rc.lastTransferred = boundary.lastTransferred
	j.rc.pendingTransfer = boundary.pendingTransfer
}

func (j *statementJournal) rcState() rcStatementState {
	return rcStatementState{
		lastTransferred: j.rc.lastTransferred,
		snapshots:       slices.Clone(j.rc.snapshots),
		pendingTransfer: j.rc.pendingTransfer,
	}
}

func (j *statementJournal) rcBoundaryState() rcBoundaryState {
	return rcBoundaryState{
		lastTransferred: j.rc.lastTransferred,
		pendingTransfer: j.rc.pendingTransfer,
	}
}

func (j *statementJournal) rollbackRCBoundary(completedStatements uint64) error {
	if err := j.validateRollbackRCBoundary(completedStatements); err != nil {
		return err
	}
	j.rollbackRCBoundaryValidated(completedStatements)
	return nil
}

func (j *statementJournal) validateRollbackRCBoundary(completedStatements uint64) error {
	if completedStatements > uint64(len(j.rc.snapshots)) {
		return moerr.NewInternalErrorNoCtxf(
			"workspace RC history mismatch: have %d snapshots, need %d completed statements",
			len(j.rc.snapshots),
			completedStatements,
		)
	}
	return nil
}

func (j *statementJournal) rollbackRCBoundaryValidated(completedStatements uint64) {
	j.rc.snapshots = j.rc.snapshots[:completedStatements]
	if completedStatements == 0 {
		j.rc.lastTransferred = types.TS{}
		j.rc.pendingTransfer = false
		return
	}
	lastSnapshot := j.rc.snapshots[completedStatements-1]
	if lastSnapshot.Less(j.rc.lastTransferred.ToTimestamp()) {
		j.rc.lastTransferred = types.TimestampToTS(lastSnapshot)
	}
}

func (j *statementJournal) rollbackCurrent() statementAttempt {
	j.current.state = statementAttemptRolledBack
	j.retryPending = true
	result := *j.current
	result.mutations = maps.Clone(result.mutations)
	result.objectDeletes = slices.Clone(result.objectDeletes)
	result.droppedTables = slices.Clone(result.droppedTables)
	result.databaseOps = slices.Clone(result.databaseOps)
	result.tableOps = slices.Clone(result.tableOps)
	result.createdTables = slices.Clone(result.createdTables)
	result.loadFiles = slices.Clone(result.loadFiles)
	result.rollbackActions = slices.Clone(result.rollbackActions)
	// Ownership moves to workspaceRollback so callbacks can run after the
	// workspace lock is released and the journal does not retain closures.
	j.current.rollbackActions = nil
	result.selectionUndo = make(map[workspaceMutationID][]int64, len(j.current.selectionUndo))
	for id, selections := range j.current.selectionUndo {
		result.selectionUndo[id] = slices.Clone(selections)
	}
	result.rewriteUndo = make(
		map[workspaceMutationID]workspaceMutationRewriteUndo,
		len(j.current.rewriteUndo),
	)
	for id, undo := range j.current.rewriteUndo {
		result.rewriteUndo[id] = undo
	}
	return result
}

var nextWorkspaceID atomic.Uint64

// txnWorkspace is the single CN owner of logical mutation visibility,
// statement-attempt state, table overlays and transaction-local DDL. Its
// identifiers remain stable when physical payloads are merged, spilled or
// released; no caller observes an internal slice position.
//
// mu protects every logical owner and index in this type. PayloadStore has its
// own lock and may be entered while mu is held; it must never call back into
// txnWorkspace. Batch cleanup and external I/O run only after workspace and
// payload-store locks have been released.
type txnWorkspace struct {
	mu sync.RWMutex

	id                      uint64
	closed                  bool
	revision                uint64
	minimumReadableRevision uint64

	nextMutationID                workspaceMutationID
	nextObjectDeleteID            workspaceObjectDeleteID
	nextCompactionInputGeneration uint64
	completedCompactionGeneration uint64
	// commitEpoch is the protocol-order boundary last published by an ordinary
	// Compile. Internal SQL captures CurrentReadView and therefore inherits the
	// caller's epoch. This is deliberately independent of StatementID: one
	// frontend statement can advance attempt ownership before its Compile and
	// can execute several internal SQL write scopes that must still be ordered
	// as one TN precommit suffix.
	commitEpoch           uint64
	activeMutations       *orderedMutationSet
	activePKCandidates    *orderedMutationSet
	activeCompactionCount int
	pendingCompactions    *orderedMutationSet
	activeBlockMeta       *orderedMutationSet
	activeObjectDeletes   map[workspaceObjectDeleteID]struct{}
	retiredMutationIDs    map[workspaceMutationID]struct{}
	retiredObjectDeletes  map[workspaceObjectDeleteID]struct{}
	published             client.WorkspaceReadView
	journal               statementJournal

	mutations     map[workspaceMutationID]*workspaceMutation
	objectDeletes map[workspaceObjectDeleteID]*workspaceObjectDelete
	// activeObjectReferences maps a physical object name to the active
	// mutations that reference it. Clone GC is a current-state query; keeping
	// this index active-only makes its cost independent of transaction history.
	// Historical read views resolve immutable mutation records and do not use
	// this index.
	activeObjectReferences map[string]map[workspaceMutationID]struct{}
	overlays               map[workspaceOverlayKey]*tableOverlay
	ddl                    workspaceDDLCatalog
	payloads               *workspacePayloadStore
	usage                  workspaceUsage
	// commitStarted closes the statement-retry protocol before terminal spill
	// may combine mutations owned by different completed statements. Commit
	// preparation may still publish internal mutations (for example transferred
	// tombstones), and transaction rollback still consumes the complete active
	// workspace. Only statement-local rollback and ordinary spill are invalid
	// after this boundary.
	commitStarted bool
}

func newTxnWorkspace() *txnWorkspace {
	id := nextWorkspaceID.Add(1)
	return &txnWorkspace{
		id:                   id,
		journal:              newStatementJournal(),
		mutations:            make(map[workspaceMutationID]*workspaceMutation),
		activeMutations:      newOrderedMutationSet(),
		activePKCandidates:   newOrderedMutationSet(),
		pendingCompactions:   newOrderedMutationSet(),
		activeBlockMeta:      newOrderedMutationSet(),
		retiredMutationIDs:   make(map[workspaceMutationID]struct{}),
		objectDeletes:        make(map[workspaceObjectDeleteID]*workspaceObjectDelete),
		activeObjectDeletes:  make(map[workspaceObjectDeleteID]struct{}),
		retiredObjectDeletes: make(map[workspaceObjectDeleteID]struct{}),
		activeObjectReferences: make(
			map[string]map[workspaceMutationID]struct{}),
		overlays: make(map[workspaceOverlayKey]*tableOverlay),
		ddl:      newWorkspaceDDLCatalog(),
		payloads: newWorkspacePayloadStore(),
	}
}

func (w *txnWorkspace) tableOverlayLocked(key workspaceOverlayKey) *tableOverlay {
	overlay := w.overlays[key]
	if overlay == nil {
		overlay = newTableOverlay()
		w.overlays[key] = overlay
	}
	return overlay
}

func (w *txnWorkspace) addCreatedTable(tableID uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	owner := w.journal.current.key()
	if previous, exists := w.ddl.createdTables[tableID]; exists {
		if previous == owner {
			return nil
		}
		return moerr.NewInternalErrorNoCtxf(
			"workspace table %d was already created by another statement attempt",
			tableID,
		)
	}
	if err := w.journal.appendCreatedTable(owner, tableID); err != nil {
		return err
	}
	w.ddl.createdTables[tableID] = owner
	w.revision++
	return nil
}

func (w *txnWorkspace) addStatementRollbackAction(action func()) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.journal.appendRollbackAction(action)
}

func (w *txnWorkspace) tableCreatedInTxn(tableID uint64) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	_, exists := w.ddl.createdTables[tableID]
	return exists
}

func (w *txnWorkspace) addTableOp(
	key tableKey,
	kind int,
	tableID uint64,
	payload *txnTable,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	owner := w.journal.current.key()
	var previousActiveKey *tableKey
	if previous, exists := w.ddl.activeTableKeyByID[tableID]; exists {
		previousCopy := previous
		previousActiveKey = &previousCopy
	}
	if kind == INSERT {
		if previousActiveKey != nil && *previousActiveKey != key {
			return moerr.NewInternalErrorNoCtxf(
				"workspace table %d has multiple active name bindings",
				tableID,
			)
		}
	}
	if err := w.journal.appendTableOp(owner, key); err != nil {
		return err
	}
	if kind == INSERT {
		w.ddl.activeTableKeyByID[tableID] = key
	} else if previousActiveKey != nil && *previousActiveKey == key {
		delete(w.ddl.activeTableKeyByID, tableID)
	}
	w.ddl.tables[key] = append(w.ddl.tables[key], workspaceTableOp{
		kind:              kind,
		tableID:           tableID,
		payload:           payload,
		owner:             owner,
		previousActiveKey: previousActiveKey,
	})
	keys := w.ddl.tableKeysByID[tableID]
	if keys == nil {
		keys = make(map[tableKey]struct{})
		w.ddl.tableKeysByID[tableID] = keys
	}
	keys[key] = struct{}{}
	w.revision++
	return nil
}

func (w *txnWorkspace) tableDeleted(key tableKey) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	ops := w.ddl.tables[key]
	return len(ops) != 0 && ops[len(ops)-1].kind == DELETE
}

func (w *txnWorkspace) activeTable(key tableKey) *txnTable {
	w.mu.RLock()
	defer w.mu.RUnlock()
	ops := w.ddl.tables[key]
	if len(ops) == 0 || ops[len(ops)-1].kind != INSERT {
		return nil
	}
	return ops[len(ops)-1].payload
}

// tableNameByID returns an active transaction-local name first. If no active
// binding exists but this transaction has touched the ID, deleted is true so
// callers do not fall through to committed catalog state.
func (w *txnWorkspace) tableNameByID(tableID uint64) (
	databaseName string,
	tableName string,
	deleted bool,
) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if key, exists := w.ddl.activeTableKeyByID[tableID]; exists {
		return key.dbName, key.name, false
	}
	keys := w.ddl.tableKeysByID[tableID]
	for key := range keys {
		for _, op := range w.ddl.tables[key] {
			if op.tableID == tableID {
				return "", "", true
			}
		}
	}
	return "", "", false
}

func (w *txnWorkspace) addDatabaseOp(
	key databaseKey,
	kind int,
	databaseID uint64,
	payload *txnDatabase,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	owner := w.journal.current.key()
	if err := w.journal.appendDatabaseOp(owner, key); err != nil {
		return err
	}
	w.ddl.databases[key] = append(w.ddl.databases[key], workspaceDatabaseOp{
		kind:       kind,
		databaseID: databaseID,
		payload:    payload,
		owner:      owner,
	})
	w.revision++
	return nil
}

func (w *txnWorkspace) databaseDeleted(key databaseKey) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	ops := w.ddl.databases[key]
	return len(ops) != 0 && ops[len(ops)-1].kind == DELETE
}

func (w *txnWorkspace) activeDatabase(key databaseKey) *txnDatabase {
	w.mu.RLock()
	defer w.mu.RUnlock()
	ops := w.ddl.databases[key]
	if len(ops) == 0 || ops[len(ops)-1].kind != INSERT {
		return nil
	}
	return ops[len(ops)-1].payload
}

func (w *txnWorkspace) ddlString() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return stringifyMap(w.ddl.tables, func(a1, a2 any) string {
		key := a1.(tableKey)
		return fmt.Sprintf("%v-%v-%v-%v:%v",
			key.accountId,
			key.databaseId,
			key.dbName,
			key.name,
			stringifySlice(a2, func(a any) string {
				op := a.(workspaceTableOp)
				if op.kind == DELETE {
					return fmt.Sprintf("DEL-%v@%v/%v",
						op.tableID, op.owner.statementID, op.owner.attemptID)
				}
				return fmt.Sprintf("INS-%v-%v@%v/%v",
					op.payload.tableId,
					op.payload.tableName,
					op.owner.statementID,
					op.owner.attemptID)
			}),
		)
	})
}

func (w *txnWorkspace) markTableDropped(
	accountID uint32,
	databaseID uint64,
	tableID uint64,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	key := workspaceOverlayKey{
		accountID:  accountID,
		databaseID: databaseID,
		tableID:    tableID,
	}
	attempt := w.journal.current.key()
	overlay := w.tableOverlayLocked(key)
	if overlay.droppedAt != nil {
		if *overlay.droppedAt == attempt {
			return nil
		}
		return moerr.NewInternalErrorNoCtx(
			"workspace table is already dropped by another statement attempt")
	}
	if err := w.journal.appendDroppedTable(
		attempt.statementID,
		attempt.attemptID,
		key,
	); err != nil {
		return err
	}
	overlay.droppedAt = &attempt
	w.revision++
	return nil
}

func (w *txnWorkspace) droppedTablesSnapshot() workspaceDroppedTables {
	w.mu.RLock()
	defer w.mu.RUnlock()

	snapshot := workspaceDroppedTables{
		keys:     make(map[workspaceOverlayKey]struct{}),
		tableIDs: make(map[uint64]struct{}),
	}
	for key, overlay := range w.overlays {
		if overlay.droppedAt == nil {
			continue
		}
		snapshot.keys[key] = struct{}{}
		snapshot.tableIDs[key.tableID] = struct{}{}
	}
	return snapshot
}

// droppedTableEntries atomically captures the current dropped-table set and
// pins only mutations owned by those exact table overlays. Drop cleanup must
// not scan or pin unrelated transaction tables after the overlay has already
// resolved the affected account/database/table identities.
func (w *txnWorkspace) droppedTableEntries() (
	workspaceDroppedTables,
	*workspaceEntrySet,
	error,
) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	snapshot := workspaceDroppedTables{
		keys:     make(map[workspaceOverlayKey]struct{}),
		tableIDs: make(map[uint64]struct{}),
	}
	var ids []workspaceMutationID
	for key, overlay := range w.overlays {
		if overlay.droppedAt == nil {
			continue
		}
		snapshot.keys[key] = struct{}{}
		snapshot.tableIDs[key.tableID] = struct{}{}
		ids = append(ids, overlay.activeMutations.ids()...)
	}
	if len(ids) == 0 {
		return snapshot, &workspaceEntrySet{}, nil
	}
	slices.SortStableFunc(ids, func(aID, bID workspaceMutationID) int {
		a := w.mutations[aID]
		b := w.mutations[bID]
		if a == nil || b == nil {
			return 0
		}
		return compareWorkspaceCommitOrder(a.commitOrder, b.commitOrder)
	})
	view := client.NewWorkspaceReadView(
		w.id,
		w.revision,
		uint64(w.nextMutationID),
	)
	entries, err := w.entriesForMutationIDsLocked(view, ids)
	return snapshot, entries, err
}

func workspaceObjectReferences(
	entry Entry,
	bat *batch.Batch,
) ([]types.Objectid, []string) {
	if entry.typ != INSERT || bat == nil || bat.IsEmpty() {
		return nil, nil
	}
	statsIdx := slices.Index(bat.Attrs, catalog.ObjectMeta_ObjectStats)
	if statsIdx == -1 {
		return nil, nil
	}

	vec := bat.Vecs[statsIdx]
	ids := make([]types.Objectid, 0, vec.Length())
	names := make([]string, 0, vec.Length())
	seen := make(map[types.Objectid]struct{}, vec.Length())
	for row := range vec.Length() {
		stats := objectio.ObjectStats(vec.GetBytesAt(row))
		objectID := *stats.ObjectName().ObjectId()
		if _, duplicate := seen[objectID]; duplicate {
			continue
		}
		seen[objectID] = struct{}{}
		ids = append(ids, objectID)
		names = append(names, stats.ObjectName().String())
	}
	return ids, names
}

func classifyWorkspaceMutation(
	entry Entry,
	bat *batch.Batch,
) workspaceMutationIndexData {
	objectIDs, objectNames := workspaceObjectReferences(entry, bat)
	data := workspaceMutationIndexData{
		objectIDs:   objectIDs,
		objectNames: objectNames,
		blockMeta: entry.typ == INSERT && bat != nil && !bat.IsEmpty() &&
			len(bat.Attrs) != 0 && bat.Attrs[0] == catalog.BlockMeta_BlockInfo,
	}
	data.memoryInsert = entry.typ == INSERT && entry.fileName == "" &&
		bat != nil && !bat.IsEmpty() && !data.blockMeta
	if bat == nil || bat.IsEmpty() || entry.fileName != "" {
		return data
	}
	if entry.typ == INSERT && entry.pkIndex.enabled {
		// The access index is derived state, not the owner of PK descriptor
		// validation. Keep malformed descriptors unindexed so the existing
		// transaction invariant check can return its contractual error instead of
		// changing append into a new panic boundary.
		if entry.pkIndex.vectorPos >= 0 && entry.pkIndex.vectorPos < len(bat.Vecs) {
			packer := types.NewPacker()
			keys := readutil.EncodePrimaryKeyVector(
				bat.Vecs[entry.pkIndex.vectorPos], packer)
			data.pkKeys = make([]string, len(keys))
			for row := range keys {
				// EncodePrimaryKeyVector returns Packer-backed slices. Store immutable
				// strings before the packer is reset or released.
				data.pkKeys[row] = string(slices.Clone(keys[row]))
			}
			packer.Close()
			data.pkIndexComplete = len(data.pkKeys) == bat.RowCount()
		}
	}
	if entry.typ == INSERT && data.memoryInsert &&
		len(bat.Vecs) != 0 && bat.Vecs[0].GetType().Oid == types.T_Rowid {
		data.rowIDs = slices.Clone(
			vector.MustFixedColNoTypeCheck[objectio.Rowid](bat.Vecs[0]))
		data.rowIDIndexComplete = len(data.rowIDs) == bat.RowCount()
	}
	if entry.typ == DELETE && bat.Vecs[0].GetType().Oid == types.T_Rowid {
		data.rowIDs = slices.Clone(
			vector.MustFixedColNoTypeCheck[objectio.Rowid](bat.Vecs[0]))
	}
	return data
}

func visibleWorkspaceRows(rowCount int, selections []int64) []bool {
	// nil is the canonical zero-allocation representation of "all rows are
	// indexed". Selection-bearing generations materialize the bitmap because
	// retirement must remove exactly the rows published into TableOverlay.
	if len(selections) == 0 {
		return nil
	}
	visible := make([]bool, rowCount)
	for row := range rowCount {
		visible[row] = true
	}
	for _, row := range selections {
		if row >= 0 && row < int64(rowCount) {
			visible[row] = false
		}
	}
	return visible
}

func workspaceRowIndexed(indexedRows []bool, row int) bool {
	return len(indexedRows) == 0 ||
		(row < len(indexedRows) && indexedRows[row])
}

func (w *txnWorkspace) publishReadView() client.WorkspaceReadView {
	w.mu.Lock()
	defer w.mu.Unlock()
	// This is the logical equivalent of the former snapshotWriteOffset
	// frontier. Only ordinary Compile creation calls PublishReadView; internal
	// SQL calls CurrentReadView and must not split the caller's protocol-order
	// group. Advancing before capturing the view makes all mutations published
	// by this Compile share one immutable commit epoch.
	w.commitEpoch++
	w.published = client.NewWorkspaceReadView(
		w.id,
		w.revision,
		uint64(w.nextMutationID),
	)
	return w.published
}

func (w *txnWorkspace) currentReadView() client.WorkspaceReadView {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return client.NewWorkspaceReadView(
		w.id,
		w.revision,
		uint64(w.nextMutationID),
	)
}

func (w *txnWorkspace) publishedReadView() client.WorkspaceReadView {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.published
}

type workspaceDiagnosticSnapshot struct {
	revision      uint64
	published     client.WorkspaceReadView
	statementID   uint64
	attemptID     uint64
	rc            rcStatementState
	activeEntries []string
}

func (w *txnWorkspace) diagnosticSnapshot() workspaceDiagnosticSnapshot {
	w.mu.RLock()
	defer w.mu.RUnlock()

	snapshot := workspaceDiagnosticSnapshot{
		revision:    w.revision,
		published:   w.published,
		statementID: w.journal.current.statementID,
		attemptID:   w.journal.current.attemptID,
		rc:          w.journal.rcState(),
	}
	for _, id := range w.activeMutationIDsInCommitOrderLocked() {
		mutation := w.mutations[id]
		snapshot.activeEntries = append(snapshot.activeEntries, mutation.entry.String())
	}
	return snapshot
}

func (w *txnWorkspace) usageSnapshot() workspaceUsage {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.usage
}

func (w *txnWorkspace) activeMutationCount() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return 0
	}
	return w.activeMutations.len()
}

func (w *txnWorkspace) validateUsage() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	type expectedActiveOverlay struct {
		mutations            map[workspaceMutationID]struct{}
		pkCandidate          map[workspaceMutationID]struct{}
		compaction           map[workspaceMutationID]struct{}
		blockMeta            map[workspaceMutationID]struct{}
		memoryDelete         map[workspaceMutationID]struct{}
		objectDelete         map[workspaceMutationID]struct{}
		insertPK             map[string]map[workspaceMutationID]uint32
		unindexedInsert      map[workspaceMutationID]struct{}
		insertRowID          map[objectio.Rowid]map[workspaceMutationID]uint32
		unindexedInsertRowID map[workspaceMutationID]struct{}
		memoryRows           map[objectio.Blockid]map[uint32]uint32
	}
	actual := workspaceUsage{}
	expectedActiveMutations := make(map[workspaceMutationID]struct{})
	expectedPKCandidates := make(map[workspaceMutationID]struct{})
	expectedCompactionCount := 0
	expectedBlockMeta := make(map[workspaceMutationID]struct{})
	expectedActiveOverlays := make(map[workspaceOverlayKey]*expectedActiveOverlay)
	expectedObjectOwners := make(
		map[workspaceOverlayKey]map[types.Objectid]map[workspaceMutationID]struct{})
	expectedObjectReferences := make(
		map[string]map[workspaceMutationID]struct{})
	for id := range w.activeMutations.byID {
		mutation := w.mutations[id]
		if mutation == nil || !mutation.active || mutation.retiredRevision != 0 {
			return moerr.NewInternalErrorNoCtx(
				"workspace active mutation index contains retired state")
		}
		expectedActiveMutations[id] = struct{}{}
		bat, err := w.payloads.currentBatch(mutation.payloadID)
		if err != nil {
			return err
		}
		actual.add(usageOfWorkspaceEntry(mutation.entry, bat))
		key := workspaceOverlayKey{
			accountID:  mutation.entry.accountId,
			databaseID: mutation.entry.databaseId,
			tableID:    mutation.entry.tableId,
		}
		expectedOverlay := expectedActiveOverlays[key]
		if expectedOverlay == nil {
			expectedOverlay = &expectedActiveOverlay{
				mutations:            make(map[workspaceMutationID]struct{}),
				pkCandidate:          make(map[workspaceMutationID]struct{}),
				compaction:           make(map[workspaceMutationID]struct{}),
				blockMeta:            make(map[workspaceMutationID]struct{}),
				memoryDelete:         make(map[workspaceMutationID]struct{}),
				objectDelete:         make(map[workspaceMutationID]struct{}),
				insertPK:             make(map[string]map[workspaceMutationID]uint32),
				unindexedInsert:      make(map[workspaceMutationID]struct{}),
				insertRowID:          make(map[objectio.Rowid]map[workspaceMutationID]uint32),
				unindexedInsertRowID: make(map[workspaceMutationID]struct{}),
				memoryRows:           make(map[objectio.Blockid]map[uint32]uint32),
			}
			expectedActiveOverlays[key] = expectedOverlay
		}
		expectedOverlay.mutations[id] = struct{}{}
		if mutation.entry.pkCheck.enabled {
			expectedOverlay.pkCandidate[id] = struct{}{}
			expectedPKCandidates[id] = struct{}{}
		}
		if mutation.entry.typ == INSERT || mutation.entry.typ == DELETE {
			expectedOverlay.compaction[id] = struct{}{}
			expectedCompactionCount++
		}
		if mutation.blockMeta {
			expectedOverlay.blockMeta[id] = struct{}{}
			expectedBlockMeta[id] = struct{}{}
		}
		if mutation.entry.typ == DELETE {
			if mutation.entry.fileName == "" {
				expectedOverlay.memoryDelete[id] = struct{}{}
			} else {
				expectedOverlay.objectDelete[id] = struct{}{}
			}
		}
		selections, err := w.payloads.currentSelections(mutation.payloadID)
		if err != nil {
			return err
		}
		visible := visibleWorkspaceRows(
			max(len(mutation.pkKeys), len(mutation.rowIDs)), selections)
		if !slices.Equal(mutation.indexedRows, visible) {
			return moerr.NewInternalErrorNoCtx(
				"workspace mutation access-index visibility mismatch")
		}
		if mutation.entry.typ == INSERT {
			if mutation.memoryInsert && !mutation.pkIndexComplete {
				expectedOverlay.unindexedInsert[id] = struct{}{}
			}
			if mutation.memoryInsert && !mutation.rowIDIndexComplete {
				expectedOverlay.unindexedInsertRowID[id] = struct{}{}
			}
			for row, pk := range mutation.pkKeys {
				if !workspaceRowIndexed(visible, row) {
					continue
				}
				owners := expectedOverlay.insertPK[pk]
				if owners == nil {
					owners = make(map[workspaceMutationID]uint32)
					expectedOverlay.insertPK[pk] = owners
				}
				owners[id]++
			}
			for row, rowID := range mutation.rowIDs {
				if !workspaceRowIndexed(visible, row) {
					continue
				}
				owners := expectedOverlay.insertRowID[rowID]
				if owners == nil {
					owners = make(map[workspaceMutationID]uint32)
					expectedOverlay.insertRowID[rowID] = owners
				}
				owners[id]++
			}
		}
		if mutation.entry.typ == DELETE && mutation.entry.fileName == "" {
			for row := range mutation.rowIDs {
				if !workspaceRowIndexed(visible, row) {
					continue
				}
				rowID := &mutation.rowIDs[row]
				blockID := rowID.CloneBlockID()
				offsets := expectedOverlay.memoryRows[blockID]
				if offsets == nil {
					offsets = make(map[uint32]uint32)
					expectedOverlay.memoryRows[blockID] = offsets
				}
				offsets[rowID.GetRowOffset()]++
			}
		}
		for _, objectID := range mutation.objectIDs {
			objects := expectedObjectOwners[key]
			if objects == nil {
				objects = make(
					map[types.Objectid]map[workspaceMutationID]struct{})
				expectedObjectOwners[key] = objects
			}
			owners := objects[objectID]
			if owners == nil {
				owners = make(map[workspaceMutationID]struct{})
				objects[objectID] = owners
			}
			owners[id] = struct{}{}
		}
		for _, objectName := range mutation.objectNames {
			refs := expectedObjectReferences[objectName]
			if refs == nil {
				refs = make(map[workspaceMutationID]struct{})
				expectedObjectReferences[objectName] = refs
			}
			refs[id] = struct{}{}
		}
	}
	if actual != w.usage {
		return moerr.NewInternalErrorNoCtxf(
			"workspace usage mismatch: got %+v, expected %+v",
			w.usage,
			actual,
		)
	}
	setsEqual := func(
		actualSet map[workspaceMutationID]struct{},
		expectedSet map[workspaceMutationID]struct{},
	) bool {
		if len(actualSet) != len(expectedSet) {
			return false
		}
		for id := range actualSet {
			if _, exists := expectedSet[id]; !exists {
				return false
			}
		}
		return true
	}
	countMapsEqual := func(
		actualMap map[uint32]uint32,
		expectedMap map[uint32]uint32,
	) bool {
		if len(actualMap) != len(expectedMap) {
			return false
		}
		for key, count := range actualMap {
			if expectedMap[key] != count {
				return false
			}
		}
		return true
	}
	ownerMapsEqual := func(
		actualMap workspaceMutationOwners,
		expectedMap map[workspaceMutationID]uint32,
	) bool {
		if actualMap.len() != len(expectedMap) {
			return false
		}
		equal := true
		actualMap.forEach(func(id workspaceMutationID, count uint32) {
			if expectedMap[id] != count {
				equal = false
			}
		})
		return equal
	}
	if !w.activeMutations.equalIDs(expectedActiveMutations) {
		return moerr.NewInternalErrorNoCtx(
			"workspace active mutation index mismatch")
	}
	if !w.activePKCandidates.equalIDs(expectedPKCandidates) ||
		w.activeCompactionCount != expectedCompactionCount ||
		!w.activeBlockMeta.equalIDs(expectedBlockMeta) {
		return moerr.NewInternalErrorNoCtx(
			"workspace active secondary index mismatch")
	}
	for id, mutation := range w.pendingCompactions.byID {
		if mutation == nil || w.mutations[id] != mutation || !mutation.active ||
			(mutation.entry.typ != INSERT && mutation.entry.typ != DELETE) {
			return moerr.NewInternalErrorNoCtx(
				"workspace pending compaction index mismatch")
		}
	}
	for key, overlay := range w.overlays {
		expectedOverlay := expectedActiveOverlays[key]
		if expectedOverlay == nil {
			expectedOverlay = &expectedActiveOverlay{
				mutations:            map[workspaceMutationID]struct{}{},
				pkCandidate:          map[workspaceMutationID]struct{}{},
				compaction:           map[workspaceMutationID]struct{}{},
				blockMeta:            map[workspaceMutationID]struct{}{},
				memoryDelete:         map[workspaceMutationID]struct{}{},
				objectDelete:         map[workspaceMutationID]struct{}{},
				insertPK:             map[string]map[workspaceMutationID]uint32{},
				unindexedInsert:      map[workspaceMutationID]struct{}{},
				insertRowID:          map[objectio.Rowid]map[workspaceMutationID]uint32{},
				unindexedInsertRowID: map[workspaceMutationID]struct{}{},
				memoryRows:           map[objectio.Blockid]map[uint32]uint32{},
			}
		}
		if !overlay.activeMutations.equalIDs(expectedOverlay.mutations) ||
			!setsEqual(overlay.activeMemoryDeleteMutations, expectedOverlay.memoryDelete) ||
			!setsEqual(overlay.activeObjectDeleteMutations, expectedOverlay.objectDelete) ||
			!setsEqual(overlay.activeUnindexedMemoryInserts, expectedOverlay.unindexedInsert) ||
			!setsEqual(overlay.activeUnindexedMemoryInsertRowIDs, expectedOverlay.unindexedInsertRowID) {
			return moerr.NewInternalErrorNoCtx(
				"workspace active overlay secondary index mismatch")
		}
		if len(overlay.activeInsertPKMutations) != len(expectedOverlay.insertPK) {
			return moerr.NewInternalErrorNoCtx(
				"workspace primary-key access index size mismatch")
		}
		for pk, owners := range overlay.activeInsertPKMutations {
			if !ownerMapsEqual(owners, expectedOverlay.insertPK[pk]) {
				return moerr.NewInternalErrorNoCtx(
					"workspace primary-key access index mismatch")
			}
		}
		if len(overlay.activeInsertRowIDMutations) != len(expectedOverlay.insertRowID) {
			return moerr.NewInternalErrorNoCtx(
				"workspace insert-rowid access index size mismatch")
		}
		for rowID, owners := range overlay.activeInsertRowIDMutations {
			if !ownerMapsEqual(owners, expectedOverlay.insertRowID[rowID]) {
				return moerr.NewInternalErrorNoCtx(
					"workspace insert-rowid access index mismatch")
			}
		}
		if len(overlay.activeMemoryDeletes) != len(expectedOverlay.memoryRows) {
			return moerr.NewInternalErrorNoCtx(
				"workspace row-delete access index size mismatch")
		}
		for blockID, offsets := range overlay.activeMemoryDeletes {
			if !countMapsEqual(offsets, expectedOverlay.memoryRows[blockID]) {
				return moerr.NewInternalErrorNoCtx(
					"workspace row-delete access index mismatch")
			}
		}
		expectedObjects := expectedObjectOwners[key]
		if len(overlay.activeUncommittedObjects) != len(expectedObjects) {
			return moerr.NewInternalErrorNoCtx(
				"workspace active object owner index size mismatch")
		}
		for objectID, owners := range overlay.activeUncommittedObjects {
			expectedOwners := expectedObjects[objectID]
			if len(owners) != len(expectedOwners) {
				return moerr.NewInternalErrorNoCtx(
					"workspace active object owner index mismatch")
			}
			for id := range owners {
				if _, exists := expectedOwners[id]; !exists {
					return moerr.NewInternalErrorNoCtx(
						"workspace active object owner index mismatch")
				}
			}
		}
	}
	if len(w.activeObjectReferences) != len(expectedObjectReferences) {
		return moerr.NewInternalErrorNoCtx(
			"workspace active object reference index size mismatch")
	}
	for name, refs := range w.activeObjectReferences {
		expectedRefs := expectedObjectReferences[name]
		if len(refs) != len(expectedRefs) {
			return moerr.NewInternalErrorNoCtx(
				"workspace active object reference index mismatch")
		}
		for id := range refs {
			if _, exists := expectedRefs[id]; !exists {
				return moerr.NewInternalErrorNoCtx(
					"workspace active object reference index mismatch")
			}
		}
	}
	expectedLoadFileCounts := make(map[string]int)
	for _, name := range w.journal.loadFiles {
		expectedLoadFileCounts[name]++
	}
	if len(expectedLoadFileCounts) != len(w.journal.loadFileCounts) {
		return moerr.NewInternalErrorNoCtx(
			"workspace LOAD file index size mismatch")
	}
	for name, count := range w.journal.loadFileCounts {
		if expectedLoadFileCounts[name] != count {
			return moerr.NewInternalErrorNoCtx(
				"workspace LOAD file index mismatch")
		}
	}
	currentLoadFileCounts := make(map[string]int)
	for _, name := range w.journal.current.loadFiles {
		currentLoadFileCounts[name]++
	}
	for name, count := range currentLoadFileCounts {
		if count > w.journal.loadFileCounts[name] {
			return moerr.NewInternalErrorNoCtx(
				"workspace current-attempt LOAD file ownership mismatch")
		}
	}
	return nil
}

func (w *txnWorkspace) beginWriteAttempt() client.WorkspaceWriteMark {
	w.mu.Lock()
	defer w.mu.Unlock()
	attempt := w.journal.current
	attempt.nextWriteScopeID++
	if attempt.activeWriteScopes == nil {
		attempt.activeWriteScopes = make(map[uint64]struct{})
	}
	attempt.activeWriteScopes[attempt.nextWriteScopeID] = struct{}{}
	return client.NewWorkspaceWriteMark(
		w.id,
		attempt.statementID,
		attempt.attemptID,
		uint64(w.nextMutationID),
		attempt.nextWriteScopeID,
	)
}

// adjustAttempt validates that the completed execution still owns the write
// mark it started with. Commit order is assigned when a logical mutation is
// created and is immutable afterwards; statement completion must not rewrite
// positions already observable through a published read view.
func (w *txnWorkspace) adjustAttempt(mark client.WorkspaceWriteMark) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.validateWriteMarkLocked(mark); err != nil {
		return err
	}
	attempt := w.journal.current
	if _, active := attempt.activeWriteScopes[mark.WriteScopeID()]; !active {
		return moerr.NewInternalErrorNoCtx(
			"workspace write scope is not active")
	}
	delete(attempt.activeWriteScopes, mark.WriteScopeID())
	return nil
}

func (w *txnWorkspace) advanceStatement() (statementAttempt, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.journal.advance()
}

func (w *txnWorkspace) beginStatementExecution() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.journal.beginExecution()
}

// endStatementExecution expires the current execution's ReadViews and detaches
// reclaimable payloads while holding the logical workspace lock. It returns
// Batches to be cleaned by Transaction after the lock has been released.
func (w *txnWorkspace) endStatementExecution() ([]*batch.Batch, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.journal.endExecution(); err != nil {
		return nil, err
	}

	// A WorkspaceReadView is a statement-scoped lease over immutable logical
	// history, not a transaction-scoped snapshot. Advance the revision even
	// when the statement published no mutation so every view handed to this
	// execution expires atomically before its retired metadata is reclaimed.
	// Existing payload leases remain valid until Close; only new resolution
	// through the expired view is rejected.
	w.revision++
	w.minimumReadableRevision = w.revision

	protected := make(map[*batch.Batch]struct{}, len(w.journal.current.rewriteUndo))
	for _, undo := range w.journal.current.rewriteUndo {
		if undo.bat != nil {
			protected[undo.bat] = struct{}{}
		}
	}
	reclaimed := w.payloads.reclaimRetired(protected)
	w.reclaimRetiredMetadataLocked()
	return reclaimed, nil
}

// reclaimRetiredMetadataLocked removes metadata that no live statement
// ReadView can resolve. It walks only mutation IDs retired since the previous
// boundary (plus rollback-protected rewrite sources), never the complete
// transaction workspace. Physical payload leases have an independent
// lifetime in PayloadStore, so a reader pinned before EndStatement may safely
// outlive this metadata.
func (w *txnWorkspace) reclaimRetiredMetadataLocked() {
	protectedMutations := make(
		map[workspaceMutationID]struct{}, len(w.journal.current.rewriteUndo))
	for id := range w.journal.current.rewriteUndo {
		protectedMutations[id] = struct{}{}
	}

	for id := range w.retiredMutationIDs {
		mutation := w.mutations[id]
		if mutation == nil {
			panic("workspace retired mutation index is inconsistent")
		}
		if mutation.retiredRevision == 0 ||
			mutation.retiredRevision > w.minimumReadableRevision {
			continue
		}
		if _, protected := protectedMutations[id]; protected {
			continue
		}
		delete(w.mutations, id)
		delete(w.retiredMutationIDs, id)
	}

	protectedObjectDeletes := make(
		map[workspaceObjectDeleteID]struct{}, len(w.journal.current.objectDeletes))
	for _, id := range w.journal.current.objectDeletes {
		protectedObjectDeletes[id] = struct{}{}
	}
	for id := range w.retiredObjectDeletes {
		objectDelete := w.objectDeletes[id]
		if objectDelete == nil {
			panic("workspace retired object delete index is inconsistent")
		}
		if objectDelete.retiredRevision == 0 ||
			objectDelete.retiredRevision > w.minimumReadableRevision {
			continue
		}
		if _, protected := protectedObjectDeletes[id]; protected {
			continue
		}
		delete(w.objectDeletes, id)
		delete(w.retiredObjectDeletes, id)
	}

	for _, overlay := range w.overlays {
		overlay.retiredMutations = nil
		overlay.retiredPKCandidateMutations = nil
		overlay.retiredObjectDeletes = nil
		clear(overlay.historicalInsertPKMutations)
		overlay.historicalUnindexedMemoryInserts = nil
		clear(overlay.historicalInsertRowIDMutations)
		overlay.historicalUnindexedMemoryInsertRowIDs = nil
	}
}

func (w *txnWorkspace) markStatementBoundaryAdvanced() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.journal.markBoundaryAdvanced()
}

func (w *txnWorkspace) reopenStatementBoundary() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.journal.reopenBoundary()
}

func (w *txnWorkspace) appendLoadFiles(names ...string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.journal.appendLoadFiles(names...)
}

func (w *txnWorkspace) allLoadFiles() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.journal.allLoadFiles()
}

// prepareLoadFileCleanup releases references that are still shared with an
// earlier statement and returns physical names whose final ownership must be
// retained until file-service deletion succeeds.
func (w *txnWorkspace) prepareLoadFileCleanup(names ...string) ([]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	requested := make(map[string]int, len(names))
	for _, name := range names {
		requested[name]++
	}
	shared := make([]string, 0, len(names))
	physical := make([]string, 0, len(requested))
	for name, count := range requested {
		owned := w.journal.loadFileCounts[name]
		if owned < count {
			return nil, moerr.NewInternalErrorNoCtxf(
				"workspace LOAD file ownership mismatch for %s: have %d, cleanup %d",
				name, owned, count)
		}
		if owned == count {
			physical = append(physical, name)
			continue
		}
		for range count {
			shared = append(shared, name)
		}
	}
	if err := w.journal.releaseLoadFiles(shared...); err != nil {
		return nil, err
	}
	slices.Sort(physical)
	return physical, nil
}

func (w *txnWorkspace) completePhysicalLoadFileCleanup(names ...string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	owned := make([]string, 0, len(names))
	for _, name := range names {
		for range w.journal.loadFileCounts[name] {
			owned = append(owned, name)
		}
	}
	return w.journal.releaseLoadFiles(owned...)
}

func (w *txnWorkspace) hasLoadFile(name string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.journal.hasLoadFile(name)
}

func (w *txnWorkspace) rcState() rcStatementState {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.journal.rcState()
}

func (w *txnWorkspace) rcBoundaryState() rcBoundaryState {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.journal.rcBoundaryState()
}

func (w *txnWorkspace) advanceRCStatement(boundary rcBoundaryPublication) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.publishBoundaryLocked(workspaceBoundaryPublication{
		advanceStatement: true,
		rc:               &boundary,
	})
}

func (w *txnWorkspace) rollbackRCBoundary(completedStatements uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.journal.rollbackRCBoundary(completedStatements)
}

func (w *txnWorkspace) append(entry Entry) workspaceMutationID {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.usage.add(usageOfWorkspaceEntry(entry, entry.bat))
	w.revision++
	indexData := classifyWorkspaceMutation(entry, entry.bat)
	payloadID := w.payloads.addMemory(entry.bat, nil, w.revision)
	entry.bat = nil
	w.appendMutationLocked(
		entry,
		payloadID,
		indexData,
		w.revision,
		w.journal.current.statementID,
		w.journal.current.attemptID,
	)
	return w.nextMutationID
}

func (w *txnWorkspace) appendObjectDelete(
	accountID uint32,
	databaseID uint64,
	tableID uint64,
	blockID types.Blockid,
	offsets []int64,
) workspaceObjectDeleteID {
	if len(offsets) == 0 {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	w.revision++
	w.nextObjectDeleteID++
	deleteID := w.nextObjectDeleteID
	key := workspaceOverlayKey{
		accountID:  accountID,
		databaseID: databaseID,
		tableID:    tableID,
	}
	objectDelete := &workspaceObjectDelete{
		id:              deleteID,
		statementID:     w.journal.current.statementID,
		attemptID:       w.journal.current.attemptID,
		createdRevision: w.revision,
		active:          true,
		key:             key,
		blockID:         blockID,
		offsets:         slices.Clone(offsets),
	}
	w.objectDeletes[deleteID] = objectDelete
	w.activeObjectDeletes[deleteID] = struct{}{}
	if err := w.journal.appendObjectDelete(
		objectDelete.statementID,
		objectDelete.attemptID,
		deleteID,
	); err != nil {
		panic(err)
	}
	overlay := w.tableOverlayLocked(key)
	overlay.activePendingObjectDeletes[deleteID] = struct{}{}
	return deleteID
}

func objectDeleteVisibleAt(
	objectDelete *workspaceObjectDelete,
	view client.WorkspaceReadView,
) bool {
	if objectDelete == nil || view.IsZero() ||
		objectDelete.createdRevision > view.Revision() {
		return false
	}
	return objectDelete.retiredRevision == 0 ||
		objectDelete.retiredRevision > view.Revision()
}

func (w *txnWorkspace) tableObjectDeletes(
	view client.WorkspaceReadView,
	accountID uint32,
	databaseID uint64,
	tableID uint64,
) (map[types.Blockid][]int64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	result := make(map[types.Blockid][]int64)
	err := w.forEachTableObjectDeleteLocked(
		view,
		workspaceOverlayKey{
			accountID:  accountID,
			databaseID: databaseID,
			tableID:    tableID,
		},
		func(objectDelete *workspaceObjectDelete) {
			result[objectDelete.blockID] = append(
				result[objectDelete.blockID], objectDelete.offsets...)
		},
	)
	return result, err
}

// tableObjectDeleteCount returns the number of logical row deletes against
// transaction-local objects for one exact table and ReadView. These deletes
// are not ordinary DELETE mutations because their source objects must be
// rewritten before commit, but they still participate in reads and aggregate
// counts before that rewrite happens.
func (w *txnWorkspace) tableObjectDeleteCount(
	view client.WorkspaceReadView,
	accountID uint32,
	databaseID uint64,
	tableID uint64,
) (uint64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var count uint64
	err := w.forEachTableObjectDeleteLocked(
		view,
		workspaceOverlayKey{
			accountID:  accountID,
			databaseID: databaseID,
			tableID:    tableID,
		},
		func(objectDelete *workspaceObjectDelete) {
			count += uint64(len(objectDelete.offsets))
		},
	)
	return count, err
}

// forEachTableObjectDeleteLocked is the single visibility implementation for
// pending object deletes. Callers must hold txnWorkspace.mu for reading or
// writing; the callback must not re-enter the workspace.
func (w *txnWorkspace) forEachTableObjectDeleteLocked(
	view client.WorkspaceReadView,
	key workspaceOverlayKey,
	fn func(*workspaceObjectDelete),
) error {
	if err := w.validateReadViewLocked(view); err != nil {
		return err
	}
	overlay := w.overlays[key]
	if overlay == nil {
		return nil
	}
	deleteIDs := make([]workspaceObjectDeleteID, 0,
		len(overlay.activePendingObjectDeletes)+len(overlay.retiredObjectDeletes))
	for deleteID := range overlay.activePendingObjectDeletes {
		deleteIDs = append(deleteIDs, deleteID)
	}
	deleteIDs = append(deleteIDs, overlay.retiredObjectDeletes...)
	slices.Sort(deleteIDs)
	for _, deleteID := range deleteIDs {
		objectDelete := w.objectDeletes[deleteID]
		if !objectDeleteVisibleAt(objectDelete, view) {
			continue
		}
		fn(objectDelete)
	}
	return nil
}

// hasTableTombstones reports whether one exact table has any transaction-local
// delete visible through view. Both ordinary DELETE mutations (in-memory
// rowids and persisted tombstone objects) and pending deletes against
// transaction-local CN objects participate in the same logical visibility
// boundary. Callers must not infer this state from transaction-wide physical
// containers because that both crosses table identities and bypasses ReadView
// generation pinning.
func (w *txnWorkspace) hasTableTombstones(
	view client.WorkspaceReadView,
	accountID uint32,
	databaseID uint64,
	tableID uint64,
) (bool, error) {
	entries, err := w.tableEntries(view, accountID, databaseID, tableID)
	if err != nil {
		return false, err
	}
	defer entries.Close()
	for idx := range entries.entries {
		entry := &entries.entries[idx]
		if entry.typ == DELETE && entry.visibleRowCount() != 0 {
			return true, nil
		}
	}

	objectDeleteCount, err := w.tableObjectDeleteCount(
		view, accountID, databaseID, tableID)
	if err != nil {
		return false, err
	}
	return objectDeleteCount != 0, nil
}

func (w *txnWorkspace) snapshotObjectDeletes() workspaceObjectDeleteSnapshot {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.snapshotObjectDeletesLocked()
}

func (w *txnWorkspace) snapshotObjectDeletesLocked() workspaceObjectDeleteSnapshot {
	snapshot := workspaceObjectDeleteSnapshot{
		blocks: make(map[types.Blockid][]int64),
	}
	snapshot.ids = make([]workspaceObjectDeleteID, 0, len(w.activeObjectDeletes))
	for deleteID := range w.activeObjectDeletes {
		snapshot.ids = append(snapshot.ids, deleteID)
	}
	slices.Sort(snapshot.ids)
	for _, deleteID := range snapshot.ids {
		objectDelete := w.objectDeletes[deleteID]
		if objectDelete == nil || !objectDelete.active || objectDelete.retiredRevision != 0 {
			panic("workspace active object delete index is inconsistent")
		}
		snapshot.blocks[objectDelete.blockID] = append(
			snapshot.blocks[objectDelete.blockID], objectDelete.offsets...)
	}
	return snapshot
}

func (w *txnWorkspace) snapshotObjectDeleteCompaction() (
	workspaceObjectDeleteSnapshot,
	error,
) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	snapshot := w.snapshotObjectDeletesLocked()
	snapshot.objects = make(map[types.Objectid]workspaceObjectMetadata)
	for _, deleteID := range snapshot.ids {
		objectDelete := w.objectDeletes[deleteID]
		objectID := *objectDelete.blockID.Object()
		metadata, err := w.activeObjectMetadataLocked(objectDelete.key, objectID)
		if err != nil {
			return workspaceObjectDeleteSnapshot{}, err
		}
		if existing, exists := snapshot.objects[objectID]; exists && existing != metadata {
			return workspaceObjectDeleteSnapshot{}, moerr.NewInternalErrorNoCtx(
				"workspace object belongs to multiple table overlays")
		}
		snapshot.objects[objectID] = metadata
	}
	return snapshot, nil
}

func (w *txnWorkspace) activeObjectMetadataLocked(
	key workspaceOverlayKey,
	objectID types.Objectid,
) (workspaceObjectMetadata, error) {
	overlay := w.overlays[key]
	if overlay == nil {
		return workspaceObjectMetadata{}, moerr.NewInternalErrorNoCtx(
			"workspace table overlay not found during object compaction")
	}

	owners := overlay.activeUncommittedObjects[objectID]
	if len(owners) == 0 {
		return workspaceObjectMetadata{}, moerr.NewInternalErrorNoCtx(
			"workspace object mutation not found during deletion compaction")
	}
	if len(owners) != 1 {
		return workspaceObjectMetadata{}, moerr.NewInternalErrorNoCtx(
			"workspace object has multiple active mutations")
	}
	var mutationID workspaceMutationID
	for mutationID = range owners {
	}
	mutation := w.mutations[mutationID]
	if mutation == nil || !mutation.active || mutation.retiredRevision != 0 {
		return workspaceObjectMetadata{}, moerr.NewInternalErrorNoCtx(
			"workspace active object owner index is inconsistent")
	}
	entry := mutation.entry
	return workspaceObjectMetadata{
		accountID:          entry.accountId,
		databaseID:         entry.databaseId,
		tableID:            entry.tableId,
		databaseName:       entry.databaseName,
		tableName:          entry.tableName,
		autoIncrEpoch:      entry.autoIncrEpoch,
		autoIncrEpochKnown: entry.autoIncrEpochKnown,
	}, nil
}

func (w *txnWorkspace) appendMutationLocked(
	entry Entry,
	payloadID workspacePayloadID,
	indexData workspaceMutationIndexData,
	revision uint64,
	statementID uint64,
	attemptID uint64,
) workspaceMutationID {
	commitOrder := w.nextMutationCommitOrderLocked(entry)
	return w.appendMutationAtCommitOrderLocked(
		entry,
		payloadID,
		indexData,
		revision,
		statementID,
		attemptID,
		commitOrder,
	)
}

// nextMutationCommitOrderLocked assigns a complete, immutable protocol
// position at publication time. commitEpoch is advanced only when an ordinary
// Compile publishes a read view. Internal SQL and sequential write scopes
// inherit that epoch, so catalog mutations they produce after a user-table
// mutation can still precede it in the TN protocol stream. StatementID remains
// solely a rollback/visibility owner and is intentionally not reused as this
// protocol boundary. The final component preserves publication order within
// one class without exposing a physical slice position or requiring Adjust to
// rewrite an already published key.
func (w *txnWorkspace) nextMutationCommitOrderLocked(
	entry Entry,
) workspaceCommitOrder {
	catalogRank := uint64(1)
	if entry.isCatalog() {
		catalogRank = 0
	}
	if entry.typ < 0 || uint64(entry.typ) > uint64(^uint32(0)) {
		panic("workspace mutation type is outside commit-order domain")
	}
	typeRank := uint64(^uint32(0) - uint32(entry.typ))
	return workspaceCommitOrder{
		w.commitEpoch,
		catalogRank,
		typeRank,
		uint64(w.nextMutationID) + 1,
	}
}

func (w *txnWorkspace) appendMutationAtCommitOrderLocked(
	entry Entry,
	payloadID workspacePayloadID,
	indexData workspaceMutationIndexData,
	revision uint64,
	statementID uint64,
	attemptID uint64,
	commitOrder workspaceCommitOrder,
) workspaceMutationID {
	id := w.publishMutationAtCommitOrderLocked(
		entry,
		payloadID,
		indexData,
		revision,
		statementID,
		attemptID,
		commitOrder,
	)
	if err := w.journal.appendMutation(statementID, attemptID, id); err != nil {
		panic(err)
	}
	return id
}

// publishMutationAtCommitOrderLocked publishes one logical mutation into the
// global registry and every derived active/table index as one transition under
// txnWorkspace.mu. The payload generation and immutable index facts must have
// been prepared before entry. This method deliberately does not assign
// statement rollback ownership: replacements use it to preserve existing
// ownership, while ordinary writes use appendMutationAtCommitOrderLocked.
func (w *txnWorkspace) publishMutationAtCommitOrderLocked(
	entry Entry,
	payloadID workspacePayloadID,
	indexData workspaceMutationIndexData,
	revision uint64,
	statementID uint64,
	attemptID uint64,
	commitOrder workspaceCommitOrder,
) workspaceMutationID {
	w.nextMutationID++
	entry.workspaceMutationID = w.nextMutationID

	m := &workspaceMutation{
		id:                  w.nextMutationID,
		statementID:         statementID,
		attemptID:           attemptID,
		createdRevision:     revision,
		commitOrder:         slices.Clone(commitOrder),
		active:              true,
		entry:               entry,
		payloadID:           payloadID,
		objectIDs:           slices.Clone(indexData.objectIDs),
		objectNames:         slices.Clone(indexData.objectNames),
		blockMeta:           indexData.blockMeta,
		pkKeys:              slices.Clone(indexData.pkKeys),
		rowIDs:              slices.Clone(indexData.rowIDs),
		memoryInsert:        indexData.memoryInsert,
		pkIndexComplete:     indexData.pkIndexComplete,
		rowIDIndexComplete:  indexData.rowIDIndexComplete,
		accessIndexRevision: revision,
	}
	w.mutations[m.id] = m
	w.activeMutations.add(m)
	key := workspaceOverlayKey{
		accountID:  entry.accountId,
		databaseID: entry.databaseId,
		tableID:    entry.tableId,
	}
	overlay := w.tableOverlayLocked(key)
	overlay.activeMutations.add(m)
	selections, err := w.payloads.currentSelections(payloadID)
	if err != nil {
		panic(err)
	}
	w.addMutationAccessIndexesLocked(overlay, m, selections)
	if entry.pkCheck.enabled {
		w.activePKCandidates.add(m)
	}
	if entry.typ == INSERT || entry.typ == DELETE {
		w.activeCompactionCount++
		w.queueCompactionLocked(m)
	}
	if indexData.blockMeta {
		w.activeBlockMeta.add(m)
	}
	for _, objectID := range indexData.objectIDs {
		owners := overlay.activeUncommittedObjects[objectID]
		if owners == nil {
			owners = make(map[workspaceMutationID]struct{})
			overlay.activeUncommittedObjects[objectID] = owners
		}
		owners[m.id] = struct{}{}
	}
	for _, objectName := range indexData.objectNames {
		refs := w.activeObjectReferences[objectName]
		if refs == nil {
			refs = make(map[workspaceMutationID]struct{})
			w.activeObjectReferences[objectName] = refs
		}
		refs[m.id] = struct{}{}
	}
	if entry.typ == DELETE {
		if entry.fileName == "" {
			overlay.activeMemoryDeleteMutations[m.id] = struct{}{}
		} else {
			overlay.activeObjectDeleteMutations[m.id] = struct{}{}
		}
	}
	return m.id
}

func (w *txnWorkspace) addMutationAccessIndexesLocked(
	overlay *tableOverlay,
	mutation *workspaceMutation,
	selections []int64,
) {
	rowCount := max(len(mutation.pkKeys), len(mutation.rowIDs))
	mutation.indexedRows = visibleWorkspaceRows(rowCount, selections)
	if mutation.entry.typ == INSERT {
		if mutation.memoryInsert && !mutation.pkIndexComplete {
			overlay.activeUnindexedMemoryInserts[mutation.id] = struct{}{}
		}
		for row, key := range mutation.pkKeys {
			if !workspaceRowIndexed(mutation.indexedRows, row) {
				continue
			}
			owners := overlay.activeInsertPKMutations[key]
			owners.add(mutation.id)
			overlay.activeInsertPKMutations[key] = owners
		}
		if mutation.memoryInsert && !mutation.rowIDIndexComplete {
			overlay.activeUnindexedMemoryInsertRowIDs[mutation.id] = struct{}{}
		}
		for row, rowID := range mutation.rowIDs {
			if !workspaceRowIndexed(mutation.indexedRows, row) {
				continue
			}
			owners := overlay.activeInsertRowIDMutations[rowID]
			owners.add(mutation.id)
			overlay.activeInsertRowIDMutations[rowID] = owners
		}
	}
	if mutation.entry.typ == DELETE && mutation.entry.fileName == "" {
		for row := range mutation.rowIDs {
			if !workspaceRowIndexed(mutation.indexedRows, row) {
				continue
			}
			rowID := &mutation.rowIDs[row]
			blockID := rowID.CloneBlockID()
			offsets := overlay.activeMemoryDeletes[blockID]
			if offsets == nil {
				offsets = make(map[uint32]uint32)
				overlay.activeMemoryDeletes[blockID] = offsets
			}
			offsets[rowID.GetRowOffset()]++
		}
	}
}

func (w *txnWorkspace) removeMutationAccessIndexesLocked(
	overlay *tableOverlay,
	mutation *workspaceMutation,
) {
	if mutation.entry.typ == INSERT {
		delete(overlay.activeUnindexedMemoryInserts, mutation.id)
		delete(overlay.activeUnindexedMemoryInsertRowIDs, mutation.id)
		for row, key := range mutation.pkKeys {
			if !workspaceRowIndexed(mutation.indexedRows, row) {
				continue
			}
			owners := overlay.activeInsertPKMutations[key]
			if !owners.remove(mutation.id) {
				panic("workspace primary-key access index is inconsistent")
			}
			if owners.len() == 0 {
				delete(overlay.activeInsertPKMutations, key)
			} else {
				overlay.activeInsertPKMutations[key] = owners
			}
		}
		for row, rowID := range mutation.rowIDs {
			if !workspaceRowIndexed(mutation.indexedRows, row) {
				continue
			}
			owners := overlay.activeInsertRowIDMutations[rowID]
			if !owners.remove(mutation.id) {
				panic("workspace insert-rowid access index is inconsistent")
			}
			if owners.len() == 0 {
				delete(overlay.activeInsertRowIDMutations, rowID)
			} else {
				overlay.activeInsertRowIDMutations[rowID] = owners
			}
		}
	}
	if mutation.entry.typ == DELETE && mutation.entry.fileName == "" {
		for row := range mutation.rowIDs {
			if !workspaceRowIndexed(mutation.indexedRows, row) {
				continue
			}
			rowID := &mutation.rowIDs[row]
			blockID := rowID.CloneBlockID()
			offsets := overlay.activeMemoryDeletes[blockID]
			offset := rowID.GetRowOffset()
			if offsets == nil || offsets[offset] == 0 {
				panic("workspace row-delete access index is inconsistent")
			}
			if offsets[offset] == 1 {
				delete(offsets, offset)
			} else {
				offsets[offset]--
			}
			if len(offsets) == 0 {
				delete(overlay.activeMemoryDeletes, blockID)
			}
		}
	}
	mutation.indexedRows = nil
}

// retainMutationAccessIndexesLocked snapshots only the access facts that are
// about to leave the current TableOverlay. These facts are sufficient to find
// candidate payload generations for any still-live statement ReadView; row
// visibility and the exact payload generation remain authoritative in
// entriesForMutationIDsLocked and PayloadStore.
func (w *txnWorkspace) retainMutationAccessIndexesLocked(
	overlay *tableOverlay,
	mutation *workspaceMutation,
	endRevision uint64,
) {
	if mutation == nil || mutation.entry.typ != INSERT ||
		mutation.accessIndexRevision >= endRevision {
		return
	}
	if mutation.memoryInsert && !mutation.pkIndexComplete {
		overlay.historicalUnindexedMemoryInserts = append(
			overlay.historicalUnindexedMemoryInserts,
			workspaceAccessIndexInterval{
				mutationID: mutation.id,
				start:      mutation.accessIndexRevision,
				end:        endRevision,
			},
		)
	}
	for row, key := range mutation.pkKeys {
		if !workspaceRowIndexed(mutation.indexedRows, row) {
			continue
		}
		overlay.historicalInsertPKMutations[key] = append(
			overlay.historicalInsertPKMutations[key],
			workspaceAccessIndexInterval{
				mutationID: mutation.id,
				start:      mutation.accessIndexRevision,
				end:        endRevision,
			},
		)
	}
	if mutation.memoryInsert && !mutation.rowIDIndexComplete {
		overlay.historicalUnindexedMemoryInsertRowIDs = append(
			overlay.historicalUnindexedMemoryInsertRowIDs,
			workspaceAccessIndexInterval{
				mutationID: mutation.id,
				start:      mutation.accessIndexRevision,
				end:        endRevision,
			},
		)
	}
	for row, rowID := range mutation.rowIDs {
		if !workspaceRowIndexed(mutation.indexedRows, row) {
			continue
		}
		overlay.historicalInsertRowIDMutations[rowID] = append(
			overlay.historicalInsertRowIDMutations[rowID],
			workspaceAccessIndexInterval{
				mutationID: mutation.id,
				start:      mutation.accessIndexRevision,
				end:        endRevision,
			},
		)
	}
}

func (w *txnWorkspace) refreshMutationAccessIndexesLocked(
	mutation *workspaceMutation,
	indexData workspaceMutationIndexData,
	selections []int64,
	newRevision uint64,
) {
	key := workspaceOverlayKey{
		accountID: mutation.entry.accountId, databaseID: mutation.entry.databaseId,
		tableID: mutation.entry.tableId,
	}
	overlay := w.overlays[key]
	if overlay == nil {
		panic("workspace mutation table overlay is missing during access-index refresh")
	}
	w.retainMutationAccessIndexesLocked(overlay, mutation, newRevision)
	w.removeMutationAccessIndexesLocked(overlay, mutation)
	mutation.pkKeys = slices.Clone(indexData.pkKeys)
	mutation.rowIDs = slices.Clone(indexData.rowIDs)
	mutation.memoryInsert = indexData.memoryInsert
	mutation.pkIndexComplete = indexData.pkIndexComplete
	mutation.rowIDIndexComplete = indexData.rowIDIndexComplete
	mutation.accessIndexRevision = newRevision
	w.addMutationAccessIndexesLocked(overlay, mutation, selections)
}

// queueCompactionLocked records that the physical input used to decide
// compaction eligibility changed. A mutation can already be pending when its
// selections or payload generation changes; advancing the generation lets an
// older in-flight plan finish without consuming that newer work.
func (w *txnWorkspace) queueCompactionLocked(mutation *workspaceMutation) {
	if mutation == nil || !mutation.active || mutation.retiredRevision != 0 ||
		(mutation.entry.typ != INSERT && mutation.entry.typ != DELETE) {
		return
	}
	w.nextCompactionInputGeneration++
	mutation.compactionInputGeneration = w.nextCompactionInputGeneration
	if !w.pendingCompactions.contains(mutation.id) {
		w.pendingCompactions.add(mutation)
	}
}

// retireMutationStateLocked is the only logical active-to-retired transition
// for workspace mutations. The caller holds txnWorkspace.mu and any replacement
// payload publication has already succeeded. The method removes the mutation
// from every global and table-local current-state index at one revision while
// retaining the immutable record for ReadViews that can still resolve it.
func (w *txnWorkspace) retireMutationStateLocked(
	mutation *workspaceMutation,
	revision uint64,
) {
	if mutation == nil || !mutation.active || mutation.retiredRevision != 0 {
		panic("workspace mutation retirement invariant violated")
	}
	mutation.active = false
	mutation.retiredRevision = revision
	key := workspaceOverlayKey{
		accountID:  mutation.entry.accountId,
		databaseID: mutation.entry.databaseId,
		tableID:    mutation.entry.tableId,
	}
	overlay := w.overlays[key]
	if overlay == nil {
		panic("workspace mutation table overlay is missing during retirement")
	}
	w.retiredMutationIDs[mutation.id] = struct{}{}
	w.retainMutationAccessIndexesLocked(overlay, mutation, revision)
	w.removeMutationAccessIndexesLocked(overlay, mutation)
	overlay.retiredMutations = append(overlay.retiredMutations, mutation.id)
	if mutation.entry.pkCheck.enabled {
		overlay.retiredPKCandidateMutations = append(
			overlay.retiredPKCandidateMutations, mutation.id)
	}
	overlay.activeMutations.remove(mutation)
	w.activeMutations.remove(mutation)
	if mutation.entry.pkCheck.enabled {
		w.activePKCandidates.remove(mutation)
	}
	if mutation.entry.typ == INSERT || mutation.entry.typ == DELETE {
		if w.activeCompactionCount == 0 {
			panic("workspace active compaction count underflow")
		}
		w.activeCompactionCount--
		if w.pendingCompactions.contains(mutation.id) {
			w.pendingCompactions.remove(mutation)
		}
	}
	if mutation.blockMeta {
		w.activeBlockMeta.remove(mutation)
	}
	if mutation.entry.typ == DELETE {
		activeDeletes := overlay.activeMemoryDeleteMutations
		if mutation.entry.fileName != "" {
			activeDeletes = overlay.activeObjectDeleteMutations
		}
		if _, exists := activeDeletes[mutation.id]; !exists {
			panic("workspace active delete index is inconsistent")
		}
		delete(activeDeletes, mutation.id)
	}
	for _, objectID := range mutation.objectIDs {
		owners := overlay.activeUncommittedObjects[objectID]
		if _, exists := owners[mutation.id]; !exists {
			panic("workspace active object owner index is inconsistent")
		}
		delete(owners, mutation.id)
		if len(owners) == 0 {
			delete(overlay.activeUncommittedObjects, objectID)
		}
	}
	for _, objectName := range mutation.objectNames {
		refs := w.activeObjectReferences[objectName]
		if _, exists := refs[mutation.id]; !exists {
			panic("workspace active object reference index is inconsistent")
		}
		delete(refs, mutation.id)
		if len(refs) == 0 {
			delete(w.activeObjectReferences, objectName)
		}
	}
}

// retireObjectDeleteStateLocked is the sole active-to-retired transition for
// transaction-local object deletes. The active index serves current
// compaction snapshots without scanning the complete delete history; the
// immutable record remains in objectDeletes while a statement-scoped read
// view or the current statement's rollback ownership can still need it.
// EndStatement expires those views and reclaims the record once rollback
// ownership has advanced to the next statement.
func (w *txnWorkspace) retireObjectDeleteStateLocked(
	objectDelete *workspaceObjectDelete,
	revision uint64,
) {
	if objectDelete == nil || !objectDelete.active ||
		objectDelete.retiredRevision != 0 {
		panic("workspace object delete retirement invariant violated")
	}
	if _, exists := w.activeObjectDeletes[objectDelete.id]; !exists {
		panic("workspace active object delete index is inconsistent")
	}
	objectDelete.active = false
	objectDelete.retiredRevision = revision
	delete(w.activeObjectDeletes, objectDelete.id)
	w.retiredObjectDeletes[objectDelete.id] = struct{}{}
	overlay := w.overlays[objectDelete.key]
	if overlay == nil {
		panic("workspace object delete table overlay is missing during retirement")
	}
	if _, exists := overlay.activePendingObjectDeletes[objectDelete.id]; !exists {
		panic("workspace active object delete overlay index is inconsistent")
	}
	delete(overlay.activePendingObjectDeletes, objectDelete.id)
	overlay.retiredObjectDeletes = append(
		overlay.retiredObjectDeletes, objectDelete.id)
}

// beginSpill captures exact logical mutations and pins their current physical
// generations. It changes no visibility revision: readers continue to see the
// source memory mutations until commitSpill publishes the replacement.
// Mutations already owned by another spill remain visible but are omitted from
// this attempt, so concurrent dump windows cannot materialize one generation
// twice.
func (w *txnWorkspace) beginSpill(
	ids []workspaceMutationID,
) (*workspaceSpillAttempt, error) {
	return w.beginSpillWithMode(ids, false)
}

// beginTerminalSpill freezes the final commit representation. Unlike an
// ordinary spill, its replacement objects may combine mutations from completed
// statements. The target still belongs to the current open attempt so a stale
// publication is rejected if the boundary changes while remote IO is running.
func (w *txnWorkspace) beginTerminalSpill(
	ids []workspaceMutationID,
) (*workspaceSpillAttempt, error) {
	return w.beginSpillWithMode(ids, true)
}

func (w *txnWorkspace) beginSpillWithMode(
	ids []workspaceMutationID,
	terminal bool,
) (*workspaceSpillAttempt, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	attempt := &workspaceSpillAttempt{workspace: w, terminal: terminal}
	if terminal {
		if w.journal.current == nil ||
			w.journal.current.state != statementAttemptOpen {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace terminal spill requires an open final statement")
		}
		w.commitStarted = true
		attempt.owner = w.journal.current.key()
	} else if w.commitStarted {
		return nil, moerr.NewInternalErrorNoCtx(
			"workspace ordinary spill cannot start after commit preparation")
	}
	seen := make(map[workspaceMutationID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			attempt.Close()
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace spill contains duplicate mutation")
		}
		seen[id] = struct{}{}
		mutation := w.mutations[id]
		if mutation == nil || !mutation.active || mutation.retiredRevision != 0 ||
			mutation.entry.fileName != "" {
			attempt.Close()
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace mutation is not spillable")
		}
		payload, acquired, err := w.payloads.tryBeginSpill(mutation.payloadID)
		if err != nil {
			attempt.Close()
			return nil, err
		}
		if !acquired {
			continue
		}
		entry := mutation.entry
		entry.bat = payload.lease.bat
		attempt.sources = append(attempt.sources, workspaceSpillSource{
			mutationID:  id,
			statementID: mutation.statementID,
			attemptID:   mutation.attemptID,
			entry:       entry,
			payload:     payload,
		})
	}
	return attempt, nil
}

// commitSpill atomically replaces every source memory mutation with the
// staged object mutations at one workspace revision. The caller still owns
// object batches when this method returns an error; ownership transfers to
// the workspace only on success.
func (w *txnWorkspace) commitSpill(
	attempt *workspaceSpillAttempt,
	objects []workspaceSpillObject,
) ([]workspaceMutationID, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if attempt == nil || attempt.workspace != w || attempt.closed || attempt.committed {
		return nil, moerr.NewInternalErrorNoCtx("invalid workspace spill attempt")
	}
	if attempt.terminal {
		if w.journal.current == nil ||
			w.journal.current.state != statementAttemptOpen ||
			w.journal.current.key() != attempt.owner {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace terminal spill boundary changed before publication")
		}
	}
	spills := make([]workspaceSpillPayload, len(attempt.sources))
	for idx, source := range attempt.sources {
		mutation := w.mutations[source.mutationID]
		if mutation == nil || !mutation.active || mutation.retiredRevision != 0 ||
			mutation.payloadID != source.payload.payloadID ||
			mutation.statementID != source.statementID ||
			mutation.attemptID != source.attemptID {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace spill source changed before publication")
		}
		spills[idx] = source.payload
	}

	sourcesByID := make(
		map[workspaceMutationID]workspaceSpillSource,
		len(attempt.sources),
	)
	for _, source := range attempt.sources {
		sourcesByID[source.mutationID] = source
	}
	claimedSources := make(
		map[workspaceMutationID]struct{},
		len(attempt.sources),
	)
	objectCommitOrders := make([]workspaceCommitOrder, len(objects))
	objectBatches := make([]*batch.Batch, len(objects))
	sourceUsage := workspaceUsage{}
	for _, source := range attempt.sources {
		sourceUsage.add(usageOfWorkspaceEntry(source.entry, source.payload.lease.bat))
	}
	objectUsage := workspaceUsage{}
	for idx := range objects {
		object := &objects[idx]
		if object.entry.bat == nil {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace spill object has no payload")
		}
		if len(object.sourceMutationIDs) == 0 {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace spill object has no logical sources")
		}
		var earliest workspaceCommitOrder
		for _, sourceID := range object.sourceMutationIDs {
			source, ok := sourcesByID[sourceID]
			if !ok {
				return nil, moerr.NewInternalErrorNoCtx(
					"workspace spill object references a foreign source")
			}
			if _, duplicate := claimedSources[sourceID]; duplicate {
				return nil, moerr.NewInternalErrorNoCtx(
					"workspace spill source belongs to multiple objects")
			}
			claimedSources[sourceID] = struct{}{}
			if (!attempt.terminal &&
				(source.statementID != object.statementID ||
					source.attemptID != object.attemptID)) ||
				!sameWorkspaceSpillGroup(source.entry, object.entry) {
				return nil, moerr.NewInternalErrorNoCtx(
					"workspace spill object does not match its logical source")
			}
			commitOrder := w.mutations[sourceID].commitOrder
			if earliest == nil ||
				compareWorkspaceCommitOrder(commitOrder, earliest) < 0 {
				earliest = commitOrder
			}
		}
		if attempt.terminal {
			object.statementID = attempt.owner.statementID
			object.attemptID = attempt.owner.attemptID
		} else {
			if err := w.journal.validatePhysicalMutationReplacement(
				statementAttemptKey{
					statementID: object.statementID,
					attemptID:   object.attemptID,
				},
				object.sourceMutationIDs,
			); err != nil {
				return nil, err
			}
		}
		objectCommitOrders[idx] = slices.Clone(earliest)
		objectBatches[idx] = object.entry.bat
		objectUsage.add(usageOfWorkspaceEntry(object.entry, object.entry.bat))
	}

	// Every visible source must belong to exactly one staged object. A source
	// with no visible rows is the only legal zero-target transition: staging
	// deliberately emits no empty object for that spill group. Rejecting every
	// other unclaimed source prevents an incomplete staging result from
	// silently retiring data at publication time.
	unclaimedByOwner := make(map[statementAttemptKey][]workspaceMutationID)
	for _, source := range attempt.sources {
		if _, claimed := claimedSources[source.mutationID]; claimed {
			continue
		}
		bat := source.payload.lease.bat
		if bat == nil || len(source.payload.lease.selections) != bat.RowCount() {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace spill left a visible source without an object")
		}
		owner := statementAttemptKey{
			statementID: source.statementID,
			attemptID:   source.attemptID,
		}
		unclaimedByOwner[owner] = append(
			unclaimedByOwner[owner], source.mutationID)
	}
	if !attempt.terminal {
		for owner, sourceIDs := range unclaimedByOwner {
			if err := w.journal.validatePhysicalMutationReplacement(
				owner, sourceIDs,
			); err != nil {
				return nil, err
			}
		}
	}

	newRevision := w.revision + 1
	payloadIDs, err := w.payloads.commitSpills(spills, objectBatches, newRevision)
	if err != nil {
		return nil, err
	}
	w.revision = newRevision
	w.usage.sub(sourceUsage)
	w.usage.add(objectUsage)
	for _, source := range attempt.sources {
		w.retireMutationStateLocked(
			w.mutations[source.mutationID], newRevision)
	}

	ids := make([]workspaceMutationID, len(objects))
	for idx, object := range objects {
		entry := object.entry
		indexData := classifyWorkspaceMutation(entry, entry.bat)
		entry.bat = nil
		ids[idx] = w.publishMutationAtCommitOrderLocked(
			entry,
			payloadIDs[idx],
			indexData,
			newRevision,
			object.statementID,
			object.attemptID,
			objectCommitOrders[idx],
		)
		if attempt.terminal {
			w.journal.replaceTerminalMutationsValidated(
				object.sourceMutationIDs,
				[]workspaceMutationID{ids[idx]},
			)
		} else {
			w.journal.replaceMutationsValidated(
				statementAttemptKey{
					statementID: object.statementID,
					attemptID:   object.attemptID,
				},
				object.sourceMutationIDs,
				[]workspaceMutationID{ids[idx]},
			)
		}
	}
	for owner, sourceIDs := range unclaimedByOwner {
		if attempt.terminal {
			w.journal.replaceTerminalMutationsValidated(sourceIDs, nil)
		} else {
			w.journal.replaceMutationsValidated(owner, sourceIDs, nil)
		}
	}
	attempt.committed = true
	return ids, nil
}

func sameWorkspaceSpillGroup(source, object Entry) bool {
	return source.typ == object.typ &&
		source.accountId == object.accountId &&
		source.databaseId == object.databaseId &&
		source.tableId == object.tableId &&
		source.databaseName == object.databaseName &&
		source.tableName == object.tableName &&
		source.autoIncrEpoch == object.autoIncrEpoch &&
		source.autoIncrEpochKnown == object.autoIncrEpochKnown
}

// replaceSelections publishes a new logical payload generation. Existing
// read views keep resolving the preceding generation; only later revisions
// observe the new selection set.
func (w *txnWorkspace) replaceSelections(
	id workspaceMutationID,
	selections []int64,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	m := w.mutations[id]
	if m == nil || !m.active || m.retiredRevision != 0 {
		return moerr.NewInternalErrorNoCtx("workspace mutation is not active")
	}
	current, err := w.payloads.currentSelections(m.payloadID)
	if err != nil {
		return err
	}
	newRevision := w.revision + 1
	if err := w.payloads.replaceSelections(
		m.payloadID, selections, newRevision,
	); err != nil {
		return err
	}
	w.refreshMutationAccessIndexesLocked(m, workspaceMutationIndexData{
		pkKeys: m.pkKeys, rowIDs: m.rowIDs,
		memoryInsert: m.memoryInsert, pkIndexComplete: m.pkIndexComplete,
		rowIDIndexComplete: m.rowIDIndexComplete,
	}, selections, newRevision)
	w.journal.recordSelectionUndo(id, current)
	w.revision = newRevision
	w.queueCompactionLocked(m)
	return nil
}

// replaceMemory atomically publishes a copy-on-write physical generation.
// The caller retains ownership of oldBat only through existing leases; newBat
// becomes the current physical payload owned by the workspace.
func (w *txnWorkspace) replaceMemory(
	id workspaceMutationID,
	oldBat *batch.Batch,
	newBat *batch.Batch,
	selections []int64,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	m := w.mutations[id]
	if m == nil || m.retiredRevision != 0 {
		return moerr.NewInternalErrorNoCtx("workspace mutation is not active")
	}
	newRevision := w.revision + 1
	indexData := classifyWorkspaceMutation(m.entry, newBat)
	if err := w.payloads.replaceMemory(
		m.payloadID, oldBat, newBat, selections, newRevision,
	); err != nil {
		return err
	}
	w.refreshMutationAccessIndexesLocked(m, indexData, selections, newRevision)
	w.usage.sub(usageOfWorkspaceEntry(m.entry, oldBat))
	w.usage.add(usageOfWorkspaceEntry(m.entry, newBat))
	w.revision = newRevision
	w.queueCompactionLocked(m)
	return nil
}

// rewriteMutations atomically retires logical mutations and publishes their
// replacements. It is deliberately different from replacing a physical
// generation: metadata may change, so an old read view must retain the old
// Entry as well as the old Batch.
//
// A replacement created by the same statement attempt is a physical rewrite
// of that statement's logical write and retains its immutable commit order. A
// later statement rewriting an older mutation is a new logical write owned by
// the current attempt (for example, ALTER of a table created earlier in the
// transaction). It must receive the current protocol order so recreated
// catalog metadata is sent to TN before the rewritten table data.
func (w *txnWorkspace) rewriteMutations(
	rewrites []workspaceMutationRewrite,
) ([]workspaceMutationRewriteResult, error) {
	if len(rewrites) == 0 {
		return nil, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	payloadRewrites := make([]workspacePayloadRewrite, len(rewrites))
	oldUsage := workspaceUsage{}
	newUsage := workspaceUsage{}
	seen := make(map[workspaceMutationID]struct{}, len(rewrites))
	for idx, rewrite := range rewrites {
		if _, ok := seen[rewrite.mutationID]; ok {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rewrite contains duplicate mutation")
		}
		seen[rewrite.mutationID] = struct{}{}
		mutation := w.mutations[rewrite.mutationID]
		if mutation == nil || !mutation.active || mutation.retiredRevision != 0 {
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rewrite mutation is not active")
		}
		if err := w.journal.validateMutationReplacement(
			statementAttemptKey{
				statementID: mutation.statementID,
				attemptID:   mutation.attemptID,
			},
			[]workspaceMutationID{mutation.id},
		); err != nil {
			return nil, err
		}
		state := workspacePayloadMemory
		if rewrite.entry.fileName != "" {
			state = workspacePayloadObject
		}
		payloadRewrites[idx] = workspacePayloadRewrite{
			payloadID:  mutation.payloadID,
			oldBat:     rewrite.oldBat,
			newBat:     rewrite.entry.bat,
			state:      state,
			selections: rewrite.selections,
		}
		oldUsage.add(usageOfWorkspaceEntry(mutation.entry, rewrite.oldBat))
		newUsage.add(usageOfWorkspaceEntry(rewrite.entry, rewrite.entry.bat))
	}
	newRevision := w.revision + 1
	payloadIDs, err := w.payloads.rewriteMany(payloadRewrites, newRevision)
	if err != nil {
		return nil, err
	}
	// Journal publication is part of the successful logical transition. Never
	// record undo before the payload store has accepted the complete rewrite.
	for _, rewrite := range rewrites {
		mutation := w.mutations[rewrite.mutationID]
		selections := rewrite.selections
		if original, changed := w.journal.current.selectionUndo[mutation.id]; changed {
			selections = original
		}
		w.journal.recordRewriteUndo(workspaceMutationRewriteUndo{
			mutationID:  mutation.id,
			payloadID:   mutation.payloadID,
			statementID: mutation.statementID,
			attemptID:   mutation.attemptID,
			commitOrder: mutation.commitOrder,
			entry:       mutation.entry,
			bat:         rewrite.oldBat,
			selections:  selections,
		})
	}
	w.revision = newRevision
	w.usage.sub(oldUsage)
	w.usage.add(newUsage)
	for _, rewrite := range rewrites {
		w.retireMutationStateLocked(
			w.mutations[rewrite.mutationID], newRevision)
	}

	results := make([]workspaceMutationRewriteResult, len(rewrites))
	for idx, rewrite := range rewrites {
		sourceCommitOrder := w.mutations[rewrite.mutationID].commitOrder
		entry := rewrite.entry
		indexData := classifyWorkspaceMutation(entry, entry.bat)
		entry.bat = nil
		source := w.mutations[rewrite.mutationID]
		owner := statementAttemptKey{
			statementID: source.statementID,
			attemptID:   source.attemptID,
		}
		var targetID workspaceMutationID
		if owner == w.journal.current.key() {
			targetID = w.publishMutationAtCommitOrderLocked(
				entry,
				payloadIDs[idx],
				indexData,
				newRevision,
				owner.statementID,
				owner.attemptID,
				sourceCommitOrder,
			)
			w.journal.replaceMutationsValidated(
				owner,
				[]workspaceMutationID{rewrite.mutationID},
				[]workspaceMutationID{targetID},
			)
		} else {
			targetID = w.appendMutationLocked(
				entry,
				payloadIDs[idx],
				indexData,
				newRevision,
				w.journal.current.statementID,
				w.journal.current.attemptID,
			)
		}
		results[idx] = workspaceMutationRewriteResult{
			sourceID: rewrite.mutationID,
			targetID: targetID,
			entry:    rewrite.entry,
		}
	}
	return results, nil
}

// transitionMutations atomically replaces a complete source set with a
// complete target set. It is the only publication API for statement-boundary
// transformations whose cardinality can change (table retirement and CN
// object compaction). On failure neither workspace visibility nor rollback
// journal changes. Targets are owned by the caller on error and by the
// workspace on success.
func (w *txnWorkspace) transitionMutations(
	sources []workspaceMutationTransitionSource,
	targets []workspaceMutationTransitionTarget,
) (workspaceMutationTransitionResult, error) {
	return w.transitionMutationsAtBoundary(
		sources,
		targets,
		workspaceObjectDeleteSnapshot{},
		workspaceBoundaryPublication{},
	)
}

func (w *txnWorkspace) transitionMutationsAndConsumeObjectDeletes(
	sources []workspaceMutationTransitionSource,
	targets []workspaceMutationTransitionTarget,
	objectDeletes workspaceObjectDeleteSnapshot,
) (workspaceMutationTransitionResult, error) {
	return w.transitionMutationsAtBoundary(
		sources,
		targets,
		objectDeletes,
		workspaceBoundaryPublication{},
	)
}

// advanceStatementWithTransition publishes a statement-boundary rewrite into
// the new statement attempt. Payload validation and publication complete
// before the journal advances, while the workspace lock prevents readers from
// observing that internal ordering. Once payload publication succeeds, the
// remaining journal and mutation updates cannot fail.
func (w *txnWorkspace) advanceStatementWithTransition(
	sources []workspaceMutationTransitionSource,
	targets []workspaceMutationTransitionTarget,
) (workspaceMutationTransitionResult, error) {
	return w.transitionMutationsAtBoundary(
		sources,
		targets,
		workspaceObjectDeleteSnapshot{},
		workspaceBoundaryPublication{advanceStatement: true},
	)
}

func (w *txnWorkspace) publishRCBoundaryWithTransition(
	sources []workspaceMutationTransitionSource,
	targets []workspaceMutationTransitionTarget,
	advanceStatement bool,
	boundary rcBoundaryPublication,
) (workspaceMutationTransitionResult, error) {
	return w.transitionMutationsAtBoundary(
		sources,
		targets,
		workspaceObjectDeleteSnapshot{},
		workspaceBoundaryPublication{
			advanceStatement: advanceStatement,
			rc:               &boundary,
		},
	)
}

func (w *txnWorkspace) transitionMutationsAtBoundary(
	sources []workspaceMutationTransitionSource,
	targets []workspaceMutationTransitionTarget,
	objectDeletes workspaceObjectDeleteSnapshot,
	boundary workspaceBoundaryPublication,
) (workspaceMutationTransitionResult, error) {
	if len(sources) == 0 && len(targets) == 0 && len(objectDeletes.ids) == 0 {
		w.mu.Lock()
		defer w.mu.Unlock()
		if err := w.publishBoundaryLocked(boundary); err != nil {
			return workspaceMutationTransitionResult{}, err
		}
		return workspaceMutationTransitionResult{}, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.validateBoundaryPublicationLocked(boundary); err != nil {
		return workspaceMutationTransitionResult{}, err
	}

	seenObjectDeletes := make(map[workspaceObjectDeleteID]struct{}, len(objectDeletes.ids))
	for _, deleteID := range objectDeletes.ids {
		if _, duplicate := seenObjectDeletes[deleteID]; duplicate {
			return workspaceMutationTransitionResult{}, moerr.NewInternalErrorNoCtx(
				"workspace transition contains duplicate object delete")
		}
		seenObjectDeletes[deleteID] = struct{}{}
		objectDelete := w.objectDeletes[deleteID]
		if objectDelete == nil || !objectDelete.active || objectDelete.retiredRevision != 0 {
			return workspaceMutationTransitionResult{}, moerr.NewInternalErrorNoCtx(
				"workspace object delete changed before publication")
		}
	}

	payloadSources := make([]workspacePayloadTransitionSource, len(sources))
	payloadTargets := make([]workspacePayloadTransitionTarget, len(targets))
	targetCommitOrders := make([]workspaceCommitOrder, len(targets))
	oldUsage := workspaceUsage{}
	newUsage := workspaceUsage{}
	seen := make(map[workspaceMutationID]struct{}, len(sources))
	sourceCommitOrders := make(
		map[workspaceMutationID]workspaceCommitOrder,
		len(sources),
	)
	undos := make([]workspaceMutationRewriteUndo, len(sources))
	for idx, source := range sources {
		if _, duplicate := seen[source.mutationID]; duplicate {
			return workspaceMutationTransitionResult{}, moerr.NewInternalErrorNoCtx(
				"workspace transition contains duplicate source mutation")
		}
		seen[source.mutationID] = struct{}{}
		mutation := w.mutations[source.mutationID]
		if mutation == nil || !mutation.active || mutation.retiredRevision != 0 {
			return workspaceMutationTransitionResult{}, moerr.NewInternalErrorNoCtx(
				"workspace transition source is not active")
		}
		if err := w.journal.validateBoundaryMutationReplacement(
			statementAttemptKey{
				statementID: mutation.statementID,
				attemptID:   mutation.attemptID,
			},
			[]workspaceMutationID{mutation.id},
			boundary.advanceStatement,
		); err != nil {
			return workspaceMutationTransitionResult{}, err
		}
		payloadSources[idx] = workspacePayloadTransitionSource{
			payloadID: mutation.payloadID,
			oldBat:    source.oldBat,
		}
		sourceCommitOrders[source.mutationID] = mutation.commitOrder
		oldUsage.add(usageOfWorkspaceEntry(mutation.entry, source.oldBat))
		selections := source.selections
		if !boundary.advanceStatement {
			if original, changed := w.journal.current.selectionUndo[mutation.id]; changed {
				selections = original
			}
		}
		undos[idx] = workspaceMutationRewriteUndo{
			mutationID:  mutation.id,
			payloadID:   mutation.payloadID,
			statementID: mutation.statementID,
			attemptID:   mutation.attemptID,
			commitOrder: mutation.commitOrder,
			entry:       mutation.entry,
			bat:         source.oldBat,
			selections:  selections,
		}
	}
	replacementCounts := make(map[workspaceMutationID]uint64, len(sources))
	for idx := range targets {
		replacementOf := targets[idx].replacementOf
		if replacementOf == 0 {
			continue
		}
		if _, ok := sourceCommitOrders[replacementOf]; !ok {
			return workspaceMutationTransitionResult{}, moerr.NewInternalErrorNoCtx(
				"workspace transition target references a non-source mutation")
		}
		replacementCounts[replacementOf]++
	}
	replacementOrdinals := make(map[workspaceMutationID]uint64, len(replacementCounts))
	for idx := range targets {
		entry := targets[idx].entry
		if entry.bat == nil {
			return workspaceMutationTransitionResult{}, moerr.NewInternalErrorNoCtx(
				"workspace transition target has no payload")
		}
		state := workspacePayloadMemory
		if entry.fileName != "" {
			state = workspacePayloadObject
		}
		payloadTargets[idx] = workspacePayloadTransitionTarget{
			bat:        entry.bat,
			state:      state,
			selections: targets[idx].selections,
		}
		if replacementOf := targets[idx].replacementOf; replacementOf != 0 {
			if replacementCounts[replacementOf] == 1 {
				targetCommitOrders[idx] = sourceCommitOrders[replacementOf]
			} else {
				replacementOrdinals[replacementOf]++
				targetCommitOrders[idx] = childWorkspaceCommitOrder(
					sourceCommitOrders[replacementOf],
					replacementOrdinals[replacementOf],
				)
			}
		}
		newUsage.add(usageOfWorkspaceEntry(entry, entry.bat))
	}

	newRevision := w.revision + 1
	var payloadIDs []workspacePayloadID
	if len(payloadSources) != 0 || len(payloadTargets) != 0 {
		var err error
		payloadIDs, err = w.payloads.transitionMany(
			payloadSources, payloadTargets, newRevision)
		if err != nil {
			return workspaceMutationTransitionResult{}, err
		}
	}
	w.publishBoundaryValidatedLocked(boundary)
	for idx := range undos {
		w.journal.recordRewriteUndo(undos[idx])
	}
	w.revision = newRevision
	w.usage.sub(oldUsage)
	w.usage.add(newUsage)
	for _, source := range sources {
		w.retireMutationStateLocked(
			w.mutations[source.mutationID], newRevision)
	}
	for _, deleteID := range objectDeletes.ids {
		w.retireObjectDeleteStateLocked(w.objectDeletes[deleteID], newRevision)
	}

	result := workspaceMutationTransitionResult{
		targetIDs: make([]workspaceMutationID, len(targets)),
	}
	for idx := range targets {
		entry := targets[idx].entry
		indexData := classifyWorkspaceMutation(entry, entry.bat)
		entry.bat = nil
		if targets[idx].replacementOf == 0 {
			result.targetIDs[idx] = w.appendMutationLocked(
				entry,
				payloadIDs[idx],
				indexData,
				newRevision,
				w.journal.current.statementID,
				w.journal.current.attemptID,
			)
		} else {
			result.targetIDs[idx] = w.appendMutationAtCommitOrderLocked(
				entry,
				payloadIDs[idx],
				indexData,
				newRevision,
				w.journal.current.statementID,
				w.journal.current.attemptID,
				targetCommitOrders[idx],
			)
		}
	}
	for _, source := range sources {
		mutation := w.mutations[source.mutationID]
		w.journal.replaceMutationsValidated(
			statementAttemptKey{
				statementID: mutation.statementID,
				attemptID:   mutation.attemptID,
			},
			[]workspaceMutationID{source.mutationID},
			result.targetIDs,
		)
	}
	return result, nil
}

// publishBoundaryLocked validates the complete boundary before applying its
// infallible state changes, preventing a statement and its RC cursor from
// advancing independently.
func (w *txnWorkspace) publishBoundaryLocked(boundary workspaceBoundaryPublication) error {
	if err := w.validateBoundaryPublicationLocked(boundary); err != nil {
		return err
	}
	w.publishBoundaryValidatedLocked(boundary)
	return nil
}

func (w *txnWorkspace) validateBoundaryPublicationLocked(
	boundary workspaceBoundaryPublication,
) error {
	if boundary.advanceStatement {
		return w.journal.validateAdvance()
	}
	return nil
}

func (w *txnWorkspace) publishBoundaryValidatedLocked(
	boundary workspaceBoundaryPublication,
) {
	if boundary.advanceStatement {
		// Once an attempt completes, its logical rollback ownership is released
		// by StatementJournal. Requeue its physical representatives so later
		// boundaries may compact them with other completed attempts. The current
		// attempt is still kept separate while it is open, so statement retry and
		// RollbackLastStatement never depend on a cross-attempt payload.
		for mutationID := range w.journal.current.mutations {
			w.queueCompactionLocked(w.mutations[mutationID])
		}
		w.journal.advanceValidated()
	}
	if boundary.rc != nil {
		w.journal.publishRCBoundary(*boundary.rc)
	}
}

// compactMemoryMany atomically publishes a complete compaction plan. Mutation
// identity, table overlay membership and commit order remain stable for every
// destination; source mutations become invisible at the same revision.
func (w *txnWorkspace) compactMemoryMany(compactions []workspaceMutationCompaction) error {
	if len(compactions) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	payloadMerges := make([]workspaceMemoryCompaction, len(compactions))
	oldUsage := workspaceUsage{}
	newUsage := workspaceUsage{}
	seen := make(map[workspaceMutationID]struct{})
	for idx, merge := range compactions {
		if len(merge.srcMutationIDs) != len(merge.srcOldBats) {
			return moerr.NewInternalErrorNoCtx(
				"workspace merge sources are invalid")
		}
		if _, ok := seen[merge.dstMutationID]; ok {
			return moerr.NewInternalErrorNoCtx(
				"workspace merge contains duplicate mutation")
		}
		seen[merge.dstMutationID] = struct{}{}
		dst := w.mutations[merge.dstMutationID]
		if dst == nil || !dst.active || dst.retiredRevision != 0 {
			return moerr.NewInternalErrorNoCtx(
				"workspace merge destination is not active")
		}
		owner := statementAttemptKey{
			statementID: dst.statementID,
			attemptID:   dst.attemptID,
		}
		compactionIDs := make(
			[]workspaceMutationID, 1, len(merge.srcMutationIDs)+1)
		compactionIDs[0] = dst.id
		oldUsage.add(usageOfWorkspaceEntry(dst.entry, merge.dstOldBat))
		newUsage.add(usageOfWorkspaceEntry(dst.entry, merge.dstNewBat))

		srcPayloadIDs := make([]workspacePayloadID, len(merge.srcMutationIDs))
		for srcIdx, srcID := range merge.srcMutationIDs {
			if _, ok := seen[srcID]; ok {
				return moerr.NewInternalErrorNoCtx(
					"workspace merge contains duplicate mutation")
			}
			seen[srcID] = struct{}{}
			src := w.mutations[srcID]
			if src == nil || !src.active || src.retiredRevision != 0 {
				return moerr.NewInternalErrorNoCtx(
					"workspace merge source is not active")
			}
			compactionIDs = append(compactionIDs, src.id)
			srcPayloadIDs[srcIdx] = src.payloadID
			oldUsage.add(usageOfWorkspaceEntry(src.entry, merge.srcOldBats[srcIdx]))
		}
		// The journal distinguishes only mutations owned by the open attempt
		// from completed mutations. Completed attempts no longer support
		// statement rollback, so their physical payloads may be coalesced while
		// retaining the destination's stable commit position. Mixing an open
		// attempt with completed data remains forbidden by this validation.
		if err := w.journal.validatePhysicalMutationReplacement(
			owner, compactionIDs,
		); err != nil {
			return err
		}
		payloadMerges[idx] = workspaceMemoryCompaction{
			dstPayloadID:  dst.payloadID,
			dstOldBat:     merge.dstOldBat,
			dstNewBat:     merge.dstNewBat,
			srcPayloadIDs: srcPayloadIDs,
			srcOldBats:    merge.srcOldBats,
		}
	}

	newRevision := w.revision + 1
	if err := w.payloads.compactMemoryMany(payloadMerges, newRevision); err != nil {
		return err
	}
	w.usage.sub(oldUsage)
	w.usage.add(newUsage)
	for _, merge := range compactions {
		dst := w.mutations[merge.dstMutationID]
		indexData := classifyWorkspaceMutation(dst.entry, merge.dstNewBat)
		w.refreshMutationAccessIndexesLocked(
			dst, indexData, nil, newRevision)
		for _, srcID := range merge.srcMutationIDs {
			w.retireMutationStateLocked(w.mutations[srcID], newRevision)
		}
		w.journal.replaceMutationsValidated(
			statementAttemptKey{
				statementID: dst.statementID,
				attemptID:   dst.attemptID,
			},
			merge.srcMutationIDs,
			[]workspaceMutationID{merge.dstMutationID},
		)
	}
	w.revision = newRevision
	return nil
}

// retireMutations removes a complete logical set at one revision. It is used
// for table-drop cleanup and other workspace-wide operations where partial
// retirement would violate statement visibility.
func (w *txnWorkspace) retireMutations(ids []workspaceMutationID) error {
	if len(ids) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	payloadIDs := make([]workspacePayloadID, len(ids))
	retiredUsage := workspaceUsage{}
	seen := make(map[workspaceMutationID]struct{}, len(ids))
	for idx, id := range ids {
		if _, ok := seen[id]; ok {
			return moerr.NewInternalErrorNoCtx(
				"workspace retirement contains duplicate mutation")
		}
		seen[id] = struct{}{}
		mutation := w.mutations[id]
		if mutation == nil || !mutation.active || mutation.retiredRevision != 0 {
			return moerr.NewInternalErrorNoCtx(
				"workspace retirement mutation is not active")
		}
		payloadIDs[idx] = mutation.payloadID
		bat, err := w.payloads.currentBatch(mutation.payloadID)
		if err != nil {
			return err
		}
		retiredUsage.add(usageOfWorkspaceEntry(mutation.entry, bat))
	}

	newRevision := w.revision + 1
	if err := w.payloads.retireMany(payloadIDs, newRevision); err != nil {
		return err
	}
	w.usage.sub(retiredUsage)
	for _, id := range ids {
		w.retireMutationStateLocked(w.mutations[id], newRevision)
	}
	w.revision = newRevision
	return nil
}

func (w *txnWorkspace) retireMutation(id workspaceMutationID) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	m := w.mutations[id]
	if m == nil {
		return moerr.NewInternalErrorNoCtx("workspace mutation does not exist")
	}
	if m.retiredRevision != 0 {
		return nil
	}
	bat, err := w.payloads.currentBatch(m.payloadID)
	if err != nil {
		return err
	}
	newRevision := w.revision + 1
	if err = w.payloads.retire(m.payloadID, newRevision); err != nil {
		return err
	}
	w.usage.sub(usageOfWorkspaceEntry(m.entry, bat))
	w.revision = newRevision
	w.retireMutationStateLocked(m, newRevision)
	return nil
}

func (w *txnWorkspace) mutationSelections(id workspaceMutationID) ([]int64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	m := w.mutations[id]
	if m == nil || !m.active || m.retiredRevision != 0 {
		return nil, moerr.NewInternalErrorNoCtx("workspace mutation is not active")
	}
	return w.payloads.currentSelections(m.payloadID)
}

func (w *txnWorkspace) addMutationSelections(
	id workspaceMutationID,
	selections []int64,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	m := w.mutations[id]
	if m == nil || !m.active || m.retiredRevision != 0 {
		return moerr.NewInternalErrorNoCtx("workspace mutation is not active")
	}
	current, err := w.payloads.currentSelections(m.payloadID)
	if err != nil {
		return err
	}
	next := append(slices.Clone(current), selections...)
	newRevision := w.revision + 1
	if err := w.payloads.replaceSelections(
		m.payloadID, next, newRevision,
	); err != nil {
		return err
	}
	w.refreshMutationAccessIndexesLocked(m, workspaceMutationIndexData{
		pkKeys:             m.pkKeys,
		rowIDs:             m.rowIDs,
		memoryInsert:       m.memoryInsert,
		pkIndexComplete:    m.pkIndexComplete,
		rowIDIndexComplete: m.rowIDIndexComplete,
	}, next, newRevision)
	w.journal.recordSelectionUndo(id, current)
	w.revision = newRevision
	w.queueCompactionLocked(m)
	return nil
}

func (w *txnWorkspace) selectAllMutationRows(id workspaceMutationID, rowCount int) error {
	selections := make([]int64, rowCount)
	for idx := range rowCount {
		selections[idx] = int64(idx)
	}
	return w.replaceSelections(id, selections)
}

func (w *txnWorkspace) validateReadViewLocked(view client.WorkspaceReadView) error {
	if view.IsZero() {
		return nil
	}
	if view.WorkspaceID() != w.id {
		return moerr.NewInternalErrorNoCtx("workspace read view belongs to another transaction")
	}
	if view.Revision() < w.minimumReadableRevision {
		return moerr.NewInternalErrorNoCtx("workspace read view has expired")
	}
	if view.MaxMutationID() > uint64(w.nextMutationID) {
		return moerr.NewInternalErrorNoCtx("workspace read view is ahead of the transaction")
	}
	if view.Revision() > w.revision {
		return moerr.NewInternalErrorNoCtx("workspace read view revision is ahead of the transaction")
	}
	return nil
}

func (w *txnWorkspace) isCurrentReadViewLocked(
	view client.WorkspaceReadView,
) bool {
	return !view.IsZero() && view.Revision() == w.revision &&
		view.MaxMutationID() == uint64(w.nextMutationID)
}

func (w *txnWorkspace) validateWriteMarkLocked(mark client.WorkspaceWriteMark) error {
	if mark.WorkspaceID() != w.id {
		return moerr.NewInternalErrorNoCtx("workspace write mark belongs to another transaction")
	}
	if mark.StatementID() != w.journal.current.statementID ||
		mark.AttemptID() != w.journal.current.attemptID {
		return moerr.NewInternalErrorNoCtx("workspace write mark belongs to another statement attempt")
	}
	if mark.MaxMutationID() > uint64(w.nextMutationID) {
		return moerr.NewInternalErrorNoCtx("workspace write mark is ahead of the transaction")
	}
	return nil
}

// tableEntries returns generation-pinned payloads for one exact table.
func (w *txnWorkspace) tableEntries(
	view client.WorkspaceReadView,
	accountID uint32,
	databaseID uint64,
	tableID uint64,
) (*workspaceEntrySet, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if err := w.validateReadViewLocked(view); err != nil {
		return nil, err
	}
	if view.IsZero() {
		return &workspaceEntrySet{}, nil
	}
	overlay := w.overlays[workspaceOverlayKey{
		accountID:  accountID,
		databaseID: databaseID,
		tableID:    tableID,
	}]
	if overlay == nil {
		return &workspaceEntrySet{}, nil
	}
	var ids []workspaceMutationID
	if view.Revision() == w.revision &&
		view.MaxMutationID() == uint64(w.nextMutationID) {
		ids = overlay.activeMutations.ids()
		return w.entriesForMutationIDsLocked(view, ids)
	} else {
		ids = make([]workspaceMutationID, 0,
			overlay.activeMutations.len()+len(overlay.retiredMutations))
		ids = append(ids, overlay.activeMutations.ids()...)
		ids = append(ids, overlay.retiredMutations...)
	}
	slices.SortStableFunc(ids, func(aID, bID workspaceMutationID) int {
		a := w.mutations[aID]
		b := w.mutations[bID]
		if a == nil || b == nil {
			return 0
		}
		return compareWorkspaceCommitOrder(a.commitOrder, b.commitOrder)
	})
	return w.entriesForMutationIDsLocked(view, ids)
}

func workspaceMutationVisibleAtReadView(
	mutation *workspaceMutation,
	view client.WorkspaceReadView,
) bool {
	return mutation != nil && uint64(mutation.id) <= view.MaxMutationID() &&
		mutation.createdRevision <= view.Revision() &&
		(mutation.retiredRevision == 0 || view.Revision() < mutation.retiredRevision)
}

func workspaceCurrentAccessFactsVisibleAtReadView(
	mutation *workspaceMutation,
	view client.WorkspaceReadView,
) bool {
	return workspaceMutationVisibleAtReadView(mutation, view) &&
		mutation.accessIndexRevision <= view.Revision()
}

func appendVisibleWorkspaceOwnerIDs(
	ids []workspaceMutationID,
	owners workspaceMutationOwners,
	mutations map[workspaceMutationID]*workspaceMutation,
	view client.WorkspaceReadView,
) []workspaceMutationID {
	owners.forEach(func(id workspaceMutationID, _ uint32) {
		if workspaceCurrentAccessFactsVisibleAtReadView(mutations[id], view) &&
			!slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	})
	return ids
}

func appendVisibleWorkspaceIntervalIDs(
	ids []workspaceMutationID,
	intervals []workspaceAccessIndexInterval,
	view client.WorkspaceReadView,
) []workspaceMutationID {
	for _, interval := range intervals {
		if uint64(interval.mutationID) <= view.MaxMutationID() &&
			interval.contains(view.Revision()) &&
			!slices.Contains(ids, interval.mutationID) {
			ids = append(ids, interval.mutationID)
		}
	}
	return ids
}

// tablePointInsertEntries resolves a finite set of exact encoded primary keys through the
// TableOverlay without opening unrelated payloads. Current access facts are
// combined with the statement-scoped facts removed after the ReadView was
// published. entriesForMutationIDsLocked then applies logical visibility and
// pins the payload generation belonging to that exact revision.
func (w *txnWorkspace) tablePointInsertEntries(
	view client.WorkspaceReadView,
	accountID uint32,
	databaseID uint64,
	tableID uint64,
	encodedKeys ...[]byte,
) (entries *workspaceEntrySet, indexed bool, err error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if err = w.validateReadViewLocked(view); err != nil {
		return nil, false, err
	}
	if view.IsZero() {
		return &workspaceEntrySet{}, true, nil
	}
	overlay := w.overlays[workspaceOverlayKey{
		accountID: accountID, databaseID: databaseID, tableID: tableID,
	}]
	if overlay == nil {
		return &workspaceEntrySet{}, true, nil
	}
	for id := range overlay.activeUnindexedMemoryInserts {
		if workspaceCurrentAccessFactsVisibleAtReadView(w.mutations[id], view) {
			return nil, false, nil
		}
	}
	for _, interval := range overlay.historicalUnindexedMemoryInserts {
		if uint64(interval.mutationID) <= view.MaxMutationID() &&
			interval.contains(view.Revision()) {
			return nil, false, nil
		}
	}
	ids := make([]workspaceMutationID, 0, len(encodedKeys))
	for _, encodedKey := range encodedKeys {
		key := string(encodedKey)
		ids = appendVisibleWorkspaceOwnerIDs(
			ids, overlay.activeInsertPKMutations[key], w.mutations, view)
		ids = appendVisibleWorkspaceIntervalIDs(
			ids, overlay.historicalInsertPKMutations[key], view)
	}
	slices.SortStableFunc(ids, func(aID, bID workspaceMutationID) int {
		return compareWorkspaceCommitOrder(
			w.mutations[aID].commitOrder, w.mutations[bID].commitOrder)
	})
	entries, err = w.entriesForMutationIDsLocked(view, ids)
	return entries, true, err
}

// tablePointInsertEntriesByRowIDs resolves transaction-local rows through the
// current TableOverlay without opening unrelated INSERT payloads. The returned
// entries are only the mutations that own at least one requested RowID.
// Incomplete index facts that overlap the requested ReadView return
// indexed=false so the caller can preserve the complete visibility path.
func (w *txnWorkspace) tablePointInsertEntriesByRowIDs(
	view client.WorkspaceReadView,
	accountID uint32,
	databaseID uint64,
	tableID uint64,
	rowIDs []objectio.Rowid,
) (entries *workspaceEntrySet, indexed bool, err error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if err = w.validateReadViewLocked(view); err != nil {
		return nil, false, err
	}
	if view.IsZero() {
		return &workspaceEntrySet{}, true, nil
	}
	overlay := w.overlays[workspaceOverlayKey{
		accountID: accountID, databaseID: databaseID, tableID: tableID,
	}]
	if overlay == nil {
		return &workspaceEntrySet{}, true, nil
	}
	for id := range overlay.activeUnindexedMemoryInsertRowIDs {
		if workspaceCurrentAccessFactsVisibleAtReadView(w.mutations[id], view) {
			return nil, false, nil
		}
	}
	for _, interval := range overlay.historicalUnindexedMemoryInsertRowIDs {
		if uint64(interval.mutationID) <= view.MaxMutationID() &&
			interval.contains(view.Revision()) {
			return nil, false, nil
		}
	}
	ownerSet := make(map[workspaceMutationID]struct{})
	for _, rowID := range rowIDs {
		overlay.activeInsertRowIDMutations[rowID].forEach(
			func(id workspaceMutationID, _ uint32) {
				if workspaceCurrentAccessFactsVisibleAtReadView(
					w.mutations[id], view,
				) {
					ownerSet[id] = struct{}{}
				}
			})
		for _, interval := range overlay.historicalInsertRowIDMutations[rowID] {
			if uint64(interval.mutationID) <= view.MaxMutationID() &&
				interval.contains(view.Revision()) {
				ownerSet[interval.mutationID] = struct{}{}
			}
		}
	}
	ids := make([]workspaceMutationID, 0, len(ownerSet))
	for id := range ownerSet {
		ids = append(ids, id)
	}
	slices.SortStableFunc(ids, func(aID, bID workspaceMutationID) int {
		return compareWorkspaceCommitOrder(
			w.mutations[aID].commitOrder, w.mutations[bID].commitOrder)
	})
	entries, err = w.entriesForMutationIDsLocked(view, ids)
	return entries, true, err
}

// tableMemoryDeleteOffsets probes only the requested row offsets in one
// committed block. Passing nil candidates returns all current offsets for
// GetTombstones. Like the PK index, this current-state index is not used for a
// historical ReadView.
func (w *txnWorkspace) tableMemoryDeleteOffsets(
	view client.WorkspaceReadView,
	accountID uint32,
	databaseID uint64,
	tableID uint64,
	blockID objectio.Blockid,
	candidates []int64,
) (deleted []int64, indexed bool, err error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if err = w.validateReadViewLocked(view); err != nil {
		return nil, false, err
	}
	if !w.isCurrentReadViewLocked(view) {
		return nil, false, nil
	}
	overlay := w.overlays[workspaceOverlayKey{
		accountID: accountID, databaseID: databaseID, tableID: tableID,
	}]
	if overlay == nil {
		return nil, true, nil
	}
	offsets := overlay.activeMemoryDeletes[blockID]
	if candidates == nil {
		deleted = make([]int64, 0, len(offsets))
		for offset := range offsets {
			deleted = append(deleted, int64(offset))
		}
		slices.Sort(deleted)
		return deleted, true, nil
	}
	deleted = make([]int64, 0, min(len(candidates), len(offsets)))
	for _, offset := range candidates {
		if offset >= 0 && offsets[uint32(offset)] != 0 {
			deleted = append(deleted, offset)
		}
	}
	return deleted, true, nil
}

func (w *txnWorkspace) tableTombstoneObjects(
	view client.WorkspaceReadView,
	accountID uint32,
	databaseID uint64,
	tableID uint64,
) (objects []objectio.ObjectStats, indexed bool, err error) {
	objects, indexed, err = w.tableObjectStats(
		view, accountID, databaseID, tableID, DELETE)
	if err != nil || !indexed || len(objects) < 2 {
		return objects, indexed, err
	}
	seen := make(map[objectio.ObjectStats]struct{})
	unique := objects[:0]
	for idx := range objects {
		if _, exists := seen[objects[idx]]; exists {
			continue
		}
		seen[objects[idx]] = struct{}{}
		unique = append(unique, objects[idx])
	}
	return unique, true, nil
}

// tableObjectStats returns the persisted data or tombstone objects visible in
// the current logical table overlay. Persisted INSERT ownership is indexed by
// object ID, while persisted DELETE ownership is indexed by mutation ID. Both
// indexes contain stable mutation identities and are updated atomically with
// mutation publication, replacement, retirement, and statement rollback.
//
// Historical statement views deliberately report indexed=false: their visible
// owners may include mutations retired from the current overlay, so callers
// must use the immutable statement journal rather than infer history from a
// current-state index.
func (w *txnWorkspace) tableObjectStats(
	view client.WorkspaceReadView,
	accountID uint32,
	databaseID uint64,
	tableID uint64,
	typ int,
) (objects []objectio.ObjectStats, indexed bool, err error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if err = w.validateReadViewLocked(view); err != nil {
		return nil, false, err
	}
	if !w.isCurrentReadViewLocked(view) {
		return nil, false, nil
	}

	overlay := w.overlays[workspaceOverlayKey{
		accountID: accountID, databaseID: databaseID, tableID: tableID,
	}]
	if overlay == nil {
		return nil, true, nil
	}

	mutationSet := make(map[workspaceMutationID]struct{})
	switch typ {
	case INSERT:
		for _, owners := range overlay.activeUncommittedObjects {
			for mutationID := range owners {
				mutationSet[mutationID] = struct{}{}
			}
		}
	case DELETE:
		for mutationID := range overlay.activeObjectDeleteMutations {
			mutationSet[mutationID] = struct{}{}
		}
	default:
		return nil, false, moerr.NewInternalErrorNoCtxf(
			"unsupported workspace object mutation type %d", typ)
	}
	if len(mutationSet) == 0 {
		return nil, true, nil
	}

	ids := make([]workspaceMutationID, 0, len(mutationSet))
	for mutationID := range mutationSet {
		ids = append(ids, mutationID)
	}
	slices.SortStableFunc(ids, func(aID, bID workspaceMutationID) int {
		a := w.mutations[aID]
		b := w.mutations[bID]
		if a == nil || b == nil {
			return 0
		}
		return compareWorkspaceCommitOrder(a.commitOrder, b.commitOrder)
	})

	entries, err := w.entriesForMutationIDsLocked(view, ids)
	if err != nil {
		return nil, false, err
	}
	defer entries.Close()
	for idx := range entries.entries {
		entry := &entries.entries[idx]
		if entry.typ != typ {
			return nil, false, moerr.NewInternalErrorNoCtxf(
				"workspace object index contains mutation type %d, expected %d",
				entry.typ, typ)
		}
		entry.forEachVisibleObjectStats(func(stats objectio.ObjectStats) {
			objects = append(objects, stats)
		})
	}
	return objects, true, nil
}

// pkCandidateEntries returns only in-memory user-table INSERT and DELETE
// mutations published with an exact duplicate-check descriptor.
// Candidate identity is maintained by each TableOverlay when a mutation is
// published; duplicate checking therefore does not scan or pin unrelated
// tables, catalog writes, object mutations, or other mutation kinds.
func (w *txnWorkspace) pkCandidateEntries(
	view client.WorkspaceReadView,
) (*workspaceEntrySet, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if err := w.validateReadViewLocked(view); err != nil {
		return nil, err
	}
	if view.IsZero() {
		return &workspaceEntrySet{}, nil
	}
	if view.Revision() == w.revision &&
		view.MaxMutationID() == uint64(w.nextMutationID) {
		return w.entriesForMutationIDsLocked(view, w.activePKCandidates.ids())
	}
	return w.overlayIndexEntriesLocked(
		view,
		w.activePKCandidates,
		func(overlay *tableOverlay) []workspaceMutationID {
			return overlay.retiredPKCandidateMutations
		},
	)
}

// beginCompactionPlan atomically pins only INSERT and DELETE mutations that
// have not been evaluated by a previous statement boundary. The input
// generation watermark makes planning proportional to newly published
// physical inputs rather than transaction history. The minimum-input gate is
// evaluated before pending IDs are enumerated or payload generations are
// pinned: statement completion is a hot path, and a deferred compaction must
// remain O(1) until enough physical input exists to justify a plan.
//
// A plan is completed only after every physical replacement succeeds.  If
// planning or publication fails, leaving the IDs pending makes the next
// attempt retry the exact same logical work instead of silently losing it.
func (w *txnWorkspace) beginCompactionPlan(
	minimumNewInputs uint64,
) (*workspaceCompactionPlan, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	newInputs := w.nextCompactionInputGeneration -
		w.completedCompactionGeneration
	if newInputs < minimumNewInputs {
		return nil, nil
	}
	view := client.NewWorkspaceReadView(
		w.id,
		w.revision,
		uint64(w.nextMutationID),
	)
	ids := w.pendingCompactions.ids()
	inputGenerations := make([]uint64, len(ids))
	for idx, id := range ids {
		inputGenerations[idx] = w.pendingCompactions.byID[id].compactionInputGeneration
	}
	entries, err := w.entriesForMutationIDsLocked(view, ids)
	if err != nil {
		return nil, err
	}
	return &workspaceCompactionPlan{
		entries:          entries,
		mutationIDs:      ids,
		inputGenerations: inputGenerations,
		inputGeneration:  w.nextCompactionInputGeneration,
		newInputs:        newInputs,
		currentAttempt:   w.journal.current.key(),
	}, nil
}

// completeCompactionPlan marks only the mutations captured by plan as
// evaluated.  Mutations appended concurrently after the snapshot remain
// pending, and mutations retired by the compaction have already removed
// themselves from the index.
func (w *txnWorkspace) completeCompactionPlan(
	plan *workspaceCompactionPlan,
	evaluated map[workspaceMutationID]struct{},
) {
	if plan == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	// Completing a no-op plan is still meaningful: it records that the planner
	// inspected this input generation. Without this watermark, incompatible or
	// one-entry tails would be re-opened on every subsequent statement.
	if plan.inputGeneration > w.completedCompactionGeneration {
		w.completedCompactionGeneration = plan.inputGeneration
	}
	for idx, id := range plan.mutationIDs {
		if _, ok := evaluated[id]; !ok {
			continue
		}
		mutation := w.mutations[id]
		if mutation == nil || !w.pendingCompactions.contains(id) ||
			mutation.compactionInputGeneration != plan.inputGenerations[idx] {
			continue
		}
		w.pendingCompactions.remove(mutation)
	}
}

// blockMetaEntries atomically pins only BlockMeta INSERT mutations. Object
// deletion compaction has no semantics for ordinary row mutations, tombstone
// batches, ALTER entries, or object statistics; selecting the immutable index
// avoids opening those unrelated payloads while preserving commit order and
// the existing payload-level validation in the compactor.
func (w *txnWorkspace) blockMetaEntries() (*workspaceEntrySet, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	view := client.NewWorkspaceReadView(
		w.id,
		w.revision,
		uint64(w.nextMutationID),
	)
	return w.entriesForMutationIDsLocked(view, w.activeBlockMeta.ids())
}

// liveObjectReferences returns the requested physical object names that are
// still referenced by active workspace mutations. Clone GC only needs this
// metadata decision; it must not pin and scan every transaction payload.
func (w *txnWorkspace) liveObjectReferences(
	names []string,
	ignoreEntry func(Entry) bool,
) map[string]struct{} {
	w.mu.RLock()
	defer w.mu.RUnlock()

	live := make(map[string]struct{}, len(names))
	for _, name := range names {
		for mutationID := range w.activeObjectReferences[name] {
			mutation := w.mutations[mutationID]
			if mutation == nil || !mutation.active ||
				mutation.retiredRevision != 0 {
				panic("workspace active object reference index is inconsistent")
			}
			if ignoreEntry != nil && ignoreEntry(mutation.entry) {
				continue
			}
			live[name] = struct{}{}
			break
		}
	}
	return live
}

// overlayIndexEntriesLocked resolves one immutable publication-time overlay
// index through a read view. The caller must hold w.mu for reading. Keeping
// ordering and payload pinning here ensures every secondary index has the same
// visibility and lifetime semantics as the canonical workspace read path.
func (w *txnWorkspace) overlayIndexEntriesLocked(
	view client.WorkspaceReadView,
	active *orderedMutationSet,
	selectRetiredIDs func(*tableOverlay) []workspaceMutationID,
) (*workspaceEntrySet, error) {
	ids := active.ids()
	for _, overlay := range w.overlays {
		ids = append(ids, selectRetiredIDs(overlay)...)
	}
	slices.SortStableFunc(ids, func(aID, bID workspaceMutationID) int {
		a := w.mutations[aID]
		b := w.mutations[bID]
		if a == nil || b == nil {
			return 0
		}
		return compareWorkspaceCommitOrder(a.commitOrder, b.commitOrder)
	})
	return w.entriesForMutationIDsLocked(view, ids)
}

// deleteTableCandidates returns one metadata-only entry for every table that
// has a currently visible, non-empty delete mutation of the requested
// physical representation. The overlay index prevents RC transfer planning
// from scanning and pinning unrelated inserts or unrelated tables.
func (w *txnWorkspace) deleteTableCandidates(isObject bool) ([]Entry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	result := make([]Entry, 0, len(w.overlays))
	for _, overlay := range w.overlays {
		ids := overlay.activeMemoryDeleteMutations
		if isObject {
			ids = overlay.activeObjectDeleteMutations
		}
		for id := range ids {
			mutation := w.mutations[id]
			if mutation == nil || !mutation.active || mutation.entry.skipTransfer {
				continue
			}
			lease, err := w.payloads.pin(mutation.payloadID, w.revision)
			if err != nil {
				return nil, err
			}
			entry := mutation.entry
			entry.bat = lease.bat
			view := workspaceEntryView{
				Entry:      entry,
				selections: lease.selections,
			}
			visible := view.visibleRowCount() != 0
			lease.Close()
			if !visible {
				continue
			}
			entry.bat = nil
			result = append(result, entry)
			break
		}
	}
	return result, nil
}

// VisitTableMutations visits the immutable mutations of one exact table that
// are visible through view. Entry payloads are generation-pinned for the
// duration of the call and must not be retained by fn after it returns.
//
// This is the concrete workspace inspection boundary used by engine tests and
// diagnostics. It deliberately exposes neither physical slice positions nor
// payload-store internals.
func (txn *Transaction) VisitTableMutations(
	ctx context.Context,
	view client.WorkspaceReadView,
	databaseID uint64,
	tableID uint64,
	fn func(Entry),
) error {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	entries, err := txn.workspace.tableEntries(view, accountID, databaseID, tableID)
	if err != nil {
		return err
	}
	defer entries.Close()
	for idx := range entries.entries {
		fn(entries.entries[idx].Entry)
	}
	return nil
}

// entries returns every mutation visible through one immutable read view in
// commit order. The returned set owns all physical payload leases; callers
// must close it and must not retain a Batch pointer after Close.
func (w *txnWorkspace) entries(
	view client.WorkspaceReadView,
) (*workspaceEntrySet, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if err := w.validateReadViewLocked(view); err != nil {
		return nil, err
	}
	if view.IsZero() {
		return &workspaceEntrySet{}, nil
	}
	var ids []workspaceMutationID
	if view.Revision() == w.revision &&
		view.MaxMutationID() == uint64(w.nextMutationID) {
		ids = w.activeMutationIDsInCommitOrderLocked()
	} else {
		ids = make([]workspaceMutationID, 0,
			w.activeMutations.len()+len(w.retiredMutationIDs))
		ids = append(ids, w.activeMutations.ids()...)
		for id := range w.retiredMutationIDs {
			ids = append(ids, id)
		}
		slices.SortStableFunc(ids, func(aID, bID workspaceMutationID) int {
			a := w.mutations[aID]
			b := w.mutations[bID]
			if a == nil || b == nil {
				return 0
			}
			return compareWorkspaceCommitOrder(a.commitOrder, b.commitOrder)
		})
	}
	return w.entriesForMutationIDsLocked(view, ids)
}

// activeMutationIDsInCommitOrderLocked materializes only the current logical
// workspace state. Immutable mutation history is retained for published
// statement ReadViews, but current-state consumers must never pay for that
// history. The caller must hold at least w.mu.RLock.
func (w *txnWorkspace) activeMutationIDsInCommitOrderLocked() []workspaceMutationID {
	return w.activeMutations.ids()
}

// commitEntries atomically captures the latest logical workspace revision and
// pins every visible payload in deterministic commit order. No mutation can
// be published between choosing the read boundary and acquiring its leases.
func (w *txnWorkspace) commitEntries() (*workspaceEntrySet, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	view := client.NewWorkspaceReadView(
		w.id,
		w.revision,
		uint64(w.nextMutationID),
	)
	ids := w.activeMutationIDsInCommitOrderLocked()
	return w.entriesForMutationIDsLocked(view, ids)
}

func (w *txnWorkspace) entriesForMutationIDsLocked(
	view client.WorkspaceReadView,
	ids []workspaceMutationID,
) (*workspaceEntrySet, error) {
	set := &workspaceEntrySet{entries: make([]workspaceEntryView, 0, len(ids))}
	for _, id := range ids {
		if uint64(id) > view.MaxMutationID() {
			continue
		}
		m := w.mutations[id]
		if m == nil || m.createdRevision > view.Revision() ||
			(m.retiredRevision != 0 && view.Revision() >= m.retiredRevision) {
			continue
		}
		lease, err := w.payloads.pin(m.payloadID, view.Revision())
		if err != nil {
			set.Close()
			return nil, err
		}
		entry := m.entry
		entry.bat = lease.bat
		set.entries = append(set.entries, workspaceEntryView{
			Entry:       entry,
			statementID: m.statementID,
			attemptID:   m.attemptID,
			selections:  lease.selections,
			lease:       lease,
		})
	}
	return set, nil
}

func (w *txnWorkspace) rollbackCurrentAttempt() (*workspaceRollback, error) {
	return w.rollbackCurrentAttemptAtBoundary(false)
}

func (w *txnWorkspace) rollbackCurrentAttemptWithRC() (*workspaceRollback, error) {
	return w.rollbackCurrentAttemptAtBoundary(true)
}

func (w *txnWorkspace) rollbackCurrentAttemptAtBoundary(
	rollbackRC bool,
) (*workspaceRollback, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.commitStarted {
		return nil, moerr.NewInternalErrorNoCtx(
			"workspace statement rollback cannot run after commit preparation")
	}
	current := w.journal.current
	var rcCompletedStatements uint64
	if rollbackRC && current.statementID > 0 {
		rcCompletedStatements = current.statementID - 1
		if err := w.journal.validateRollbackRCBoundary(rcCompletedStatements); err != nil {
			return nil, err
		}
	}

	mutationIDs := workspaceMutationIDs(current.mutations)
	view := client.NewWorkspaceReadView(
		w.id,
		w.revision,
		uint64(w.nextMutationID),
	)
	entries, err := w.entriesForMutationIDsLocked(view, mutationIDs)
	if err != nil {
		return nil, err
	}

	payloadIDs := make([]workspacePayloadID, 0, len(entries.entries))
	retiredUsage := workspaceUsage{}
	for idx := range entries.entries {
		mutation := w.mutations[entries.entries[idx].workspaceMutationID]
		if mutation == nil || !mutation.active || mutation.retiredRevision != 0 {
			entries.Close()
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rollback mutation changed before retirement")
		}
		payloadIDs = append(payloadIDs, mutation.payloadID)
		retiredUsage.add(usageOfWorkspaceEntry(
			mutation.entry,
			entries.entries[idx].bat,
		))
	}

	attemptMutationIDs := make(map[workspaceMutationID]struct{}, len(mutationIDs))
	for _, id := range mutationIDs {
		attemptMutationIDs[id] = struct{}{}
	}
	selectionRestores := make([]workspacePayloadSelectionRestore, 0, len(current.selectionUndo))
	selectionRestoreMutationIDs := make(
		[]workspaceMutationID, 0, len(current.selectionUndo))
	for id, selections := range current.selectionUndo {
		// A mutation created by this attempt is retired in full. Restoring one
		// of its intermediate selection generations would be both unnecessary
		// and contradictory.
		if _, retired := attemptMutationIDs[id]; retired {
			continue
		}
		// A rewritten older mutation is restored from rewriteUndo, including
		// the selection set visible before this attempt first changed it.
		if _, rewritten := current.rewriteUndo[id]; rewritten {
			continue
		}
		mutation := w.mutations[id]
		if mutation == nil || !mutation.active || mutation.retiredRevision != 0 {
			entries.Close()
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rollback selection target is not active")
		}
		selectionRestores = append(selectionRestores, workspacePayloadSelectionRestore{
			payloadID:  mutation.payloadID,
			selections: selections,
		})
		selectionRestoreMutationIDs = append(selectionRestoreMutationIDs, id)
	}
	rewriteRestores := make([]workspacePayloadRewriteRestore, 0, len(current.rewriteUndo))
	restoredUsage := workspaceUsage{}
	for _, undo := range current.rewriteUndo {
		source := w.mutations[undo.mutationID]
		if source == nil || source.active || source.retiredRevision == 0 ||
			source.payloadID != undo.payloadID {
			entries.Close()
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rollback rewrite source changed")
		}
		rewriteRestores = append(rewriteRestores, workspacePayloadRewriteRestore{
			payloadID:  undo.payloadID,
			bat:        undo.bat,
			selections: undo.selections,
		})
		restoredUsage.add(usageOfWorkspaceEntry(undo.entry, undo.bat))
	}
	activeObjectDeletes := make([]workspaceObjectDeleteID, 0, len(current.objectDeletes))
	for _, deleteID := range current.objectDeletes {
		objectDelete := w.objectDeletes[deleteID]
		if objectDelete == nil {
			entries.Close()
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rollback object delete is missing")
		}
		if !objectDelete.active {
			continue
		}
		if objectDelete.retiredRevision != 0 ||
			objectDelete.statementID != current.statementID ||
			objectDelete.attemptID != current.attemptID {
			entries.Close()
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rollback object delete changed")
		}
		activeObjectDeletes = append(activeObjectDeletes, deleteID)
	}
	for _, key := range current.droppedTables {
		overlay := w.overlays[key]
		if overlay == nil || overlay.droppedAt == nil ||
			*overlay.droppedAt != current.key() {
			entries.Close()
			return nil, moerr.NewInternalErrorNoCtx(
				"workspace rollback dropped table changed")
		}
	}
	if err := w.validateDDLRollbackLocked(current); err != nil {
		entries.Close()
		return nil, err
	}

	rollbackRevision := w.revision
	if len(payloadIDs) != 0 || len(selectionRestores) != 0 ||
		len(rewriteRestores) != 0 || len(activeObjectDeletes) != 0 ||
		len(current.droppedTables) != 0 || len(current.databaseOps) != 0 ||
		len(current.tableOps) != 0 || len(current.createdTables) != 0 {
		rollbackRevision++
		if len(payloadIDs) != 0 || len(selectionRestores) != 0 || len(rewriteRestores) != 0 {
			if err = w.payloads.rollbackAttempt(
				payloadIDs,
				selectionRestores,
				rewriteRestores,
				rollbackRevision,
			); err != nil {
				entries.Close()
				return nil, err
			}
		}
		w.revision = rollbackRevision
		w.usage.sub(retiredUsage)
		w.usage.add(restoredUsage)
		for _, id := range selectionRestoreMutationIDs {
			if mutation := w.mutations[id]; mutation != nil && mutation.active &&
				mutation.retiredRevision == 0 {
				w.refreshMutationAccessIndexesLocked(mutation, workspaceMutationIndexData{
					pkKeys: mutation.pkKeys, rowIDs: mutation.rowIDs,
					memoryInsert:       mutation.memoryInsert,
					pkIndexComplete:    mutation.pkIndexComplete,
					rowIDIndexComplete: mutation.rowIDIndexComplete,
				}, current.selectionUndo[id], rollbackRevision)
				w.queueCompactionLocked(mutation)
			}
		}
	}
	attempt := w.journal.rollbackCurrent()
	if rollbackRC && current.statementID > 0 {
		w.journal.rollbackRCBoundaryValidated(rcCompletedStatements)
	}
	for _, id := range mutationIDs {
		m := w.mutations[id]
		if m == nil || !m.active {
			continue
		}
		w.retireMutationStateLocked(m, rollbackRevision)
	}
	for _, deleteID := range activeObjectDeletes {
		w.retireObjectDeleteStateLocked(
			w.objectDeletes[deleteID], rollbackRevision)
	}
	for _, key := range attempt.droppedTables {
		w.overlays[key].droppedAt = nil
	}
	w.rollbackDDLLocked(attempt)
	for _, undo := range attempt.rewriteUndo {
		entry := undo.entry
		indexData := classifyWorkspaceMutation(entry, undo.bat)
		entry.bat = nil
		w.publishMutationAtCommitOrderLocked(
			entry,
			undo.payloadID,
			indexData,
			rollbackRevision,
			undo.statementID,
			undo.attemptID,
			undo.commitOrder,
		)
	}
	return &workspaceRollback{
		statementID: attempt.statementID,
		attemptID:   attempt.attemptID,
		mutationIDs: mutationIDs,
		entries:     entries,
		loadFiles:   slices.Clone(attempt.loadFiles),
		actions:     attempt.rollbackActions,
	}, nil
}

func (w *txnWorkspace) validateDDLRollbackLocked(attempt *statementAttempt) error {
	owner := attempt.key()
	databaseCounts := make(map[databaseKey]int)
	for _, key := range attempt.databaseOps {
		databaseCounts[key]++
	}
	for key, count := range databaseCounts {
		ops := w.ddl.databases[key]
		if len(ops) < count {
			return moerr.NewInternalErrorNoCtx(
				"workspace rollback database operation is missing")
		}
		for _, op := range ops[len(ops)-count:] {
			if op.owner != owner {
				return moerr.NewInternalErrorNoCtx(
					"workspace rollback database operation changed")
			}
		}
	}

	tableCounts := make(map[tableKey]int)
	for _, key := range attempt.tableOps {
		tableCounts[key]++
	}
	for key, count := range tableCounts {
		ops := w.ddl.tables[key]
		if len(ops) < count {
			return moerr.NewInternalErrorNoCtx(
				"workspace rollback table operation is missing")
		}
		for _, op := range ops[len(ops)-count:] {
			if op.owner != owner {
				return moerr.NewInternalErrorNoCtx(
					"workspace rollback table operation changed")
			}
		}
	}

	for _, tableID := range attempt.createdTables {
		if w.ddl.createdTables[tableID] != owner {
			return moerr.NewInternalErrorNoCtx(
				"workspace rollback created table changed")
		}
	}
	return nil
}

func (w *txnWorkspace) rollbackDDLLocked(attempt statementAttempt) {
	for idx := len(attempt.databaseOps) - 1; idx >= 0; idx-- {
		key := attempt.databaseOps[idx]
		ops := w.ddl.databases[key]
		ops = ops[:len(ops)-1]
		if len(ops) == 0 {
			delete(w.ddl.databases, key)
		} else {
			w.ddl.databases[key] = ops
		}
	}
	for idx := len(attempt.tableOps) - 1; idx >= 0; idx-- {
		key := attempt.tableOps[idx]
		ops := w.ddl.tables[key]
		removed := ops[len(ops)-1]
		if removed.previousActiveKey == nil {
			delete(w.ddl.activeTableKeyByID, removed.tableID)
		} else {
			w.ddl.activeTableKeyByID[removed.tableID] = *removed.previousActiveKey
		}
		ops = ops[:len(ops)-1]
		if len(ops) == 0 {
			delete(w.ddl.tables, key)
		} else {
			w.ddl.tables[key] = ops
		}
		stillIndexed := false
		for _, op := range ops {
			if op.tableID == removed.tableID {
				stillIndexed = true
				break
			}
		}
		if !stillIndexed {
			keys := w.ddl.tableKeysByID[removed.tableID]
			delete(keys, key)
			if len(keys) == 0 {
				delete(w.ddl.tableKeysByID, removed.tableID)
			}
		}
	}
	for _, tableID := range attempt.createdTables {
		delete(w.ddl.createdTables, tableID)
	}
}

func (w *txnWorkspace) close(mp *mpool.MPool) error {
	w.mu.Lock()
	batches, err := w.payloads.takeAll()
	if err != nil {
		w.mu.Unlock()
		return err
	}
	w.closed = true
	w.mutations = nil
	w.activeMutations = nil
	w.activePKCandidates = nil
	w.activeCompactionCount = 0
	w.pendingCompactions = nil
	w.activeBlockMeta = nil
	w.retiredMutationIDs = nil
	w.objectDeletes = nil
	w.activeObjectDeletes = nil
	w.retiredObjectDeletes = nil
	w.activeObjectReferences = nil
	w.overlays = nil
	w.ddl = workspaceDDLCatalog{}
	w.usage = workspaceUsage{}
	w.mu.Unlock()

	for _, bat := range batches {
		bat.Clean(mp)
	}
	return nil
}
