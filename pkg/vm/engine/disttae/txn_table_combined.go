// Copyright 2021-2024 Matrix Origin
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

package disttae

import (
	"container/heap"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	splan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
)

var _ engine.Relation = (*combinedTxnTable)(nil)

type pruneFunc func(
	ctx context.Context,
	param engine.RangesParam,
) ([]engine.Relation, error)

type prunePKFunc func(
	bat *batch.Batch,
	partitionIndex int32,
) ([]engine.Relation, error)

type tablesFunc func() ([]engine.Relation, error)

type combinedTxnTable struct {
	primary     *txnTable
	pruneFunc   pruneFunc
	tablesFunc  tablesFunc
	prunePKFunc prunePKFunc
}

func newCombinedTxnTable(
	primary *txnTable,
	tablesFunc tablesFunc,
	pruneFunc pruneFunc,
	prunePKFunc prunePKFunc,
) *combinedTxnTable {
	return &combinedTxnTable{
		primary:     primary,
		pruneFunc:   filterNilPrunedRelations(pruneFunc),
		tablesFunc:  filterNilTableRelations(tablesFunc),
		prunePKFunc: filterNilPKPrunedRelations(prunePKFunc),
	}
}

func filterNilRelations(relations []engine.Relation) []engine.Relation {
	for i, rel := range relations {
		if rel == nil {
			filtered := make([]engine.Relation, 0, len(relations)-1)
			filtered = append(filtered, relations[:i]...)
			for _, remaining := range relations[i+1:] {
				if remaining != nil {
					filtered = append(filtered, remaining)
				}
			}
			return filtered
		}
	}
	return relations
}

func filterNilTableRelations(fn tablesFunc) tablesFunc {
	return func() ([]engine.Relation, error) {
		relations, err := fn()
		if err != nil {
			return nil, err
		}
		return filterNilRelations(relations), nil
	}
}

func filterNilPrunedRelations(fn pruneFunc) pruneFunc {
	return func(
		ctx context.Context,
		param engine.RangesParam,
	) ([]engine.Relation, error) {
		relations, err := fn(ctx, param)
		if err != nil {
			return nil, err
		}
		return filterNilRelations(relations), nil
	}
}

func filterNilPKPrunedRelations(fn prunePKFunc) prunePKFunc {
	return func(
		bat *batch.Batch,
		partitionIndex int32,
	) ([]engine.Relation, error) {
		relations, err := fn(bat, partitionIndex)
		if err != nil {
			return nil, err
		}
		return filterNilRelations(relations), nil
	}
}

func (t *combinedTxnTable) Ranges(
	ctx context.Context,
	param engine.RangesParam,
) (engine.RelData, error) {
	relations, err := t.pruneFunc(ctx, param)
	if err != nil {
		return nil, err
	}

	pd := newCombinedRelData()
	for _, rel := range relations {
		if err := pd.add(ctx, rel, param); err != nil {
			return nil, err
		}
	}
	return pd, nil
}

func (t *combinedTxnTable) BuildReaders(
	ctx context.Context,
	proc any,
	expr *plan.Expr,
	relData engine.RelData,
	num int,
	txnOffset int,
	orderBy bool,
	policy engine.TombstoneApplyPolicy,
	filterHint engine.FilterHint,
) ([]engine.Reader, error) {
	preparedHint, mainFilter, owned, err := prepareMembershipFilter(
		filterHint,
		membershipFilterAdmissionForProcess(proc),
	)
	if err != nil {
		return nil, err
	}
	if owned {
		defer mainFilter.Free()
	}
	filterHint = preparedHint

	var readers []engine.Reader
	if relData == nil {
		tables, err := t.tablesFunc()
		if err != nil {
			return nil, err
		}
		for _, rel := range tables {
			r, err := rel.BuildReaders(
				ctx,
				proc,
				expr,
				nil,
				num,
				txnOffset,
				orderBy,
				policy,
				filterHint,
			)
			if err != nil {
				closeReaders(readers)
				return nil, err
			}
			readers = append(readers, r...)
		}
		return ensureReaders(readers, num), nil
	}

	r := relData.(*CombinedRelData)
	for idx, data := range r.tables {
		rel := r.relations[idx]
		r, err := rel.BuildReaders(
			ctx,
			proc,
			expr,
			data,
			num,
			txnOffset,
			orderBy,
			policy,
			filterHint,
		)
		if err != nil {
			closeReaders(readers)
			return nil, err
		}
		readers = append(readers, r...)
	}
	return ensureReaders(readers, num), nil
}

func closeReaders(readers []engine.Reader) {
	for _, rd := range readers {
		rd.Close()
	}
}

func ensureReaders(readers []engine.Reader, num int) []engine.Reader {
	if len(readers) > 0 {
		return readers
	}
	readers = make([]engine.Reader, num)
	for i := range readers {
		readers[i] = new(readutil.EmptyReader)
	}
	return readers
}

func (t *combinedTxnTable) BuildShardingReaders(
	ctx context.Context,
	proc any,
	expr *plan.Expr,
	relData engine.RelData,
	num int,
	txnOffset int,
	orderBy bool,
	policy engine.TombstoneApplyPolicy,
) ([]engine.Reader, error) {
	panic("Not Support")
}

func (t *combinedTxnTable) Rows(
	ctx context.Context,
) (uint64, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return 0, err
	}

	rows := uint64(0)
	for _, rel := range tables {
		v, err := rel.Rows(ctx)
		if err != nil {
			return 0, err
		}

		rows += v
	}
	return rows, nil
}

func (t *combinedTxnTable) Stats(
	ctx context.Context,
	sync bool,
) (*statsinfo.StatsInfo, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return nil, err
	}

	value := splan.NewStatsInfo()
	for _, rel := range tables {
		v, err := rel.Stats(ctx, sync)
		if err != nil {
			return nil, err
		}

		value.Merge(v)
	}
	return value, nil
}

func (t *combinedTxnTable) Size(
	ctx context.Context,
	columnName string,
) (uint64, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return 0, err
	}

	value := uint64(0)
	for _, rel := range tables {
		v, err := rel.Size(ctx, columnName)
		if err != nil {
			return 0, err
		}

		value += v
	}
	return value, nil
}

func (t *combinedTxnTable) CollectTombstones(
	ctx context.Context,
	txnOffset int,
	policy engine.TombstoneCollectPolicy,
) (engine.Tombstoner, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return nil, err
	}

	var tombstone engine.Tombstoner
	for _, rel := range tables {
		t, err := rel.CollectTombstones(ctx, txnOffset, policy)
		if err != nil {
			return nil, err
		}
		if tombstone == nil {
			tombstone = t
			continue
		}
		if err := tombstone.Merge(t); err != nil {
			return nil, err
		}
	}
	return tombstone, nil
}

func (t *combinedTxnTable) StarCount(ctx context.Context) (uint64, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return 0, err
	}

	var total uint64
	for _, rel := range tables {
		count, err := rel.StarCount(ctx)
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func (t *combinedTxnTable) EstimateCommittedTombstoneCount(ctx context.Context) (int, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return 0, err
	}

	var total int
	for _, rel := range tables {
		count, err := rel.EstimateCommittedTombstoneCount(ctx)
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func (t *combinedTxnTable) CollectChanges(
	ctx context.Context,
	from, to types.TS,
	skipDeletes bool,
	mp *mpool.MPool,
) (engine.ChangesHandle, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return nil, err
	}

	handle := &combinedChangesHandle{
		mp:       mp,
		snapshot: from.IsEmpty(),
	}
	for _, rel := range tables {
		partitionHandle, err := rel.CollectChanges(ctx, from, to, skipDeletes, mp)
		if err != nil {
			_ = handle.Close()
			return nil, err
		}
		if partitionHandle != nil {
			handle.handles = append(handle.handles, partitionHandle)
		}
	}
	return handle, nil
}

type combinedChangesHandle struct {
	handles         []engine.ChangesHandle
	pending         []pendingPartitionChanges
	frontier        pendingChangesHeap
	frontierGroups  map[types.TS]pendingChangeGroup
	mp              *mpool.MPool
	idx             int
	snapshot        bool
	closed          bool
	pendingLoaded   bool
	lastCommitTS    types.TS
	hasLastCommitTS bool
	tailRangeMask   pendingChangeMask
	tailRangeRows   int
}

type pendingPartitionChanges struct {
	data            *batch.Batch
	tombstone       *batch.Batch
	dataOffset      int
	tombstoneOffset int
	hint            engine.ChangesHandle_Hint
	exhausted       bool
	closed          bool
}

type pendingChangeKind uint8

const (
	pendingChangeData pendingChangeKind = iota
	pendingChangeTombstone
)

type pendingChangeMask uint8

const (
	pendingChangeNone     pendingChangeMask = 0
	pendingChangeDataMask pendingChangeMask = 1 << iota
	pendingChangeTombstoneMask
)

const combinedTailMaxRows = int(objectio.BlockMaxRows)

type pendingChangeFrontier struct {
	partition int
	kind      pendingChangeKind
	commitTS  types.TS
}

type pendingChangeGroup struct {
	data      int
	tombstone int
}

func (g pendingChangeGroup) mask() pendingChangeMask {
	var mask pendingChangeMask
	if g.data > 0 {
		mask |= pendingChangeDataMask
	}
	if g.tombstone > 0 {
		mask |= pendingChangeTombstoneMask
	}
	return mask
}

func (m pendingChangeMask) isPure() bool {
	return m == pendingChangeDataMask || m == pendingChangeTombstoneMask
}

type pendingChangesHeap []pendingChangeFrontier

func (h pendingChangesHeap) Len() int {
	return len(h)
}

func (h pendingChangesHeap) Less(i, j int) bool {
	if h[i].commitTS.EQ(&h[j].commitTS) {
		if h[i].partition == h[j].partition {
			return h[i].kind < h[j].kind
		}
		return h[i].partition < h[j].partition
	}
	return h[i].commitTS.LT(&h[j].commitTS)
}

func (h pendingChangesHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *pendingChangesHeap) Push(value any) {
	*h = append(*h, value.(pendingChangeFrontier))
}

func (h *pendingChangesHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

func (h *combinedChangesHandle) Next(
	ctx context.Context,
	mp *mpool.MPool,
) (data *batch.Batch, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	if h.closed {
		return nil, nil, engine.ChangesHandle_Tail_done, nil
	}
	if h.mp == nil {
		h.mp = mp
	}
	if h.snapshot {
		return h.nextSnapshot(ctx, mp)
	}
	return h.nextTail(ctx, mp)
}

func (h *combinedChangesHandle) nextSnapshot(
	ctx context.Context,
	mp *mpool.MPool,
) (data *batch.Batch, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	// Checkpoint chunks share a synthetic commit timestamp. They do not form a
	// transaction boundary, so return each child chunk directly instead of
	// accumulating an entire partitioned snapshot by that timestamp.
	h.ensurePending()
	for h.idx < len(h.handles) {
		for {
			data, tombstone, hint, err = h.handles[h.idx].Next(ctx, mp)
			if err != nil {
				return h.fail(mp, data, tombstone, err)
			}
			hadEmptyBatch := false
			if data != nil && data.RowCount() == 0 {
				data.Clean(mp)
				data = nil
				hadEmptyBatch = true
			}
			if tombstone != nil && tombstone.RowCount() == 0 {
				tombstone.Clean(mp)
				tombstone = nil
				hadEmptyBatch = true
			}
			if data != nil || tombstone != nil {
				if hint != engine.ChangesHandle_Snapshot {
					return h.fail(mp, data, tombstone, moerr.NewInternalErrorNoCtx("checkpoint changes handle returned tail data"))
				}
				return data, tombstone, hint, nil
			}
			if hadEmptyBatch {
				continue
			}
			if err = h.closePartition(h.idx, mp); err != nil {
				return h.fail(mp, nil, nil, err)
			}
			h.idx++
			break
		}
	}
	return nil, nil, engine.ChangesHandle_Tail_done, nil
}

func (h *combinedChangesHandle) nextTail(
	ctx context.Context,
	mp *mpool.MPool,
) (data *batch.Batch, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	h.ensurePending()

	if !h.pendingLoaded {
		if err = h.loadPending(ctx, mp); err != nil {
			return h.fail(mp, nil, nil, err)
		}
		h.pendingLoaded = true
	}
	commitTS, ok := h.nextCommitTS()
	if !ok {
		return nil, nil, engine.ChangesHandle_Tail_done, nil
	}
	h.lastCommitTS = commitTS
	h.hasLastCommitTS = true

	var groupHint engine.ChangesHandle_Hint
	hasGroupHint := false
	for h.frontier.Len() > 0 && h.frontier[0].commitTS.EQ(&commitTS) {
		entry := h.popFrontier()
		pending := &h.pending[entry.partition]

		var src **batch.Batch
		var offset *int
		var dst **batch.Batch
		if entry.kind == pendingChangeData {
			src = &pending.data
			offset = &pending.dataOffset
			dst = &data
		} else {
			src = &pending.tombstone
			offset = &pending.tombstoneOffset
			dst = &tombstone
		}

		var appended bool
		if appended, err = appendChangesAtCommitTS(dst, src, offset, commitTS, mp); err != nil {
			return h.fail(mp, data, tombstone, err)
		}
		if !appended {
			return h.fail(mp, data, tombstone, moerr.NewInternalErrorNoCtx("combined changes frontier lost commit timestamp"))
		}
		groupHint, hasGroupHint, err = mergeChangesHint(groupHint, hasGroupHint, pending.hint)
		if err != nil {
			return h.fail(mp, data, tombstone, err)
		}

		if *src != nil {
			if err = h.pushFrontier(entry.partition, entry.kind); err != nil {
				return h.fail(mp, data, tombstone, err)
			}
		} else if pending.data == nil && pending.tombstone == nil {
			if err = h.loadPendingPartition(ctx, mp, entry.partition); err != nil {
				return h.fail(mp, data, tombstone, err)
			}
		}
	}

	if !hasGroupHint {
		return h.fail(mp, data, tombstone, moerr.NewInternalErrorNoCtx("combined changes handle lost commit timestamp"))
	}
	if groupHint == engine.ChangesHandle_Snapshot {
		return h.fail(mp, data, tombstone, moerr.NewInternalErrorNoCtx("tail changes handle returned snapshot data"))
	}
	mask := changeMask(data, tombstone)
	if h.tailRangeMask != pendingChangeNone && h.tailRangeMask != mask {
		return h.fail(mp, data, tombstone, moerr.NewInternalErrorNoCtx("combined tail range changed operation kind before completion"))
	}

	// A mixed timestamp remains a Tail_done boundary: the sink applies all
	// deletes before all inserts, so spanning an earlier insert and a later
	// delete could reverse the final state for one primary key. Consecutive
	// pure insert or pure delete timestamps are safe to accumulate and are
	// bounded to the regular change-handler batch size.
	return data, tombstone, h.tailHint(mask, changeRows(data, tombstone)), nil
}

func (h *combinedChangesHandle) Close() error {
	if h.closed {
		return nil
	}
	return h.closeRemaining()
}

func (h *combinedChangesHandle) closeRemaining() error {
	if h.closed {
		return nil
	}
	h.closed = true
	h.ensurePending()
	var firstErr error
	for i := range h.handles {
		pending := &h.pending[i]
		cleanPendingChanges(pending, h.mp)
		if pending.closed {
			continue
		}
		pending.closed = true
		if err := h.handles[i].Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	h.frontier = nil
	h.frontierGroups = nil
	h.tailRangeMask = pendingChangeNone
	h.tailRangeRows = 0
	return firstErr
}

func (h *combinedChangesHandle) ensurePending() {
	if len(h.pending) == len(h.handles) {
		return
	}
	h.pending = make([]pendingPartitionChanges, len(h.handles))
}

func (h *combinedChangesHandle) loadPending(ctx context.Context, mp *mpool.MPool) error {
	for i := range h.pending {
		if err := h.loadPendingPartition(ctx, mp, i); err != nil {
			return err
		}
	}
	return nil
}

func (h *combinedChangesHandle) loadPendingPartition(ctx context.Context, mp *mpool.MPool, index int) error {
	pending := &h.pending[index]
	if pending.exhausted || pending.data != nil || pending.tombstone != nil {
		return nil
	}
	data, tombstone, hint, err := h.handles[index].Next(ctx, mp)
	if err != nil {
		return err
	}
	if data != nil && data.RowCount() == 0 {
		data.Clean(mp)
		data = nil
	}
	if tombstone != nil && tombstone.RowCount() == 0 {
		tombstone.Clean(mp)
		tombstone = nil
	}
	if data != nil || tombstone != nil {
		pending.data = data
		pending.tombstone = tombstone
		pending.dataOffset = 0
		pending.tombstoneOffset = 0
		pending.hint = hint
		return h.pushPartitionFrontier(index)
	}
	pending.exhausted = true
	return h.closePartition(index, mp)
}

func (h *combinedChangesHandle) closePartition(index int, mp *mpool.MPool) error {
	pending := &h.pending[index]
	if pending.closed {
		return nil
	}
	cleanPendingChanges(pending, mp)
	pending.closed = true
	return h.handles[index].Close()
}

func (h *combinedChangesHandle) nextCommitTS() (types.TS, bool) {
	if h.frontier.Len() == 0 {
		return types.TS{}, false
	}
	return h.frontier[0].commitTS, true
}

func (h *combinedChangesHandle) pushPartitionFrontier(index int) error {
	if err := h.pushFrontier(index, pendingChangeData); err != nil {
		return err
	}
	return h.pushFrontier(index, pendingChangeTombstone)
}

func (h *combinedChangesHandle) pushFrontier(index int, kind pendingChangeKind) error {
	pending := &h.pending[index]
	var source *batch.Batch
	var offset int
	if kind == pendingChangeData {
		source = pending.data
		offset = pending.dataOffset
	} else {
		source = pending.tombstone
		offset = pending.tombstoneOffset
	}
	commitTS, ok := commitTSAt(source, offset)
	if !ok {
		return nil
	}
	if h.hasLastCommitTS && commitTS.LT(&h.lastCommitTS) {
		return moerr.NewInternalErrorNoCtx("partition change stream is not ordered by commit timestamp")
	}
	entry := pendingChangeFrontier{
		partition: index,
		kind:      kind,
		commitTS:  commitTS,
	}
	h.addFrontier(entry)
	heap.Push(&h.frontier, entry)
	return nil
}

func (h *combinedChangesHandle) addFrontier(entry pendingChangeFrontier) {
	if h.frontierGroups == nil {
		h.frontierGroups = make(map[types.TS]pendingChangeGroup)
	}
	group := h.frontierGroups[entry.commitTS]
	if entry.kind == pendingChangeData {
		group.data++
	} else {
		group.tombstone++
	}
	h.frontierGroups[entry.commitTS] = group
}

func (h *combinedChangesHandle) popFrontier() pendingChangeFrontier {
	entry := heap.Pop(&h.frontier).(pendingChangeFrontier)
	group := h.frontierGroups[entry.commitTS]
	if entry.kind == pendingChangeData {
		group.data--
	} else {
		group.tombstone--
	}
	if group.data == 0 && group.tombstone == 0 {
		delete(h.frontierGroups, entry.commitTS)
	} else {
		h.frontierGroups[entry.commitTS] = group
	}
	return entry
}

func (h *combinedChangesHandle) nextFrontierMask() pendingChangeMask {
	if h.frontier.Len() == 0 {
		return pendingChangeNone
	}
	return h.frontierGroups[h.frontier[0].commitTS].mask()
}

func (h *combinedChangesHandle) tailHint(mask pendingChangeMask, rows int) engine.ChangesHandle_Hint {
	if mask.isPure() && h.nextFrontierMask() == mask && h.tailRangeRows+rows < combinedTailMaxRows {
		h.tailRangeMask = mask
		h.tailRangeRows += rows
		return engine.ChangesHandle_Tail_wip
	}
	h.tailRangeMask = pendingChangeNone
	h.tailRangeRows = 0
	return engine.ChangesHandle_Tail_done
}

func (h *combinedChangesHandle) fail(mp *mpool.MPool, data, tombstone *batch.Batch, err error) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	if data != nil {
		data.Clean(mp)
	}
	if tombstone != nil {
		tombstone.Clean(mp)
	}
	_ = h.closeRemaining()
	return nil, nil, engine.ChangesHandle_Tail_done, err
}

func cleanPendingChanges(pending *pendingPartitionChanges, mp *mpool.MPool) {
	if pending.data != nil {
		pending.data.Clean(mp)
		pending.data = nil
	}
	if pending.tombstone != nil {
		pending.tombstone.Clean(mp)
		pending.tombstone = nil
	}
	pending.dataOffset = 0
	pending.tombstoneOffset = 0
}

func mergeChangesHint(current engine.ChangesHandle_Hint, hasCurrent bool, next engine.ChangesHandle_Hint) (engine.ChangesHandle_Hint, bool, error) {
	if !hasCurrent {
		return next, true, nil
	}
	if (current == engine.ChangesHandle_Snapshot) != (next == engine.ChangesHandle_Snapshot) {
		return engine.ChangesHandle_Tail_done, false, moerr.NewInternalErrorNoCtx("partition change streams mixed snapshot and tail data at the same commit timestamp")
	}
	if current == engine.ChangesHandle_Snapshot {
		return current, true, nil
	}
	return engine.ChangesHandle_Tail_wip, true, nil
}

func appendChangesAtCommitTS(dst **batch.Batch, src **batch.Batch, offset *int, commitTS types.TS, mp *mpool.MPool) (bool, error) {
	if *src == nil {
		return false, nil
	}
	first, _ := commitTSAt(*src, *offset)
	if !first.EQ(&commitTS) {
		return false, nil
	}

	count := 1
	for *offset+count < (*src).RowCount() {
		ts, _ := commitTSAt(*src, *offset+count)
		if !ts.EQ(&commitTS) {
			break
		}
		count++
	}
	if *dst == nil {
		*dst = newChangeBatch(*src)
	}
	if err := (*dst).UnionWindow(*src, *offset, count, mp); err != nil {
		return false, err
	}

	*offset += count
	if *offset == (*src).RowCount() {
		(*src).Clean(mp)
		*src = nil
		*offset = 0
	}
	return true, nil
}

func newChangeBatch(src *batch.Batch) *batch.Batch {
	dst := batch.NewWithSize(len(src.Vecs))
	dst.Attrs = append(dst.Attrs, src.Attrs...)
	for i, vec := range src.Vecs {
		dst.Vecs[i] = vector.NewVec(*vec.GetType())
	}
	return dst
}

func changeMask(data, tombstone *batch.Batch) pendingChangeMask {
	var mask pendingChangeMask
	if data != nil {
		mask |= pendingChangeDataMask
	}
	if tombstone != nil {
		mask |= pendingChangeTombstoneMask
	}
	return mask
}

func changeRows(data, tombstone *batch.Batch) int {
	rows := 0
	if data != nil {
		rows += data.RowCount()
	}
	if tombstone != nil {
		rows += tombstone.RowCount()
	}
	return rows
}

func commitTSAt(bat *batch.Batch, row int) (types.TS, bool) {
	if bat == nil || bat.RowCount() == 0 {
		return types.TS{}, false
	}
	commitTS := bat.Vecs[len(bat.Vecs)-1]
	if commitTS.IsConst() {
		row = 0
	}
	return vector.GetFixedAtNoTypeCheck[types.TS](commitTS, row), true
}

func (t *combinedTxnTable) CollectObjectList(
	ctx context.Context,
	from, to types.TS,
	bat *batch.Batch,
	mp *mpool.MPool,
) error {
	panic("not implemented")
}

func (t *combinedTxnTable) ApproxObjectsNum(ctx context.Context) int {
	tables, err := t.tablesFunc()
	if err != nil {
		return 0
	}

	num := 0
	for _, rel := range tables {
		num += rel.ApproxObjectsNum(ctx)
	}
	return num
}

func (t *combinedTxnTable) MergeObjects(
	ctx context.Context,
	objstats []objectio.ObjectStats,
	targetObjSize uint32,
) (*api.MergeCommitEntry, error) {
	panic("not implemented")
}

func (t *combinedTxnTable) GetNonAppendableObjectStats(ctx context.Context) ([]objectio.ObjectStats, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return nil, err
	}

	var stats []objectio.ObjectStats
	for _, rel := range tables {
		values, err := rel.GetNonAppendableObjectStats(ctx)
		if err != nil {
			return nil, err
		}
		stats = append(stats, values...)
	}
	return stats, nil
}

func (t *combinedTxnTable) GetColumMetadataScanInfo(
	ctx context.Context,
	name string,
	visitTombstone bool,
) ([]*plan.MetadataScanInfo, error) {
	tables, err := t.tablesFunc()
	if err != nil {
		return nil, err
	}

	var values []*plan.MetadataScanInfo
	for _, rel := range tables {
		v, err := rel.GetColumMetadataScanInfo(ctx, name, visitTombstone)
		if err != nil {
			return nil, err
		}
		values = append(values, v...)
	}
	return values, nil
}

func (t *combinedTxnTable) UpdateConstraint(context.Context, *engine.ConstraintDef) error {
	panic("not implemented")
}

func (t *combinedTxnTable) AlterTable(ctx context.Context, c *engine.ConstraintDef, reqs []*api.AlterTableReq) error {
	return t.primary.AlterTable(ctx, c, reqs)
}

func (t *combinedTxnTable) TableRenameInTxn(ctx context.Context, constraint [][]byte) error {
	panic("not implemented")
}

func (t *combinedTxnTable) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	panic("not implemented")
}

func (t *combinedTxnTable) TableDefs(ctx context.Context) ([]engine.TableDef, error) {
	return t.primary.TableDefs(ctx)
}

func (t *combinedTxnTable) GetTableDef(ctx context.Context) *plan.TableDef {
	return t.primary.GetTableDef(ctx)
}

func (t *combinedTxnTable) CopyTableDef(ctx context.Context) *plan.TableDef {
	return t.primary.CopyTableDef(ctx)
}

func (t *combinedTxnTable) GetPrimaryKeys(ctx context.Context) ([]*engine.Attribute, error) {
	return t.primary.GetPrimaryKeys(ctx)
}

func (t *combinedTxnTable) AddTableDef(context.Context, engine.TableDef) error {
	return nil
}

func (t *combinedTxnTable) DelTableDef(context.Context, engine.TableDef) error {
	return nil
}

func (t *combinedTxnTable) GetTableID(ctx context.Context) uint64 {
	return t.primary.GetTableID(ctx)
}

func (t *combinedTxnTable) GetTableName() string {
	return t.primary.GetTableName()
}

func (t *combinedTxnTable) GetDBID(ctx context.Context) uint64 {
	return t.primary.GetDBID(ctx)
}

func (t *combinedTxnTable) TableColumns(ctx context.Context) ([]*engine.Attribute, error) {
	return t.primary.TableColumns(ctx)
}

func (t *combinedTxnTable) GetEngineType() engine.EngineType {
	return t.primary.GetEngineType()
}

func (t *combinedTxnTable) GetProcess() any {
	return t.primary.GetProcess()
}

func (t *combinedTxnTable) PrimaryKeysMayBeModified(
	ctx context.Context,
	from types.TS,
	to types.TS,
	bat *batch.Batch,
	pkIndex int32,
	partitionIndex int32,
) (bool, error) {
	relations, err := t.prunePKFunc(bat, partitionIndex)
	if err != nil {
		return false, err
	}

	changed := false
	for _, rel := range relations {
		v, e := rel.PrimaryKeysMayBeModified(
			ctx,
			from,
			to,
			bat,
			pkIndex,
			partitionIndex,
		)
		if e != nil {
			return false, e
		}
		if v {
			changed = true
			break
		}
	}
	return changed, err
}

func (t *combinedTxnTable) Write(context.Context, *batch.Batch) error {
	panic("BUG: cannot write data to partition primary table")
}

func (t *combinedTxnTable) Delete(context.Context, *batch.Batch, string) error {
	panic("BUG: cannot delete data to partition primary table")
}

func (t *combinedTxnTable) PrimaryKeysMayBeUpserted(
	ctx context.Context,
	from types.TS,
	to types.TS,
	bat *batch.Batch,
	pkIndex int32,
) (bool, error) {
	relations, err := t.prunePKFunc(bat, -1)
	if err != nil {
		return false, err
	}

	changed := false
	for _, rel := range relations {
		v, e := rel.PrimaryKeysMayBeUpserted(
			ctx,
			from,
			to,
			bat,
			pkIndex,
		)
		if e != nil {
			return false, e
		}
		if v {
			changed = true
			break
		}
	}
	return changed, err
}

func (t *combinedTxnTable) Reset(op client.TxnOperator) error {
	return moerr.NewInternalErrorNoCtx("cannot reset a shared combined relation")
}

func (t *combinedTxnTable) GetFlushTS(
	ctx context.Context,
) (types.TS, error) {
	return t.primary.GetFlushTS(ctx)
}

func (t *combinedTxnTable) GetExtraInfo() *api.SchemaExtra {
	return t.primary.extraInfo
}

type CombinedRelData struct {
	cnt       int
	blocks    objectio.BlockInfoSlice
	tables    []engine.RelData
	relations []engine.Relation
}

func newCombinedRelData() *CombinedRelData {
	return &CombinedRelData{}
}

func (r *CombinedRelData) add(
	ctx context.Context,
	table engine.Relation,
	param engine.RangesParam,
) error {
	data, err := table.Ranges(
		ctx,
		param,
	)
	if err != nil {
		return err
	}

	r.relations = append(r.relations, table)
	r.tables = append(r.tables, data)
	r.cnt += data.DataCnt()
	r.blocks = append(r.blocks, data.GetBlockInfoSlice()...)
	return nil
}

func (r *CombinedRelData) AttachTombstones(tombstones engine.Tombstoner) error {
	for _, p := range r.tables {
		if err := p.AttachTombstones(tombstones); err != nil {
			return err
		}
	}
	return nil
}

func (r *CombinedRelData) BuildEmptyRelData(preAllocSize int) engine.RelData {
	for _, p := range r.tables {
		return p.BuildEmptyRelData(preAllocSize)
	}
	panic("BUG: no partitions")
}

func (r *CombinedRelData) DataCnt() int {
	return r.cnt
}

func (r *CombinedRelData) GetBlockInfoSlice() objectio.BlockInfoSlice {
	return r.blocks
}

func (r *CombinedRelData) GetType() engine.RelDataType {
	panic("not implemented")
}

func (r *CombinedRelData) String() string {
	return "PartitionedRelData"
}

func (r *CombinedRelData) MarshalBinary() ([]byte, error) {
	panic("not implemented")
}

func (r *CombinedRelData) UnmarshalBinary(buf []byte) error {
	panic("not implemented")
}

func (r *CombinedRelData) GetTombstones() engine.Tombstoner {
	panic("not implemented")
}

func (r *CombinedRelData) DataSlice(begin, end int) engine.RelData {
	panic("not implemented")
}

func (r *CombinedRelData) SetBlockInfo(i int, blk *objectio.BlockInfo) {
	panic("not implemented")
}

func (r *CombinedRelData) GetBlockInfo(i int) objectio.BlockInfo {
	panic("not implemented")
}

func (r *CombinedRelData) AppendBlockInfo(blk *objectio.BlockInfo) {
	panic("not implemented")
}

func (r *CombinedRelData) AppendBlockInfoSlice(objectio.BlockInfoSlice) {
	panic("not implemented")
}

func (r *CombinedRelData) Split(i int) []engine.RelData {
	panic("not implemented")
}
