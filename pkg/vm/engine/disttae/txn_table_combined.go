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
		pruneFunc:   pruneFunc,
		tablesFunc:  tablesFunc,
		prunePKFunc: prunePKFunc,
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
				return nil, err
			}
			readers = append(readers, r...)
		}
		return readers, nil
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
			return nil, err
		}
		readers = append(readers, r...)
	}
	return readers, nil
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
	handles  []engine.ChangesHandle
	pending  []pendingPartitionChanges
	mp       *mpool.MPool
	idx      int
	snapshot bool
	closed   bool
}

type pendingPartitionChanges struct {
	data      *batch.Batch
	tombstone *batch.Batch
	hint      engine.ChangesHandle_Hint
	exhausted bool
	closed    bool
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

	if err = h.loadPending(ctx, mp); err != nil {
		return h.fail(mp, nil, nil, err)
	}
	commitTS, ok := h.nextCommitTS()
	if !ok {
		return nil, nil, engine.ChangesHandle_Tail_done, nil
	}

	var groupHint engine.ChangesHandle_Hint
	hasGroupHint := false
	for {
		for i := range h.pending {
			pending := &h.pending[i]
			var appended bool
			if appended, err = appendChangesAtCommitTS(&data, &pending.data, commitTS, mp); err != nil {
				return h.fail(mp, data, tombstone, err)
			}
			if appended {
				groupHint, hasGroupHint, err = mergeChangesHint(groupHint, hasGroupHint, pending.hint)
				if err != nil {
					return h.fail(mp, data, tombstone, err)
				}
			}

			if appended, err = appendChangesAtCommitTS(&tombstone, &pending.tombstone, commitTS, mp); err != nil {
				return h.fail(mp, data, tombstone, err)
			}
			if appended {
				groupHint, hasGroupHint, err = mergeChangesHint(groupHint, hasGroupHint, pending.hint)
				if err != nil {
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
		if err = h.loadPending(ctx, mp); err != nil {
			return h.fail(mp, data, tombstone, err)
		}

		nextTS, hasNext := h.nextCommitTS()
		if !hasNext || !nextTS.EQ(&commitTS) {
			if hasNext && nextTS.LT(&commitTS) {
				return h.fail(mp, data, tombstone, moerr.NewInternalErrorNoCtx("partition change stream is not ordered by commit timestamp"))
			}
			break
		}
	}

	// A tail-done boundary is emitted for each commit timestamp, after every
	// partition has contributed its rows for that timestamp. This keeps the
	// CDC consumer's atomic batch aligned with the logical transaction instead
	// of the partition that happened to be drained first.
	return data, tombstone, engine.ChangesHandle_Tail_done, nil
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
		pending := &h.pending[i]
		if pending.exhausted || pending.data != nil || pending.tombstone != nil {
			continue
		}
		for {
			data, tombstone, hint, err := h.handles[i].Next(ctx, mp)
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
				pending.hint = hint
				break
			}
			pending.exhausted = true
			if err = h.closePartition(i, mp); err != nil {
				return err
			}
			break
		}
	}
	return nil
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
	var next types.TS
	found := false
	for i := range h.pending {
		pending := &h.pending[i]
		if ts, ok := firstCommitTS(pending.data); ok && (!found || ts.LT(&next)) {
			next = ts
			found = true
		}
		if ts, ok := firstCommitTS(pending.tombstone); ok && (!found || ts.LT(&next)) {
			next = ts
			found = true
		}
	}
	return next, found
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

func appendChangesAtCommitTS(dst **batch.Batch, src **batch.Batch, commitTS types.TS, mp *mpool.MPool) (bool, error) {
	if *src == nil {
		return false, nil
	}
	first, _ := firstCommitTS(*src)
	if !first.EQ(&commitTS) {
		return false, nil
	}

	count := 1
	for count < (*src).RowCount() {
		ts, _ := commitTSAt(*src, count)
		if !ts.EQ(&commitTS) {
			break
		}
		count++
	}
	if *dst == nil {
		*dst = newChangeBatch(*src)
	}
	if err := (*dst).UnionWindow(*src, 0, count, mp); err != nil {
		return false, err
	}

	if count == (*src).RowCount() {
		(*src).Clean(mp)
		*src = nil
		return true, nil
	}
	sels := make([]int64, count)
	for i := range sels {
		sels[i] = int64(i)
	}
	(*src).Shrink(sels, true)
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

func firstCommitTS(bat *batch.Batch) (types.TS, bool) {
	return commitTSAt(bat, 0)
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
