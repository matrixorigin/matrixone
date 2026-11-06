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
	"bytes"
	"context"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

type statementKeyView struct {
	// Lookup and Close have one execution owner. Close may wait for a lookup,
	// but transaction rollback only cancels it and never waits under txn.Lock.
	sync.Mutex
	ctx                context.Context
	cancel             context.CancelCauseFunc
	txn                *Transaction
	proc               *process.Process
	snapshot           timestamp.Timestamp
	rollbackCount      int
	tables             map[uint64]*statementKeyTable
	objects            map[string]struct{}
	reservations       []engine.StatementViewReservation
	reserveMemory      func(uint64) (engine.StatementViewReservation, error)
	hashAllocation     *hashtable.AllocationAccountSelection
	iteratorAllocation *hashmap.IteratorAllocation
	vectorAllocation   *vector.AllocationAccountSelection
	registered         bool
	closed             bool
}

type statementKeyTable struct {
	relation                                *txnTableDelegate
	table                                   *txnTable
	def                                     *plan.TableDef
	attrs                                   []string
	colTypes                                []types.Type
	pk                                      int
	state                                   *logtailreplay.PartitionState
	local                                   bool
	entries                                 []Entry
	columnPositions                         [][]int
	selections                              map[*batch.Batch][]int64
	objects                                 []objectio.ObjectStats
	tombstones                              engine.Tombstoner
	cnDeletes                               map[types.Blockid][]int64
	index                                   hashmap.HashMap
	iterator                                hashmap.Iterator
	entryPositions, rowPositions, binaryKey *vector.Vector
	indexed                                 bool
	workspaceProbed                         bool
}

func (tbl *txnTableDelegate) NewStatementKeyView(
	ctx context.Context, relations []engine.Relation, p any, account *mpool.AllocationAccount,
	reserve func(uint64) (engine.StatementViewReservation, error),
) (engine.StatementKeyView, error) {
	if len(relations) == 0 || account == nil || reserve == nil {
		return nil, moerr.NewInternalError(ctx, "invalid statement key view capture")
	}
	proc, ok := p.(*process.Process)
	if !ok || proc == nil {
		return nil, moerr.NewInternalError(ctx, "statement key view requires a process")
	}
	txn := tbl.origin.getTxn()
	viewCtx, cancel := context.WithCancelCause(ctx)
	v := &statementKeyView{ctx: viewCtx, cancel: cancel, txn: txn, proc: proc,
		snapshot: txn.op.SnapshotTS(), tables: make(map[uint64]*statementKeyTable), objects: make(map[string]struct{}), reserveMemory: reserve}
	complete := false
	defer func() {
		if !complete {
			_ = v.Close()
		}
	}()
	if err := txn.engine.registerStatementView(); err != nil {
		return nil, err
	}
	v.registered = true
	var err error
	v.hashAllocation, err = hashtable.NewAllocationAccountSelection(account, mpool.AllocationOwnerMin, 16, 17)
	if err != nil {
		return nil, err
	}
	v.iteratorAllocation, err = hashmap.NewIteratorAllocation(account, mpool.AllocationOwnerMin, 18)
	if err != nil {
		return nil, err
	}
	v.vectorAllocation, err = vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerMin, 19, 20, 21, 22)
	if err != nil {
		return nil, err
	}
	// Relation/partition resolution can execute internal SQL. Do it before the
	// atomic workspace capture and reject any snapshot change across that work.
	for _, relation := range relations {
		if err = statementViewCause(ctx, v); err != nil {
			return nil, err
		}
		d, ok := relation.(*txnTableDelegate)
		if !ok || d.combined.is || d.origin.getTxn() != txn {
			return nil, moerr.NewNotSupported(ctx, "statement key view requires relations from the current transaction")
		}
		id := d.origin.tableId
		if _, exists := v.tables[id]; exists {
			continue
		}
		t := &statementKeyTable{relation: d, table: d.origin, def: d.GetTableDef(ctx),
			selections: make(map[*batch.Batch][]int64), cnDeletes: make(map[types.Blockid][]int64)}
		t.local, err = d.isLocal()
		if err != nil {
			return nil, err
		}
		t.pk = int(t.def.Name2ColIndex[t.def.Pkey.PkeyColName])
		for _, c := range t.def.Cols {
			t.attrs = append(t.attrs, c.Name)
			t.colTypes = append(t.colTypes, vector.ProtoTypeToType(c.Typ))
		}
		t.state, err = t.table.getPartitionStateAt(ctx, v.snapshot)
		if err != nil {
			return nil, err
		}
		v.tables[id] = t
	}
	if err = v.captureWorkspace(ctx, reserve); err != nil {
		return nil, err
	}

	complete = true
	return v, nil
}

// captureWorkspace publishes one immutable target-family capture. Defers release
// both mutexes before the constructor's partial-capture cleanup can run.
func (v *statementKeyView) captureWorkspace(ctx context.Context, reserve func(uint64) (engine.StatementViewReservation, error)) error {
	txn := v.txn
	if err := statementViewCause(ctx, v); err != nil {
		return err
	}
	// The small pin record can outlive this query after rollback. Admit it to
	// the existing engine quota outside transaction/view locks. Count references
	// conservatively; shared pins return unused admission below.
	txn.Lock()
	var objectRefs uint64
	pins := txn.statementKeyPins.Load()
	if pins != nil {
		pins.Lock()
	}
	for _, entry := range txn.writes {
		if table := v.tables[entry.tableId]; table == nil || entry.databaseId != table.table.db.databaseId || entry.bat == nil {
			continue
		}
		if pos := slices.Index(entry.bat.Attrs, catalog.ObjectMeta_ObjectStats); pos >= 0 {
			for i := range entry.bat.Vecs[pos].Length() {
				if pins != nil {
					stats := objectio.ObjectStats(entry.bat.Vecs[pos].GetBytesAt(i))
					if pins.objects[stats.ObjectName().String()] != nil {
						continue
					}
				}
				objectRefs++
			}
		}
	}
	if pins != nil {
		pins.Unlock()
	}
	txn.Unlock()
	if objectRefs > uint64(math.MaxInt64/statementObjectPinQuota) {
		return mpool.ErrAllocationAccountCapacity
	}
	unusedQuota := int64(objectRefs) * statementObjectPinQuota
	if unusedQuota != 0 {
		if _, ok := txn.engine.AcquireQuota(unusedQuota); !ok {
			return mpool.ErrAllocationAccountCapacity
		}
		defer func() { txn.engine.ReleaseQuota(unusedQuota) }()
	}
	txn.Lock()
	defer txn.Unlock()
	if txn.removed || !txn.op.SnapshotTS().Equal(v.snapshot) {
		return moerr.NewTxnNeedRetryNoCtx()
	}
	v.rollbackCount = txn.rollbackCount
	// Retained references and copied metadata share the query's existing budget.
	// The reservation also covers detached batches until the last lookup closes.
	var retained uint64
	overflow := false
	add := func(count, size uint64) {
		if size != 0 && count > (math.MaxUint64-retained)/size {
			overflow = true
			return
		}
		retained += count * size
	}
	for _, t := range v.tables {
		add(1, uint64(reflect.TypeFor[statementKeyTable]().Size()))
		for _, e := range txn.writes {
			if e.tableId != t.table.tableId || e.databaseId != t.table.db.databaseId || e.bat == nil {
				continue
			}
			add(1, uint64(max(e.bat.Allocated(), e.bat.Size())))
			add(1, uint64(reflect.TypeFor[Entry]().Size())+256)
			add(uint64(len(t.attrs)), 8)
			add(uint64(len(txn.batchSelectList[e.bat])), 8+types.RowidSize)
			if e.typ == DELETE && e.fileName == "" {
				add(uint64(e.bat.RowCount()), types.RowidSize)
			}
			if pos := slices.Index(e.bat.Attrs, catalog.ObjectMeta_ObjectStats); pos >= 0 {
				// Copied stats plus object-name/reference-map metadata, in
				// addition to the pinned metadata batch itself.
				add(uint64(e.bat.Vecs[pos].Length()), uint64(objectio.ObjectStatsLen+256))
			}
		}
		txn.deletedBlocks.iter(func(bid *types.Blockid, offsets []int64) bool {
			summary := txn.cnObjsSummary[*bid.Object()]
			if summary.dbName == t.table.db.databaseName && summary.tbName == t.table.tableName {
				add(uint64(len(offsets)), 8+types.RowidSize)
				add(1, 128)
			}
			return true
		})
	}
	if overflow {
		return mpool.ErrAllocationAccountCapacity
	}
	reservation, reserveErr := reserve(retained)
	if reserveErr != nil {
		return reserveErr
	}
	if reservation == nil {
		return moerr.NewInternalError(ctx, "nil statement view reservation")
	}
	v.reservations = append(v.reservations, reservation)
	pins = txn.statementKeyPins.Load()
	if pins == nil {
		pins = &statementKeyPins{batches: make(map[*batch.Batch]*statementBatchPin), objects: make(map[string]*statementObjectPin), views: make(map[*statementKeyView]struct{})}
		txn.statementKeyPins.Store(pins)
	}
	pins.Lock()
	defer pins.Unlock()
	pins.views[v] = struct{}{}
	for _, t := range v.tables {
		tomb := readutil.NewEmptyTombstoneData()
		for _, e := range txn.writes {
			if e.tableId != t.table.tableId || e.databaseId != t.table.db.databaseId || e.bat == nil {
				continue
			}
			var columns []int
			if e.typ == INSERT && e.fileName == "" && !e.bat.IsEmpty() {
				columns = make([]int, len(t.attrs))
				for i, name := range t.attrs {
					columns[i] = -1
					for pos, attr := range e.bat.Attrs {
						if strings.EqualFold(name, attr) {
							columns[i] = pos
							break
						}
					}
					pos := columns[i]
					if pos < 0 || pos >= len(e.bat.Vecs) || e.bat.Vecs[pos] == nil {
						return moerr.NewInternalErrorf(ctx, "missing statement workspace column %s.%s", t.def.Name, name)
					}
					actual := e.bat.Vecs[pos].GetType()
					// Workspace UPDATE buffers may widen VARCHAR metadata. The
					// writer has already enforced the schema's value width; copying
					// the bytes into the declared type must preserve that value.
					sameType := actual.Eq(t.colTypes[i])
					if t.colTypes[i].IsVarlen() {
						sameType = actual.Oid == t.colTypes[i].Oid
					}
					if !sameType || actual.TypeSize() != t.colTypes[i].TypeSize() {
						return moerr.NewInternalErrorf(ctx, "invalid statement workspace type %s.%s: %d/%d/%d vs %d/%d/%d", t.def.Name, name, actual.Oid, actual.Width, actual.Scale, t.colTypes[i].Oid, t.colTypes[i].Width, t.colTypes[i].Scale)
					}
				}
			}
			t.entries = append(t.entries, e) // Entry values survive Adjust's reordering.
			t.columnPositions = append(t.columnPositions, columns)
			pin := pins.batches[e.bat]
			if pin == nil {
				pin = &statementBatchPin{}
				pins.batches[e.bat] = pin
			}
			pin.refs++
			sels := slices.Clone(txn.batchSelectList[e.bat])
			t.selections[e.bat] = sels
			if len(sels) > 0 {
				ids := vector.MustFixedColWithTypeCheck[types.Rowid](e.bat.Vecs[0])
				for _, s := range sels {
					_ = tomb.AppendInMemory(ids[s])
				}
			}
			if e.typ == DELETE && e.fileName == "" {
				_ = tomb.AppendInMemory(vector.MustFixedColWithTypeCheck[types.Rowid](e.bat.Vecs[0])...)
			}
			for _, stats := range collectObjectStatsFromEntry(e) {
				name := stats.ObjectName().String()
				if _, exists := v.objects[name]; !exists {
					op := pins.objects[name]
					if op == nil {
						if unusedQuota < statementObjectPinQuota {
							// Workspace objects grew while admission was outside
							// txn.Lock. No uncharged record may be published.
							return moerr.NewTxnNeedRetryNoCtx()
						}
						op = &statementObjectPin{name: name}
						unusedQuota -= statementObjectPinQuota
						pins.objects[name] = op
					}
					v.objects[name] = struct{}{}
					op.refs++
				}
				if e.typ == INSERT {
					t.objects = append(t.objects, stats)
				} else if e.typ == DELETE {
					_ = tomb.AppendFiles(stats)
				}
			}
		}
		txn.deletedBlocks.iter(func(bid *types.Blockid, offsets []int64) bool {
			summary := txn.cnObjsSummary[*bid.Object()]
			if _, exists := t.selections[summary.objBat]; exists {
				copied := slices.Clone(offsets)
				t.cnDeletes[*bid] = copied
				for _, o := range copied {
					_ = tomb.AppendInMemory(types.NewRowid(bid, uint32(o)))
				}
			}
			return true
		})
		tomb.SortInMemory()
		t.tombstones = tomb
	}

	return nil
}

func (v *statementKeyView) validateSnapshot(ctx context.Context) error {
	if err := statementViewCause(ctx, v); err != nil {
		return err
	}
	if v.closed || !v.txn.op.SnapshotTS().Equal(v.snapshot) {
		return moerr.NewTxnNeedRetryNoCtx()
	}
	return nil
}

func (v *statementKeyView) Validate(ctx context.Context) error {
	v.Lock()
	defer v.Unlock()
	if err := v.validateSnapshot(ctx); err != nil {
		return err
	}
	txn := v.txn
	txn.Lock()
	defer txn.Unlock()
	if txn.removed || txn.rollbackCount != v.rollbackCount {
		return moerr.NewTxnNeedRetryNoCtx()
	}
	for _, t := range v.tables {
		if _, dropped := txn.tablesInVain[t.table.tableId]; dropped {
			return moerr.NewTxnNeedRetryNoCtx()
		}
		current := make(map[*batch.Batch]Entry, len(t.entries))
		for _, e := range txn.writes {
			if e.tableId == t.table.tableId && e.databaseId == t.table.db.databaseId && e.bat != nil {
				current[e.bat] = e
			}
		}
		if len(current) != len(t.entries) {
			return moerr.NewTxnNeedRetryNoCtx()
		}
		for _, e := range t.entries {
			now, exists := current[e.bat]
			if !exists || now.typ != e.typ || now.fileName != e.fileName || !slices.Equal(txn.batchSelectList[e.bat], t.selections[e.bat]) {
				return moerr.NewTxnNeedRetryNoCtx()
			}
		}
		changed, observed := false, 0
		txn.deletedBlocks.iter(func(bid *types.Blockid, offsets []int64) bool {
			summary := txn.cnObjsSummary[*bid.Object()]
			if _, tracked := t.selections[summary.objBat]; tracked {
				observed++
				if !slices.Equal(offsets, t.cnDeletes[*bid]) {
					changed = true
					return false
				}
			}
			return true
		})
		if changed || observed != len(t.cnDeletes) {
			return moerr.NewTxnNeedRetryNoCtx()
		}
	}
	return nil
}

func (v *statementKeyView) Lookup(ctx context.Context, tableID uint64, key *vector.Vector, row int) (result *batch.Batch, err error) {
	v.Lock()
	defer v.Unlock()
	defer func() {
		if err != nil {
			err = primaryShardReadError(ctx, err)
			v.cancel(err)
			if result != nil {
				result.Clean(v.proc.Mp())
				result = nil
			}
		}
	}()
	if err = v.validateSnapshot(ctx); err != nil {
		return nil, err
	}
	t := v.tables[tableID]
	if t == nil || key == nil || row < 0 || row >= key.Length() {
		return nil, moerr.NewInternalError(ctx, "invalid statement key lookup")
	}
	if key.IsNull(uint64(row)) {
		return nil, nil
	}
	lookupCtx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(v.ctx, func() { cancel(context.Cause(v.ctx)) })
	defer func() { stop(); cancel(nil) }()
	ctx = lookupCtx
	if result, err = v.lookupWorkspace(ctx, t, key, row); err != nil || result != nil {
		return result, err
	}
	queryKey, err := vector.NewOffHeapVecWithTypeAndAllocation(t.colTypes[t.pk], v.vectorAllocation)
	if err != nil {
		return nil, err
	}
	defer queryKey.Free(v.proc.Mp())
	if key.GetType().Oid != queryKey.GetType().Oid && !(key.GetType().Oid == types.T_uint16 && queryKey.GetType().Oid == types.T_enum) {
		return nil, moerr.NewInternalError(ctx, "statement key lookup type mismatch")
	}
	if err = queryKey.UnionOne(key, int64(row), v.proc.Mp()); err != nil {
		return nil, err
	}
	if uint64(queryKey.Size()) > (math.MaxUint64-2048)/2 {
		return nil, mpool.ErrAllocationAccountCapacity
	}
	temporary, err := v.reserveMemory(2*uint64(queryKey.Size()) + 2048)
	if err != nil {
		return nil, err
	}
	defer temporary.Release()
	expr, err := statementKeyProbe(ctx, t, queryKey)
	if err != nil {
		return nil, err
	}
	reader, rangeReservations, err := v.newReader(lookupCtx, t, expr)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = closeStatementKeyReader(reader, err)
		for _, reservation := range rangeReservations {
			reservation.Release()
		}
		if err != nil && result != nil {
			result.Clean(v.proc.Mp())
			result = nil
		}
	}()
	b, err := v.newBatch(t)
	if err != nil {
		return nil, err
	}
	defer b.Clean(v.proc.Mp())
	for {
		if err = v.validateSnapshot(lookupCtx); err != nil {
			return nil, err
		}
		b.CleanOnlyData()
		end, readErr := reader.Read(lookupCtx, t.attrs, expr, v.proc.Mp(), b)
		if readErr != nil {
			return nil, readErr
		}
		if remote, ok := reader.(*shardingLocalReader); ok && remote.iteratePhase == InRemote {
			if err = v.applyRemoteDeletes(lookupCtx, t, b); err != nil {
				return nil, err
			}
		}
		for i := 0; i < b.RowCount(); i++ {
			if sameStatementKey(b.Vecs[t.pk], i, queryKey, 0) {
				result, err = v.copyRow(t, b, i, nil)
				return result, err
			}
		}
		if end {
			return nil, nil
		}
	}
}

// Keep the original typed read/cancellation error: transaction retry dispatch
// inspects *moerr.Error directly. A secondary close failure is diagnostic only.
func closeStatementKeyReader(reader engine.Reader, primary error) error {
	closeErr := reader.Close()
	if primary == nil {
		return closeErr
	}
	if closeErr != nil {
		logutil.Warn("statement key view reader close failed", zap.Error(closeErr))
	}
	return primary
}

func sameStatementKey(a *vector.Vector, ar int, b *vector.Vector, br int) bool {
	if a.IsNull(uint64(ar)) || b.IsNull(uint64(br)) {
		return false
	}
	return bytes.Equal(a.GetRawBytesAt(ar), b.GetRawBytesAt(br))
}

// statementKeyProbe narrows candidates only. The caller always applies
// sameStatementKey, including exact physical PK bits, to every returned row.
func statementKeyProbe(ctx context.Context, table *statementKeyTable, key *vector.Vector) (*plan.Expr, error) {
	column := func() *plan.Expr {
		return readutil.NewColumnExpr(table.pk, table.def.Cols[table.pk].Typ, table.def.Pkey.PkeyColName)
	}
	closedRange := func(lower, upper []byte) (*plan.Expr, error) {
		bound := func(value []byte) *plan.Expr {
			return &plan.Expr{Typ: table.def.Cols[table.pk].Typ, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{
				IsConst: true, Data: append([]byte(nil), value...),
			}}}
		}
		ge, err := plan2.BindFuncExprImplByPlanExpr(ctx, ">=", []*plan.Expr{column(), bound(lower)})
		if err != nil {
			return nil, err
		}
		le, err := plan2.BindFuncExprImplByPlanExpr(ctx, "<=", []*plan.Expr{column(), bound(upper)})
		if err != nil {
			return nil, err
		}
		return plan2.BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{ge, le})
	}
	switch key.GetType().Oid {
	case types.T_float32:
		value := vector.GetFixedAtNoTypeCheck[float32](key, 0)
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, nil
		}
		if value == 0 {
			lower, upper := math.Float32frombits(1<<31), float32(0)
			return closedRange(types.EncodeFloat32(&lower), types.EncodeFloat32(&upper))
		}
	case types.T_float64:
		value := vector.GetFixedAtNoTypeCheck[float64](key, 0)
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, nil
		}
		if value == 0 {
			// IN normalization merges signed zero before packed-memory
			// filtering. The closed packed interval retains both encodings.
			lower, upper := math.Copysign(0, -1), float64(0)
			return closedRange(types.EncodeFloat64(&lower), types.EncodeFloat64(&upper))
		}
	}
	data, err := key.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return plan2.MakeInExpr(ctx, column(), int32(key.Length()), data, false), nil
}

func (v *statementKeyView) copyRow(t *statementKeyTable, b *batch.Batch, row int, columns []int) (*batch.Batch, error) {
	out, err := v.newBatch(t)
	if err != nil {
		return nil, err
	}
	for i := range t.colTypes {
		col := i
		if columns != nil {
			col = columns[i]
		}
		if col < 0 {
			out.Clean(v.proc.Mp())
			return nil, moerr.NewInternalError(v.ctx, "missing statement original column")
		}
		err = out.Vecs[i].UnionOne(b.Vecs[col], int64(row), v.proc.Mp())
		if err != nil {
			out.Clean(v.proc.Mp())
			return nil, err
		}
	}
	out.SetRowCount(1)
	return out, nil
}

func (v *statementKeyView) newBatch(t *statementKeyTable) (*batch.Batch, error) {
	b := batch.NewOffHeapWithSize(len(t.colTypes))
	if err := b.SetAllocationAccount(v.vectorAllocation); err != nil {
		return nil, err
	}
	b.Attrs = t.attrs
	for i, typ := range t.colTypes {
		var err error
		b.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(typ, v.vectorAllocation)
		if err != nil {
			b.Clean(v.proc.Mp())
			return nil, err
		}
	}
	return b, nil
}

func (v *statementKeyView) lookupWorkspace(ctx context.Context, t *statementKeyTable, key *vector.Vector, row int) (*batch.Batch, error) {
	if !t.indexed {
		if !t.workspaceProbed {
			t.workspaceProbed = true
			for entry, e := range t.entries {
				if e.typ != INSERT || e.fileName != "" || e.bat.IsEmpty() {
					continue
				}
				pk := e.bat.Vecs[t.columnPositions[entry][t.pk]]
				ids := vector.MustFixedColWithTypeCheck[types.Rowid](e.bat.Vecs[0])
				for i, id := range ids {
					if i%hashmap.UnitLimit == 0 {
						if err := statementViewCause(ctx, v); err != nil {
							return nil, err
						}
					}
					offset := [1]int64{int64(id.GetRowOffset())}
					if sameStatementKey(pk, i, key, row) && len(t.tombstones.ApplyInMemTombstones(id.BorrowBlockID(), offset[:], nil)) != 0 {
						return v.copyRow(t, e.bat, i, t.columnPositions[entry])
					}
				}
			}
			return nil, nil
		}
		if err := v.indexWorkspace(ctx, t); err != nil {
			return nil, err
		}
	}
	return v.lookupIndex(t, key, row)
}

func (v *statementKeyView) lookupIndex(t *statementKeyTable, key *vector.Vector, row int) (*batch.Batch, error) {
	if t.iterator == nil {
		return nil, nil
	}
	t.binaryKey.CleanOnlyData()
	if err := vector.AppendBytes(t.binaryKey, key.GetRawBytesAt(row), false, v.proc.Mp()); err != nil {
		return nil, err
	}
	key, row = t.binaryKey, 0
	groups, valid, err := t.iterator.Find(row, 1, []*vector.Vector{key})
	if err != nil {
		return nil, err
	}
	if valid[0] == 0 || groups[0] == 0 {
		return nil, nil
	}
	group := int(groups[0] - 1)
	entry := vector.GetFixedAtNoTypeCheck[int64](t.entryPositions, group)
	at := vector.GetFixedAtNoTypeCheck[int64](t.rowPositions, group)
	return v.copyRow(t, t.entries[entry].bat, int(at), t.columnPositions[entry])
}

func (v *statementKeyView) indexWorkspace(ctx context.Context, t *statementKeyTable) error {
	// Promote once from the same immutable capture. Failure is terminal; there
	// is no unaccounted map or O(actions*workspace) rescan fallback.
	for entry, e := range t.entries {
		if e.typ != INSERT || e.fileName != "" || e.bat.IsEmpty() {
			continue
		}
		pk := t.columnPositions[entry][t.pk]
		if pk < 0 {
			return moerr.NewInternalError(ctx, "missing workspace primary key")
		}
		keys := e.bat.Vecs[pk]
		rowids := vector.MustFixedColWithTypeCheck[types.Rowid](e.bat.Vecs[0])
		for i := 0; i < e.bat.RowCount(); i++ {
			if i%hashmap.UnitLimit == 0 {
				if err := statementViewCause(ctx, v); err != nil {
					return err
				}
			}
			id := rowids[i]
			offsets := [1]int64{int64(id.GetRowOffset())}
			if len(t.tombstones.ApplyInMemTombstones(id.BorrowBlockID(), offsets[:], nil)) == 0 || keys.IsNull(uint64(i)) {
				continue
			}
			if err := v.indexRow(ctx, t, keys, i, int64(entry), int64(i)); err != nil {
				return err
			}
		}
	}
	t.indexed = true
	return nil
}

func (v *statementKeyView) indexRow(ctx context.Context, t *statementKeyTable, keys *vector.Vector, row int, entry, at int64) error {
	if t.index == nil {
		h, err := hashmap.NewStrHashMapWithAllocations(false, v.proc.Mp(), v.hashAllocation, v.iteratorAllocation)
		if err != nil {
			return err
		}
		t.index, t.iterator = h, h.NewIterator()
		t.entryPositions, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), v.vectorAllocation)
		if err != nil {
			return err
		}
		t.rowPositions, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), v.vectorAllocation)
		if err != nil {
			return err
		}
		t.binaryKey, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_varbinary.ToType(), v.vectorAllocation)
		if err != nil {
			return err
		}
	}
	t.binaryKey.CleanOnlyData()
	if err := vector.AppendBytes(t.binaryKey, keys.GetRawBytesAt(row), false, v.proc.Mp()); err != nil {
		return err
	}
	keys, row = t.binaryKey, 0
	fresh, err := t.iterator.DetectDup([]*vector.Vector{keys}, row)
	if err != nil {
		return err
	}
	if !fresh {
		return moerr.NewInternalError(ctx, "duplicate primary key in statement prior data")
	}
	if err = vector.AppendFixed(t.entryPositions, entry, false, v.proc.Mp()); err != nil {
		return err
	}
	return vector.AppendFixed(t.rowPositions, at, false, v.proc.Mp())
}

// statementKeyRanges owns one selected block collector and its query charge.
// A new capacity is admitted while the old charge still owns the copy source.
// The factor covers allocator rounding; remote wire storage is admitted separately.
type statementKeyRanges struct {
	blocks      objectio.BlockInfoSlice
	reservation engine.StatementViewReservation
}

func (r *statementKeyRanges) append(ctx context.Context, reserve func(uint64) (engine.StatementViewReservation, error), out *objectio.BlockInfoSlice, block *objectio.BlockInfo) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if len(*out) > math.MaxInt-objectio.BlockInfoSize {
		return mpool.ErrAllocationAccountCapacity
	}
	required := len(*out) + objectio.BlockInfoSize
	if required > cap(*out) {
		capacity := max(2*objectio.BlockInfoSize, required)
		if cap(*out) <= math.MaxInt/2 {
			capacity = max(capacity, cap(*out)*2)
		}
		if uint64(capacity) > (math.MaxUint64-128)/2 {
			return mpool.ErrAllocationAccountCapacity
		}
		next, err := reserve(uint64(capacity)*2 + 128)
		if err != nil {
			return err
		}
		if next == nil {
			return moerr.NewInternalError(ctx, "missing statement range reservation")
		}
		owned := make(objectio.BlockInfoSlice, len(*out), capacity)
		copy(owned, *out)
		previous := r.reservation
		*out, r.reservation = owned, next
		if previous != nil {
			previous.Release()
		}
	}
	out.AppendBlockInfo(block)
	return nil
}

func (r *statementKeyRanges) release() {
	r.blocks = nil
	if r.reservation != nil {
		r.reservation.Release()
		r.reservation = nil
	}
}

func (v *statementKeyView) newReader(ctx context.Context, t *statementKeyTable, expr *plan.Expr) (result engine.Reader, reservations []engine.StatementViewReservation, err error) {
	local := statementKeyRanges{}
	var reader engine.Reader
	defer func() {
		if err != nil {
			if reader != nil {
				_ = closeStatementKeyReader(reader, err)
			}
			local.release()
			for _, r := range reservations {
				r.Release()
			}
			reservations = nil
		}
	}()
	appendBlock := func(out *objectio.BlockInfoSlice, block *objectio.BlockInfo) error {
		return local.append(ctx, v.reserveMemory, out, block)
	}
	if err = appendBlock(&local.blocks, &objectio.EmptyBlockInfo); err != nil {
		return nil, nil, err
	}
	state := t.state
	category := engine.GeneralLocalDataSource
	if !t.local {
		state = nil
		category = engine.ShardingLocalDataSource
	}
	var filters []*plan.Expr
	if expr != nil {
		filters = []*plan.Expr{expr}
	}
	if err = t.table.rangesOnePart(ctx, state, v.snapshot, t.def, engine.RangesParam{BlockFilters: filters, BlockAppender: appendBlock}, &local.blocks, v.proc, t.objects); err != nil {
		return nil, nil, err
	}
	policy := engine.TombstoneApplyPolicy(engine.Policy_CheckCommittedOnly)
	if !t.local {
		policy |= engine.Policy_SkipCommittedInMemory | engine.Policy_SkipCommittedS3
	}
	var ds *LocalDisttaeDataSource
	ds, err = NewLocalDataSource(ctx, t.table, types.TimestampToTS(v.snapshot), 0, t.state, local.blocks, t.tombstones, false, policy, category)
	if err != nil {
		return nil, nil, err
	}
	ds.statementView = true
	reader, err = readutil.NewReader(ctx, v.proc.Mp(), v.txn.engine.packerPool, v.txn.engine.fs, t.def, v.snapshot, expr, ds, readutil.GetThresholdForReader(1), engine.FilterHint{})
	if err != nil {
		ds.Close()
		return nil, nil, err
	}
	result = reader
	if !t.local {
		var remoteRanges engine.RelData
		remoteRanges, reservations, err = v.loadCommittedRanges(ctx, t, expr)
		if err != nil {
			return nil, nil, err
		}
		result = &shardingLocalReader{lrd: reader, tblDelegate: t.relation, remoteRelData: remoteRanges, remoteTombApplyPolicy: engine.Policy_CheckCommittedOnly}
	}
	if err = v.validateSnapshot(ctx); err != nil {
		return nil, reservations, err
	}
	reservations = append(reservations, local.reservation)
	local.reservation = nil // The reader lifetime now owns the selected slice.
	return result, reservations, nil
}

func (v *statementKeyView) loadCommittedRanges(ctx context.Context, t *statementKeyTable, expr *plan.Expr) (result engine.RelData, reservations []engine.StatementViewReservation, err error) {
	selected := statementKeyRanges{}
	defer func() {
		if err != nil {
			selected.release()
		}
	}()
	err = t.relation.forwardRead(ctx, shardservice.ReadRanges, func(request *shardservice.ReadRequest) {
		request.Param.RangesParam.DataCollectPolicy = engine.Policy_CollectCommittedData
		request.Param.RangesParam.PreAllocSize = 2
		if expr != nil {
			request.Param.RangesParam.Exprs = []*plan.Expr{expr}
		}
	}, func(response []byte) error {
		borrowed, decodeErr := readutil.UnmarshalRelationData(response)
		if decodeErr != nil {
			return decodeErr
		}
		for i := 0; i < borrowed.DataCnt(); i++ {
			block := borrowed.GetBlockInfo(i)
			if decodeErr = selected.append(ctx, v.reserveMemory, &selected.blocks, &block); decodeErr != nil {
				return decodeErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, primaryShardReadError(ctx, err)
	}
	if err = v.validateSnapshot(ctx); err != nil {
		return nil, nil, err
	}
	if len(selected.blocks) != 0 {
		var size int
		size, err = readutil.BlockListEncodedSize(len(selected.blocks))
		if err != nil {
			return nil, nil, err
		}
		var wire engine.StatementViewReservation
		wire, err = v.reserveMemory(uint64(size)*2 + 128)
		if err != nil {
			return nil, nil, err
		}
		if wire == nil {
			return nil, nil, moerr.NewInternalError(ctx, "missing statement range wire reservation")
		}
		// ReadBuildReader serializes this list after the local reader drains.
		// Keep its allocation-rounding allowance through reader close as well.
		reservations = append(reservations, wire)
	}
	data := readutil.NewBlockListRelationData(0)
	data.SetBlockList(selected.blocks)
	if selected.reservation != nil {
		reservations = append(reservations, selected.reservation)
		selected.reservation = nil
	}
	return data, reservations, nil
}

func (v *statementKeyView) applyRemoteDeletes(ctx context.Context, t *statementKeyTable, b *batch.Batch) error {
	if b.IsEmpty() {
		return nil
	}
	ids := vector.MustFixedColWithTypeCheck[types.Rowid](b.Vecs[t.def.Name2ColIndex[catalog.Row_ID]])
	keep := vector.GetSels()
	defer func() { vector.PutSels(keep) }()
	mask := objectio.GetReusableBitmap()
	defer mask.Release()
	ts := types.TimestampToTS(v.snapshot)
	for start := 0; start < len(ids); {
		bid := ids[start].CloneBlockID()
		end := start + 1
		for end < len(ids) && ids[end].CloneBlockID() == bid {
			end++
		}
		mask.Clear()
		t.tombstones.ApplyInMemTombstones(&bid, nil, &mask)
		if _, err := t.tombstones.ApplyPersistedTombstones(ctx, v.txn.engine.fs, &ts, &bid, nil, &mask); err != nil {
			return err
		}
		for i := start; i < end; i++ {
			if !mask.Contains(uint64(ids[i].GetRowOffset())) {
				keep = append(keep, int64(i))
			}
		}
		start = end
	}
	b.Shrink(keep, false)
	return nil
}

func (v *statementKeyView) Close() (err error) {
	if v == nil {
		return nil
	}
	v.cancel(context.Canceled)
	v.Lock()
	if v.closed {
		v.Unlock()
		return nil
	}
	defer v.Unlock()
	v.closed = true

	for _, t := range v.tables {
		hashmap.IteratorClearOwner(t.iterator)
		t.iterator = nil
		if t.index != nil {
			t.index.Free()
			t.index = nil
		}
		for _, vec := range []*vector.Vector{t.entryPositions, t.rowPositions, t.binaryKey} {
			if vec != nil {
				vec.Free(v.proc.Mp())
			}
		}
	}
	// Partial resolution has no pins; partial capture is owned by this handle.
	if pins := v.txn.statementKeyPins.Load(); pins != nil {
		pins.Lock()
		_, captured := pins.views[v]
		pins.Unlock()
		if captured {
			head, tail := v.releasePins()
			v.finishRegistration(head, tail)
			return nil
		}
	}
	v.clearCapture()
	for _, r := range v.reservations {
		r.Release()
	}
	v.reservations = nil
	v.finishRegistration(nil, nil)
	return nil
}

func (v *statementKeyView) finishRegistration(head, tail *statementObjectPin) {
	if v.registered {
		v.registered = false
		v.txn.engine.releaseStatementView(head, tail)
	}
}

func (v *statementKeyView) clearCapture() {
	// A prepared operator may retain a closed handle until rebinding. Drop
	// data-scaled metadata before relinquishing the capture reservation.
	v.tables = nil
	v.objects = nil
	v.hashAllocation = nil
	v.iteratorAllocation = nil
	v.vectorAllocation = nil
	v.reserveMemory = nil
}
