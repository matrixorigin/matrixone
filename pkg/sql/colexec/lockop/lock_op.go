// Copyright 2023 Matrix Origin
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

package lockop

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

func String(v any, buf *bytes.Buffer) {
	arg := v.(*Argument)
	buf.WriteString("lock-op(")
	n := len(arg.targets) - 1
	for idx, target := range arg.targets {
		buf.WriteString(fmt.Sprintf("%d-%d-%d",
			target.tableID,
			target.primaryColumnIndexInBatch,
			target.refreshTimestampIndexInBatch))
		if idx < n {
			buf.WriteString(",")
		}
	}
	buf.WriteString(")")
}

func Prepare(proc *process.Process, v any) error {
	arg := v.(*Argument)
	arg.rt = &state{}
	arg.rt.fetchers = make([]FetchLockRowsFunc, 0, len(arg.targets))
	for idx := range arg.targets {
		arg.rt.fetchers = append(arg.rt.fetchers,
			GetFetchRowsFunc(arg.targets[idx].primaryColumnType))
	}
	arg.rt.parker = types.NewPacker(proc.Mp())
	arg.rt.retryError = nil
	arg.rt.step = stepLock
	if arg.block {
		arg.rt.InitReceiver(proc, true)
	}
	return nil
}

// Call the lock op is used to add locks into lockservice of the Table operated by the
// current transaction under a pessimistic transaction.
//
// In RC's transaction mode, after successful locking, if an accessed data is found to be
// concurrently modified by other transactions, a Timestamp column will be put on the output
// vectors for querying the latest data, and subsequent op needs to check this column to check
// whether the latest data needs to be read.
func Call(
	idx int,
	proc *process.Process,
	v any,
	isFirst bool,
	isLast bool) (process.ExecStatus, error) {
	arg, ok := v.(*Argument)
	if !ok {
		getLogger().Fatal("invalid argument",
			zap.Any("argument", arg))
	}

	txnOp := proc.TxnOperator
	if !txnOp.Txn().IsPessimistic() {
		return process.ExecNext, nil
	}

	if !arg.block {
		ok, err := callNonBlocking(idx, proc, arg)
		if ok {
			return process.ExecStop, err
		}
		return process.ExecNext, err
	}
	ok, err := callBlocking(idx, proc, arg, isFirst, isLast)
	if ok {
		return process.ExecStop, err
	}
	return process.ExecNext, err
}

func callNonBlocking(
	idx int,
	proc *process.Process,
	arg *Argument) (bool, error) {
	bat := proc.InputBatch()

	if bat == nil {
		return true, arg.rt.retryError
	}
	if bat.RowCount() == 0 {
		bat.Clean(proc.Mp())
		proc.SetInputBatch(batch.EmptyBatch)
		return false, nil
	}

	if err := performLock(bat, proc, arg); err != nil {
		bat.Clean(proc.Mp())
		return true, err
	}
	return false, nil
}

func callBlocking(
	idx int,
	proc *process.Process,
	arg *Argument,
	isFirst bool,
	isLast bool) (bool, error) {

	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	proc.SetInputBatch(batch.EmptyBatch)

	switch arg.rt.step {
	case stepLock:
		bat, err := arg.getBatch(proc, anal, isFirst)
		if err != nil {
			return false, err
		}

		// no input batch any more, means all lock performed.
		if bat == nil {
			arg.rt.step = stepDownstream
			if len(arg.rt.cachedBatches) == 0 {
				arg.rt.step = stepEnd
			}
			return false, nil
		}

		// skip empty batch
		if bat.RowCount() == 0 {
			bat.Clean(proc.Mp())
			return false, nil
		}

		if err := performLock(bat, proc, arg); err != nil {
			bat.Clean(proc.Mp())
			return false, err
		}
		// blocking lock node. Never pass the input batch into downstream operators before
		// all lock are performed.
		arg.rt.cachedBatches = append(arg.rt.cachedBatches, bat)
		return false, nil
	case stepDownstream:
		if arg.rt.retryError != nil {
			arg.rt.step = stepEnd
			return false, nil
		}

		bat := arg.rt.cachedBatches[0]
		arg.rt.cachedBatches = arg.rt.cachedBatches[1:]
		if len(arg.rt.cachedBatches) == 0 {
			arg.rt.step = stepEnd
		}

		proc.SetInputBatch(bat)
		return false, nil
	case stepEnd:
		arg.cleanCachedBatch(proc)
		proc.SetInputBatch(nil)
		return true, arg.rt.retryError
	default:
		panic("BUG")
	}
}

func performLock(
	bat *batch.Batch,
	proc *process.Process,
	arg *Argument) error {
	txnFeature := proc.TxnClient.(client.TxnClientWithFeature)
	needRetry := false
	for idx, target := range arg.targets {
		getLogger().Debug("lock",
			zap.Uint64("table", target.tableID),
			zap.Bool("filter", target.filter != nil),
			zap.Int32("filter-col", target.filterColIndexInBatch),
			zap.Int32("primary-index", target.primaryColumnIndexInBatch))
		var filterCols []int32
		priVec := bat.GetVector(target.primaryColumnIndexInBatch)
		if target.filter != nil {
			filterCols = vector.MustFixedCol[int32](bat.GetVector(target.filterColIndexInBatch))
		}
		locked, refreshTS, err := doLock(
			proc.Ctx,
			arg.block,
			arg.engine,
			target.tableID,
			proc,
			priVec,
			target.primaryColumnType,
			DefaultLockOptions(arg.rt.parker).
				WithLockMode(lock.LockMode_Exclusive).
				WithFetchLockRowsFunc(arg.rt.fetchers[idx]).
				WithMaxBytesPerLock(int(proc.LockService.GetConfig().MaxLockRowCount)).
				WithFilterRows(target.filter, filterCols).
				WithLockTable(target.lockTable).
				WithHasNewVersionInRangeFunc(arg.rt.hasNewVersionInRange),
		)
		if getLogger().Enabled(zap.DebugLevel) {
			getLogger().Debug("lock result",
				zap.Uint64("table", target.tableID),
				zap.Bool("locked", locked),
				zap.Int32("primary-index", target.primaryColumnIndexInBatch),
				zap.String("refresh-ts", refreshTS.DebugString()),
				zap.Error(err))
		}
		if err != nil {
			return err
		}
		if !locked {
			continue
		}

		// refreshTS is last commit ts + 1, because we need see the committed data.
		if txnFeature.RefreshExpressionEnabled() &&
			target.refreshTimestampIndexInBatch != -1 {
			vec := bat.GetVector(target.refreshTimestampIndexInBatch)
			ts := types.BuildTS(refreshTS.PhysicalTime, refreshTS.LogicalTime)
			n := priVec.Length()
			for i := 0; i < n; i++ {
				vector.AppendFixed(vec, ts, false, proc.Mp())
			}
			continue
		}

		// if need to retry, do not return the retry error immediately, first try to get all
		// the locks to avoid another conflict when retrying
		if !needRetry && !refreshTS.IsEmpty() {
			needRetry = true
		}
	}
	// when a transaction needs to operate on many data, there may be multiple conflicts on the
	// data, and if you go to retry every time a conflict occurs, you will also encounter conflicts
	// when you retry. We need to return the conflict after all the locks have been added successfully,
	// so that the retry will definitely succeed because all the locks have been put.
	if needRetry && arg.rt.retryError == nil {
		arg.rt.retryError = moerr.NewTxnNeedRetry(proc.Ctx)
	}
	return nil
}

// LockTable lock table, all rows in the table will be locked, and wait current txn
// closed.
func LockTable(
	eng engine.Engine,
	proc *process.Process,
	tableID uint64,
	pkType types.Type) error {
	if !proc.TxnOperator.Txn().IsPessimistic() {
		return nil
	}
	parker := types.NewPacker(proc.Mp())
	defer parker.FreeMem()

	opts := DefaultLockOptions(parker).
		WithLockTable(true).
		WithFetchLockRowsFunc(GetFetchRowsFunc(pkType))
	_, refreshTS, err := doLock(
		proc.Ctx,
		false,
		eng,
		tableID,
		proc,
		nil,
		pkType,
		opts)
	if err != nil {
		return err
	}
	// If the returned timestamp is not empty, we should return a retry error,
	if !refreshTS.IsEmpty() {
		return moerr.NewTxnNeedRetry(proc.Ctx)
	}
	return nil
}

// LockRow lock rows in table, rows will be locked, and wait current txn closed.
func LockRows(
	eng engine.Engine,
	proc *process.Process,
	tableID uint64,
	vec *vector.Vector,
	pkType types.Type,
) error {
	if !proc.TxnOperator.Txn().IsPessimistic() {
		return nil
	}

	parker := types.NewPacker(proc.Mp())
	defer parker.FreeMem()

	opts := DefaultLockOptions(parker).
		WithLockTable(false).
		WithFetchLockRowsFunc(GetFetchRowsFunc(pkType))
	_, refreshTS, err := doLock(
		proc.Ctx,
		false,
		eng,
		tableID,
		proc,
		vec,
		pkType,
		opts)
	if err != nil {
		return err
	}
	// If the returned timestamp is not empty, we should return a retry error,
	if !refreshTS.IsEmpty() {
		return moerr.NewTxnNeedRetry(proc.Ctx)
	}
	return nil
}

// doLock locks a set of data so that no other transaction can modify it.
// The data is described by the primary key. When the returned timestamp.IsEmpty
// is false, it means there is a conflict with other transactions and the data to
// be manipulated has been modified, you need to get the latest data at timestamp.
func doLock(
	ctx context.Context,
	blocking bool,
	eng engine.Engine,
	tableID uint64,
	proc *process.Process,
	vec *vector.Vector,
	pkType types.Type,
	opts LockOptions) (bool, timestamp.Timestamp, error) {
	txnOp := proc.TxnOperator
	txnClient := proc.TxnClient
	lockService := proc.LockService

	if !txnOp.Txn().IsPessimistic() {
		return false, timestamp.Timestamp{}, nil
	}

	if opts.maxCountPerLock == 0 {
		opts.maxCountPerLock = int(lockService.GetConfig().MaxLockRowCount)
	}
	fetchFunc := opts.fetchFunc
	if fetchFunc == nil {
		fetchFunc = GetFetchRowsFunc(pkType)
	}

	has, rows, g := fetchFunc(
		vec,
		opts.parker,
		pkType,
		opts.maxCountPerLock,
		opts.lockTable,
		opts.filter,
		opts.filterCols)
	if !has {
		return false, timestamp.Timestamp{}, nil
	}

	txn := txnOp.Txn()
	options := lock.LockOptions{
		Granularity: g,
		Policy:      lock.WaitPolicy_Wait,
		Mode:        opts.mode,
	}
	if txn.Mirror {
		options.ForwardTo = txn.LockService
		if options.ForwardTo == "" {
			panic("forward to empty lock service")
		}
	} else {
		// FIXME: in launch model, multi-cn will use same process level runtime. So lockservice will be wrong.
		if txn.LockService != lockService.GetConfig().ServiceID {
			lockService = lockservice.GetLockServiceByServiceID(txn.LockService)
		}
	}

	result, err := lockService.Lock(
		ctx,
		tableID,
		rows,
		txn.ID,
		options)
	if err != nil {
		return false, timestamp.Timestamp{}, err
	}

	// add bind locks
	if err := txnOp.AddLockTable(result.LockedOn); err != nil {
		return false, timestamp.Timestamp{}, err
	}

	snapshotTS := txnOp.Txn().SnapshotTS
	// if has no conflict, lockedTS means the latest commit ts of this table
	lockedTS := result.Timestamp

	// if no conflict, maybe data has been updated in [snapshotTS, lockedTS]. So wen need check here
	if !result.HasConflict &&
		snapshotTS.LessEq(lockedTS) && // only retry when snapshotTS <= lockedTS, means lost some update in rc mode.
		!txnOp.IsRetry() &&
		txnOp.Txn().IsRCIsolation() {

		// wait last committed logtail applied
		newSnapshotTS, err := txnClient.WaitLogTailAppliedAt(ctx, lockedTS)
		if err != nil {
			return false, timestamp.Timestamp{}, err
		}

		fn := opts.hasNewVersionInRangeFunc
		if fn == nil {
			fn = hasNewVersionInRange
		}

		// if [snapshotTS, lockedTS] has been modified, need retry at new snapshot ts
		changed, err := fn(proc, tableID, eng, vec, snapshotTS.Prev(), lockedTS)
		if err != nil {
			return false, timestamp.Timestamp{}, err
		}
		if changed {
			if err := txnOp.UpdateSnapshot(ctx, newSnapshotTS); err != nil {
				return false, timestamp.Timestamp{}, err
			}
			return true, newSnapshotTS, nil
		}
	}

	// no conflict or has conflict, but all prev txn all aborted
	// current txn can read and write normally
	if !result.HasConflict ||
		!result.HasPrevCommit {
		return true, timestamp.Timestamp{}, nil
	}

	// Arriving here means that at least one of the conflicting
	// transactions has committed.
	//
	// For the RC schema we need some retries between
	// [txn.snapshot ts, prev.commit ts] (de-duplication for insert, re-query for
	// update and delete).
	//
	// For the SI schema the current transaction needs to be abort (TODO: later
	// we can consider recording the ReadSet of the transaction and check if data
	// is modified between [snapshotTS,prev.commits] and raise the SnapshotTS of
	// the SI transaction to eliminate conflicts)
	if !txnOp.Txn().IsRCIsolation() {
		return false, timestamp.Timestamp{}, moerr.NewTxnWWConflict(ctx)
	}

	// forward rc's snapshot ts
	snapshotTS = result.Timestamp.Next()
	if err := txnOp.UpdateSnapshot(ctx, snapshotTS); err != nil {
		return false, timestamp.Timestamp{}, err
	}
	return true, snapshotTS, nil
}

// DefaultLockOptions create a default lock operation. The parker is used to
// encode primary key into lock row.
func DefaultLockOptions(parker *types.Packer) LockOptions {
	return LockOptions{
		mode:            lock.LockMode_Exclusive,
		lockTable:       false,
		maxCountPerLock: 0,
		parker:          parker,
	}
}

// WithLockMode set lock mode, Exclusive or Shared
func (opts LockOptions) WithLockMode(mode lock.LockMode) LockOptions {
	opts.mode = mode
	return opts
}

// WithLockTable set lock all table
func (opts LockOptions) WithLockTable(lockTable bool) LockOptions {
	opts.lockTable = lockTable
	return opts
}

// WithMaxBytesPerLock every lock operation, will add some lock rows into
// lockservice. If very many rows of data are added at once, this can result
// in an excessive memory footprint. This value limits the amount of lock memory
// that can be allocated per lock operation, and if it is exceeded, it will be
// converted to a range lock.
func (opts LockOptions) WithMaxBytesPerLock(maxBytesPerLock int) LockOptions {
	opts.maxCountPerLock = maxBytesPerLock
	return opts
}

// WithFetchLockRowsFunc set the primary key into lock rows conversion function.
func (opts LockOptions) WithFetchLockRowsFunc(fetchFunc FetchLockRowsFunc) LockOptions {
	opts.fetchFunc = fetchFunc
	return opts
}

// WithFilterRows set filter rows, filterCols used to rowsFilter func
func (opts LockOptions) WithFilterRows(
	filter RowsFilter,
	filterCols []int32) LockOptions {
	opts.filter = filter
	opts.filterCols = filterCols
	return opts
}

// WithHasNewVersionInRangeFunc setup hasNewVersionInRange func
func (opts LockOptions) WithHasNewVersionInRangeFunc(fn hasNewVersionInRangeFunc) LockOptions {
	opts.hasNewVersionInRangeFunc = fn
	return opts
}

// NewArgument create new lock op argument.
func NewArgument(engine engine.Engine) *Argument {
	return &Argument{
		engine: engine,
	}
}

// Block return if lock operator is a blocked node.
func (arg *Argument) Block() bool {
	return arg.block
}

// SetBlock set the lock op is blocked. If true lock op will block the current pipeline, and cache
// all input batches. And wait for all the input's batch to be locked before outputting the cached batch
// to the downstream operator. E.g. select for update, only we get all lock result, then select can be
// performed, otherwise, if we need retry in RC mode, we may get wrong result.
func (arg *Argument) SetBlock(block bool) *Argument {
	arg.block = block
	return arg
}

// AddLockTarget add lock targets
func (arg *Argument) CopyToPipelineTarget() []*pipeline.LockTarget {
	targets := make([]*pipeline.LockTarget, len(arg.targets))
	for i, target := range arg.targets {
		targets[i] = &pipeline.LockTarget{
			TableId:            target.tableID,
			PrimaryColIdxInBat: target.primaryColumnIndexInBatch,
			PrimaryColTyp:      plan.MakePlan2Type(&target.primaryColumnType),
			RefreshTsIdxInBat:  target.refreshTimestampIndexInBatch,
			FilterColIdxInBat:  target.filterColIndexInBatch,
			LockTable:          target.lockTable,
		}
	}
	return targets
}

// AddLockTarget add lock targets
func (arg *Argument) AddLockTarget(
	tableID uint64,
	primaryColumnIndexInBatch int32,
	primaryColumnType types.Type,
	refreshTimestampIndexInBatch int32) *Argument {
	arg.targets = append(arg.targets, lockTarget{
		tableID:                      tableID,
		primaryColumnIndexInBatch:    primaryColumnIndexInBatch,
		primaryColumnType:            primaryColumnType,
		refreshTimestampIndexInBatch: refreshTimestampIndexInBatch,
	})
	return arg
}

// LockTable lock all table, used for delete, truncate and drop table
func (arg *Argument) LockTable(tableID uint64) *Argument {
	for idx := range arg.targets {
		if arg.targets[idx].tableID == tableID {
			arg.targets[idx].lockTable = true
			break
		}
	}
	return arg
}

// AddLockTargetWithPartition add lock targets for partition tables. Our partitioned table implementation
// has each partition as a separate table. So when modifying data, these rows may belong to different
// partitions. For lock op does not care about the logic of data and partition mapping calculation, the
// caller needs to tell the lock op.
//
// tableIDs: the set of ids of the sub-tables of the partition to which the data of the current operation is
// attributed after calculation.
//
// partitionTableIDMappingInBatch: the ID index of the sub-table corresponding to the data. Index of tableIDs
func (arg *Argument) AddLockTargetWithPartition(
	tableIDs []uint64,
	primaryColumnIndexInBatch int32,
	primaryColumnType types.Type,
	refreshTimestampIndexInBatch int32,
	partitionTableIDMappingInBatch int32) *Argument {
	if len(tableIDs) == 0 {
		panic("invalid partition table ids")
	}

	// only one partition table, process as normal table
	if len(tableIDs) == 1 {
		return arg.AddLockTarget(tableIDs[0],
			primaryColumnIndexInBatch,
			primaryColumnType,
			refreshTimestampIndexInBatch,
		)
	}

	for _, tableID := range tableIDs {
		arg.targets = append(arg.targets, lockTarget{
			tableID:                      tableID,
			primaryColumnIndexInBatch:    primaryColumnIndexInBatch,
			primaryColumnType:            primaryColumnType,
			refreshTimestampIndexInBatch: refreshTimestampIndexInBatch,
			filter:                       getRowsFilter(tableID, tableIDs),
			filterColIndexInBatch:        partitionTableIDMappingInBatch,
		})
	}
	return arg
}

// Free free mem
func (arg *Argument) Free(
	proc *process.Process,
	pipelineFailed bool) {
	if arg.rt == nil {
		return
	}
	if arg.rt.parker != nil {
		arg.rt.parker.FreeMem()
	}
	arg.rt.retryError = nil
	arg.cleanCachedBatch(proc)
	arg.rt.FreeMergeTypeOperator(pipelineFailed)
}

func (arg *Argument) cleanCachedBatch(proc *process.Process) {
	for _, bat := range arg.rt.cachedBatches {
		bat.Clean(proc.Mp())
	}
	arg.rt.cachedBatches = nil
}

func (arg *Argument) getBatch(
	proc *process.Process,
	anal process.Analyze,
	isFirst bool) (*batch.Batch, error) {
	fn := arg.rt.batchFetchFunc
	if fn == nil {
		fn = arg.rt.ReceiveFromAllRegs
	}

	bat, end, err := fn(anal)
	if err != nil {
		return nil, err
	}
	if end {
		return nil, nil
	}
	anal.Input(bat, isFirst)
	return bat, nil
}

func getRowsFilter(
	tableID uint64,
	partitionTables []uint64) RowsFilter {
	return func(
		row int,
		filterCols []int32) bool {
		return partitionTables[filterCols[row]] == tableID
	}
}

// [from, to].
// 1. if has a mvcc record <= from, return false, means no changed
// 2. otherwise return true, changed
func hasNewVersionInRange(
	proc *process.Process,
	tableID uint64,
	eng engine.Engine,
	vec *vector.Vector,
	from, to timestamp.Timestamp) (bool, error) {
	if vec == nil {
		return false, nil
	}
	txnClient := proc.TxnClient
	txnOp, err := txnClient.New(proc.Ctx, to.Prev())
	if err != nil {
		return false, err
	}
	//txnOp is a new transaction, so we need to start a new statement
	txnOp.GetWorkspace().StartStatement()
	defer func() {
		_ = txnOp.Rollback(proc.Ctx)
		txnOp.GetWorkspace().EndStatement()
	}()
	if err := eng.New(proc.Ctx, txnOp); err != nil {
		return false, err
	}

	dbName, tableName, _, err := eng.GetRelationById(proc.Ctx, txnOp, tableID)
	if err != nil {
		if strings.Contains(err.Error(), "can not find table by id") {
			return false, nil
		}
		return false, err
	}
	db, err := eng.Database(proc.Ctx, dbName, txnOp)
	if err != nil {
		return false, err
	}
	rel, err := db.Relation(proc.Ctx, tableName, proc)
	if err != nil {
		return false, err
	}
	if err := txnOp.GetWorkspace().IncrStatementID(proc.Ctx, false); err != nil {
		return false, nil
	}
	_, err = rel.Ranges(proc.Ctx, nil)
	if err != nil {
		return false, err
	}
	fromTS := types.BuildTS(from.PhysicalTime, from.LogicalTime)
	toTS := types.BuildTS(to.PhysicalTime, to.LogicalTime)
	return rel.PrimaryKeysMayBeModified(proc.Ctx, fromTS, toTS, vec)
}
