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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

var (
	retryError               = moerr.NewTxnNeedRetryNoCtx()
	retryWithDefChangedError = moerr.NewTxnNeedRetryWithDefChangedNoCtx()
)

const argName = "lock_op"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": lock-op(")
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

func (arg *Argument) Prepare(proc *process.Process) error {
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
func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	txnOp := proc.TxnOperator
	if !txnOp.Txn().IsPessimistic() {
		return arg.GetChildren(0).Call(proc)
	}

	if !arg.block {
		return callNonBlocking(proc, arg)
	}

	return callBlocking(proc, arg, arg.GetIsFirst(), arg.GetIsLast())
}

func callNonBlocking(
	proc *process.Process,
	arg *Argument) (vm.CallResult, error) {

	result, err := arg.GetChildren(0).Call(proc)
	if err != nil {
		return result, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	if result.Batch == nil {
		return result, arg.rt.retryError
	}
	bat := result.Batch
	if bat.IsEmpty() {
		return result, err
	}

	if err := performLock(bat, proc, arg); err != nil {
		return result, err
	}

	return result, nil
}

func callBlocking(
	proc *process.Process,
	arg *Argument,
	isFirst bool,
	_ bool) (vm.CallResult, error) {

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	result := vm.NewCallResult()
	if arg.rt.step == stepLock {
		for {
			bat, err := arg.getBatch(proc, anal, isFirst)
			if err != nil {
				return result, err
			}

			// no input batch any more, means all lock performed.
			if bat == nil {
				arg.rt.step = stepDownstream
				if len(arg.rt.cachedBatches) == 0 {
					arg.rt.step = stepEnd
				}
				break
			}

			// skip empty batch
			if bat.IsEmpty() {
				continue
			}

			if err := performLock(bat, proc, arg); err != nil {
				return result, err
			}

			// blocking lock node. Never pass the input batch into downstream operators before
			// all lock are performed.
			arg.rt.cachedBatches = append(arg.rt.cachedBatches, bat)
		}
	}

	if arg.rt.step == stepDownstream {
		if arg.rt.retryError != nil {
			arg.rt.step = stepEnd
			return result, arg.rt.retryError
		}

		if len(arg.rt.cachedBatches) == 0 {
			arg.rt.step = stepEnd
		} else {
			bat := arg.rt.cachedBatches[0]
			arg.rt.cachedBatches = arg.rt.cachedBatches[1:]
			result.Batch = bat
			return result, nil
		}
	}

	if arg.rt.step == stepEnd {
		result.Status = vm.ExecStop
		arg.cleanCachedBatch(proc)
		return result, arg.rt.retryError
	}

	panic("BUG")
}

func performLock(
	bat *batch.Batch,
	proc *process.Process,
	arg *Argument) error {
	needRetry := false
	for idx, target := range arg.targets {
		if proc.TxnOperator.LockSkipped(target.tableID, target.mode) {
			return nil
		}
		getLogger().Debug("lock",
			zap.Uint64("table", target.tableID),
			zap.Bool("filter", target.filter != nil),
			zap.Int32("filter-col", target.filterColIndexInBatch),
			zap.Int32("primary-index", target.primaryColumnIndexInBatch))
		var filterCols []int32
		priVec := bat.GetVector(target.primaryColumnIndexInBatch)
		// For partitioned tables, filter is not nil
		if target.filter != nil {
			filterCols = vector.MustFixedCol[int32](bat.GetVector(target.filterColIndexInBatch))
			for _, value := range filterCols {
				// has Illegal Partition index
				if value == -1 {
					return moerr.NewInvalidInput(proc.Ctx, "Table has no partition for value from column_list")
				}
			}
		}
		locked, defChanged, refreshTS, err := doLock(
			proc.Ctx,
			arg.engine,
			nil,
			target.tableID,
			proc,
			priVec,
			target.primaryColumnType,
			DefaultLockOptions(arg.rt.parker).
				WithLockMode(lock.LockMode_Exclusive).
				WithFetchLockRowsFunc(arg.rt.fetchers[idx]).
				WithMaxBytesPerLock(int(proc.LockService.GetConfig().MaxLockRowCount)).
				WithFilterRows(target.filter, filterCols).
				WithLockTable(target.lockTable, target.changeDef).
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
		if proc.TxnClient.RefreshExpressionEnabled() &&
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
		if !arg.rt.defChanged {
			arg.rt.defChanged = defChanged
		}
	}
	// when a transaction needs to operate on many data, there may be multiple conflicts on the
	// data, and if you go to retry every time a conflict occurs, you will also encounter conflicts
	// when you retry. We need to return the conflict after all the locks have been added successfully,
	// so that the retry will definitely succeed because all the locks have been put.
	if needRetry && arg.rt.retryError == nil {
		arg.rt.retryError = retryError
	}
	if arg.rt.defChanged {
		arg.rt.retryError = retryWithDefChangedError
	}
	return nil
}

// LockTable lock table, all rows in the table will be locked, and wait current txn
// closed.
func LockTable(
	eng engine.Engine,
	proc *process.Process,
	tableID uint64,
	pkType types.Type,
	changeDef bool) error {
	txnOp := proc.TxnOperator
	if !txnOp.Txn().IsPessimistic() {
		return nil
	}
	parker := types.NewPacker(proc.Mp())
	defer parker.FreeMem()

	opts := DefaultLockOptions(parker).
		WithLockTable(true, changeDef).
		WithFetchLockRowsFunc(GetFetchRowsFunc(pkType))
	_, defChanged, refreshTS, err := doLock(
		proc.Ctx,
		eng,
		nil,
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
		if !defChanged {
			return retryError
		}
		return retryWithDefChangedError
	}
	return nil
}

// LockRow lock rows in table, rows will be locked, and wait current txn closed.
func LockRows(
	eng engine.Engine,
	proc *process.Process,
	rel engine.Relation,
	tableID uint64,
	vec *vector.Vector,
	pkType types.Type,
	lockMode lock.LockMode,
	sharding lock.Sharding,
	group uint32,
) error {
	txnOp := proc.TxnOperator
	if !txnOp.Txn().IsPessimistic() {
		return nil
	}

	parker := types.NewPacker(proc.Mp())
	defer parker.FreeMem()

	opts := DefaultLockOptions(parker).
		WithLockTable(false, false).
		WithLockSharding(sharding).
		WithLockMode(lockMode).
		WithLockGroup(group).
		WithFetchLockRowsFunc(GetFetchRowsFunc(pkType))
	_, defChanged, refreshTS, err := doLock(
		proc.Ctx,
		eng,
		rel,
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
		if !defChanged {
			return retryError
		}
		return retryWithDefChangedError
	}
	return nil
}

// doLock locks a set of data so that no other transaction can modify it.
// The data is described by the primary key. When the returned timestamp.IsEmpty
// is false, it means there is a conflict with other transactions and the data to
// be manipulated has been modified, you need to get the latest data at timestamp.
func doLock(
	ctx context.Context,
	eng engine.Engine,
	rel engine.Relation,
	tableID uint64,
	proc *process.Process,
	vec *vector.Vector,
	pkType types.Type,
	opts LockOptions) (bool, bool, timestamp.Timestamp, error) {
	txnOp := proc.TxnOperator
	txnClient := proc.TxnClient
	lockService := proc.LockService

	if !txnOp.Txn().IsPessimistic() {
		return false, false, timestamp.Timestamp{}, nil
	}

	seq := txnOp.NextSequence()
	startAt := time.Now()
	trace.GetService().AddTxnDurationAction(
		txnOp,
		client.LockEvent,
		seq,
		tableID,
		0,
		nil)

	//in this case:
	// create table t1 (a int primary key, b int ,c int, unique key(b,c));
	// insert into t1 values (1,1,null);
	// update t1 set b = b+1 where a = 1;
	//    here MO will use 't1 left join hidden_tbl' to fetch the PK in hidden table to lock,
	//    but the result will be ConstNull vector
	if vec != nil && vec.IsConstNull() {
		return false, false, timestamp.Timestamp{}, nil
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
		return false, false, timestamp.Timestamp{}, nil
	}

	txn := txnOp.Txn()
	options := lock.LockOptions{
		Granularity:     g,
		Policy:          proc.WaitPolicy,
		Mode:            opts.mode,
		TableDefChanged: opts.changeDef,
		Sharding:        opts.sharding,
		Group:           opts.group,
		SnapShotTs:      txnOp.CreateTS(),
	}
	if txn.Mirror {
		options.ForwardTo = txn.LockService
		if options.ForwardTo == "" {
			panic("forward to empty lock service")
		}
	} else {
		// FIXME: in launch model, multi-cn will use same process level runtime. So lockservice will be wrong.
		if txn.LockService != lockService.GetServiceID() {
			lockService = lockservice.GetLockServiceByServiceID(txn.LockService)
		}
	}

	key := txnOp.AddWaitLock(tableID, rows, options)
	defer txnOp.RemoveWaitLock(key)

	var err error
	var result lock.Result
	for {
		result, err = lockService.Lock(
			ctx,
			tableID,
			rows,
			txn.ID,
			options)
		if !canRetryLock(txnOp, err) {
			break
		}
	}
	trace.GetService().AddTxnDurationAction(
		txnOp,
		client.LockEvent,
		seq,
		tableID,
		time.Since(startAt),
		nil)
	if err != nil {
		return false, false, timestamp.Timestamp{}, err
	}

	// add bind locks
	if err = txnOp.AddLockTable(result.LockedOn); err != nil {
		return false, false, timestamp.Timestamp{}, err
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
			return false, false, timestamp.Timestamp{}, err
		}

		fn := opts.hasNewVersionInRangeFunc
		if fn == nil {
			fn = hasNewVersionInRange
		}

		// if [snapshotTS, newSnapshotTS] has been modified, need retry at new snapshot ts
		changed, err := fn(proc, rel, tableID, eng, vec, snapshotTS, newSnapshotTS)
		if err != nil {
			return false, false, timestamp.Timestamp{}, err
		}

		if changed {
			trace.GetService().TxnNoConflictChanged(
				proc.TxnOperator,
				tableID,
				lockedTS,
				newSnapshotTS)
			if err := txnOp.UpdateSnapshot(ctx, newSnapshotTS); err != nil {
				return false, false, timestamp.Timestamp{}, err
			}
			return true, false, newSnapshotTS, nil
		}
	}

	// no conflict or has conflict, but all prev txn all aborted
	// current txn can read and write normally
	if !result.HasConflict ||
		!result.HasPrevCommit {
		return true, false, timestamp.Timestamp{}, nil
	} else if lockedTS.Less(snapshotTS) {
		return true, false, timestamp.Timestamp{}, nil
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
		return false, false, timestamp.Timestamp{}, moerr.NewTxnWWConflict(ctx, tableID, "SI not support retry")
	}

	// forward rc's snapshot ts
	snapshotTS = result.Timestamp.Next()

	trace.GetService().TxnConflictChanged(
		proc.TxnOperator,
		tableID,
		snapshotTS)
	if err := txnOp.UpdateSnapshot(ctx, snapshotTS); err != nil {
		return false, false, timestamp.Timestamp{}, err
	}
	return true, result.TableDefChanged, snapshotTS, nil
}

func canRetryLock(txn client.TxnOperator, err error) bool {
	if moerr.IsMoErrCode(err, moerr.ErrRetryForCNRollingRestart) {
		return true
	}
	if !moerr.IsMoErrCode(err, moerr.ErrLockTableBindChanged) ||
		!moerr.IsMoErrCode(err, moerr.ErrLockTableNotFound) {
		return false
	}
	if txn.LockTableCount() == 0 {
		return true
	}
	return false
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

// WithLockSharding set lock sharding
func (opts LockOptions) WithLockSharding(sharding lock.Sharding) LockOptions {
	opts.sharding = sharding
	return opts
}

// WithLockGroup set lock group
func (opts LockOptions) WithLockGroup(group uint32) LockOptions {
	opts.group = group
	return opts
}

// WithLockMode set lock mode, Exclusive or Shared
func (opts LockOptions) WithLockMode(mode lock.LockMode) LockOptions {
	opts.mode = mode
	return opts
}

// WithLockTable set lock all table
func (opts LockOptions) WithLockTable(lockTable, changeDef bool) LockOptions {
	opts.lockTable = lockTable
	opts.changeDef = changeDef
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
func NewArgumentByEngine(engine engine.Engine) *Argument {
	arg := reuse.Alloc[Argument](nil)
	arg.engine = engine
	return arg
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
			ChangeDef:          target.changeDef,
			Mode:               target.mode,
		}
	}
	return targets
}

// AddLockTarget add lock target, LockMode_Exclusive will used
func (arg *Argument) AddLockTarget(
	tableID uint64,
	primaryColumnIndexInBatch int32,
	primaryColumnType types.Type,
	refreshTimestampIndexInBatch int32) *Argument {
	return arg.AddLockTargetWithMode(
		tableID,
		lock.LockMode_Exclusive,
		primaryColumnIndexInBatch,
		primaryColumnType,
		refreshTimestampIndexInBatch)
}

// AddLockTargetWithMode add lock target with lock mode
func (arg *Argument) AddLockTargetWithMode(
	tableID uint64,
	mode lock.LockMode,
	primaryColumnIndexInBatch int32,
	primaryColumnType types.Type,
	refreshTimestampIndexInBatch int32) *Argument {
	arg.targets = append(arg.targets, lockTarget{
		tableID:                      tableID,
		primaryColumnIndexInBatch:    primaryColumnIndexInBatch,
		primaryColumnType:            primaryColumnType,
		refreshTimestampIndexInBatch: refreshTimestampIndexInBatch,
		mode:                         mode,
	})
	return arg
}

// LockTable lock all table, used for delete, truncate and drop table
func (arg *Argument) LockTable(
	tableID uint64,
	changeDef bool) *Argument {
	return arg.LockTableWithMode(
		tableID,
		lock.LockMode_Exclusive,
		changeDef)
}

// LockTableWithMode is similar to LockTable, but with specify
// lock mode
func (arg *Argument) LockTableWithMode(
	tableID uint64,
	mode lock.LockMode,
	changeDef bool) *Argument {
	for idx := range arg.targets {
		if arg.targets[idx].tableID == tableID {
			arg.targets[idx].lockTable = true
			arg.targets[idx].changeDef = changeDef
			arg.targets[idx].mode = mode
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
	return arg.AddLockTargetWithPartitionAndMode(
		tableIDs,
		lock.LockMode_Exclusive,
		primaryColumnIndexInBatch,
		primaryColumnType,
		refreshTimestampIndexInBatch,
		partitionTableIDMappingInBatch)
}

// AddLockTargetWithPartitionAndMode is similar to AddLockTargetWithPartition, but you can specify
// the lock mode
func (arg *Argument) AddLockTargetWithPartitionAndMode(
	tableIDs []uint64,
	mode lock.LockMode,
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
			mode:                         mode,
		})
	}
	return arg
}

// Free free mem
func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
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

func (arg *Argument) cleanCachedBatch(_ *process.Process) {
	// do not need clean,  only set nil
	// for _, bat := range arg.rt.cachedBatches {
	// 	bat.Clean(proc.Mp())
	// }
	arg.rt.cachedBatches = nil
}

func (arg *Argument) getBatch(
	_ *process.Process,
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
	rel engine.Relation,
	tableID uint64,
	eng engine.Engine,
	vec *vector.Vector,
	from, to timestamp.Timestamp) (bool, error) {
	if vec == nil {
		return false, nil
	}

	if rel == nil {
		var err error
		txnOp := proc.TxnOperator
		_, _, rel, err = eng.GetRelationById(proc.Ctx, txnOp, tableID)
		if err != nil {
			if strings.Contains(err.Error(), "can not find table by id") {
				return false, nil
			}
			return false, err
		}
	}
	fromTS := types.BuildTS(from.PhysicalTime, from.LogicalTime)
	toTS := types.BuildTS(to.PhysicalTime, to.LogicalTime)
	return rel.PrimaryKeysMayBeModified(proc.Ctx, fromTS, toTS, vec)
}
