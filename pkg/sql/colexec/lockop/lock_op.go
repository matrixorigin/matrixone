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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
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
	for idx := range arg.targets {
		arg.targets[idx].fetcher = GetFetchRowsFunc(arg.targets[idx].primaryColumnType)
	}
	arg.parker = types.NewPacker(proc.Mp())
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
	_ bool,
	_ bool) (bool, error) {
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}

	arg, ok := v.(*Argument)
	if !ok {
		getLogger().Fatal("invalid argument",
			zap.Any("argument", arg))
	}

	txnOp := proc.TxnOperator
	for _, target := range arg.targets {
		getLogger().Debug("lock",
			zap.Uint64("table", target.tableID),
			zap.Int32("primary-index", target.primaryColumnIndexInBatch))
		priVec := bat.GetVector(target.primaryColumnIndexInBatch)
		refreshTS, err := Lock(
			proc.Ctx,
			target.tableID,
			txnOp,
			proc.LockService,
			priVec,
			target.primaryColumnType,
			DefaultLockOptions(arg.parker).
				WithLockMode(lock.LockMode_Exclusive).
				WithFetchLockRowsFunc(target.fetcher).
				WithMaxBytesPerLock(int(proc.LockService.GetConfig().MaxLockRowBytes)),
		)
		if getLogger().Enabled(zap.DebugLevel) {
			getLogger().Debug("lock result",
				zap.Uint64("table", target.tableID),
				zap.Int32("primary-index", target.primaryColumnIndexInBatch),
				zap.String("refresh-ts", refreshTS.DebugString()),
				zap.Error(err))
		}
		if err != nil {
			return false, err
		}

		vec := bat.GetVector(target.refreshTimestampIndexInBatch)
		ts := types.BuildTS(refreshTS.PhysicalTime, refreshTS.LogicalTime)
		n := priVec.Length()
		for i := 0; i < n; i++ {
			vector.AppendFixed(vec, ts, false, proc.Mp())
		}
	}
	return false, nil
}

// Lock locks a set of data so that no other transaction can modify it.
// The data is described by the primary key. When the returned timestamp.IsEmpty
// is false, it means there is a conflict with other transactions and the data to
// be manipulated has been modified, you need to get the latest data at timestamp.
func Lock(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
	lockService lockservice.LockService,
	vec *vector.Vector,
	pkType types.Type,
	opts LockOptions) (timestamp.Timestamp, error) {
	if !txnOp.Txn().IsPessimistic() {
		return timestamp.Timestamp{}, nil
	}

	if opts.maxBytesPerLock == 0 {
		opts.maxBytesPerLock = int(lockService.GetConfig().MaxLockRowBytes)
	}
	fetchFunc := opts.fetchFunc
	if fetchFunc == nil {
		fetchFunc = GetFetchRowsFunc(pkType)
	}

	rows, g := fetchFunc(
		vec,
		opts.parker,
		pkType,
		opts.maxBytesPerLock,
		opts.lockTable)
	result, err := lockService.Lock(
		ctx,
		tableID,
		rows,
		txnOp.Txn().ID,
		lock.LockOptions{
			Granularity: g,
			Policy:      lock.WaitPolicy_Wait,
			Mode:        opts.mode,
		})
	if err != nil {
		return timestamp.Timestamp{}, err
	}

	// add bind locks
	if err := txnOp.AddLockTable(result.LockedOn); err != nil {
		return timestamp.Timestamp{}, err
	}

	// no conflict or has conflict, but all prev txn all aborted
	// current txn can read and write normally
	if !result.HasConflict ||
		!result.HasPrevCommit {
		return timestamp.Timestamp{}, nil
	}

	// Arriving here means that at least one of the conflicting
	// transactions has committed.
	//
	// For the RC schema we need some retries between
	// [txn.snapshotts, prev.committs] (de-duplication for insert, re-query for
	// update and delete).
	//
	// For the SI schema the current transaction needs to be abort (TODO: later
	// we can consider recording the ReadSet of the transaction and check if data
	// is modified between [snapshotTS,prev.commits] and raise the SnapshotTS of
	// the SI transaction to eliminate conflicts)
	if !txnOp.Txn().IsRCIsolation() {
		return timestamp.Timestamp{}, moerr.NewTxnWWConflict(ctx)
	}

	// forward rc's snapshot ts
	if err := txnOp.UpdateSnapshot(result.Timestamp); err != nil {
		return timestamp.Timestamp{}, err
	}
	return result.Timestamp.Next(), nil
}

// DefaultLockOptions create a default lock operation. The parker is used to
// encode primary key into lock row.
func DefaultLockOptions(parker *types.Packer) LockOptions {
	return LockOptions{
		mode:            lock.LockMode_Exclusive,
		lockTable:       false,
		maxBytesPerLock: 0,
		parker:          parker,
	}
}

// WithLockMode set lock mode, Exclusive or Shared
func (opts LockOptions) WithLockMode(mode lock.LockMode) LockOptions {
	opts.mode = mode
	return opts
}

// WithLockTable set lock all table
func (opts LockOptions) WithLockTable() LockOptions {
	opts.lockTable = true
	return opts
}

// WithMaxBytesPerLock every lock operation, will add some lock rows into
// lockservice. If very many rows of data are added at once, this can result
// in an excessive memory footprint. This value limits the amount of lock memory
// that can be allocated per lock operation, and if it is exceeded, it will be
// converted to a range lock.
func (opts LockOptions) WithMaxBytesPerLock(maxBytesPerLock int) LockOptions {
	opts.maxBytesPerLock = maxBytesPerLock
	return opts
}

// WithFetchLockRowsFunc set the primary key into lock rows conversion function.
func (opts LockOptions) WithFetchLockRowsFunc(fetchFunc FetchLockRowsFunc) LockOptions {
	opts.fetchFunc = fetchFunc
	return opts
}

// NewArgument create new lock op argument.
func NewArgument() *Argument {
	return &Argument{}
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

// Free free mem
func (arg *Argument) Free(
	proc *process.Process,
	pipelineFailed bool) {
	if arg.parker != nil {
		arg.parker.FreeMem()
	}
}
