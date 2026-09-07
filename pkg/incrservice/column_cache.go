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

package incrservice

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/zap"
	"golang.org/x/exp/constraints"
)

var (
	maxRetryTimes = 2
)

// A valid allocation of rows*increment unit values contains enough members of
// every compatible session series for the current request.  This bound is a
// last-resort guard for corrupt/legacy step metadata or a broken allocator;
// it prevents an impossible residue from spinning forever while leaving
// normal allocation behavior unchanged.
const maxAutoIncrementAllocationsPerRow = 8

type columnCache struct {
	sync.RWMutex
	logger        *log.MOLogger
	col           AutoColumn
	cfg           Config
	ranges        *ranges
	allocator     valueAllocator
	allocating    bool
	allocatingC   chan error
	overflow      bool
	terminal      bool
	terminalValue uint64
	terminalTS    timestamp.Timestamp
	// For the load scenario, if the machine is good enough, there will be very many goroutines to
	// concurrently fetch the value of the self-increasing column, which will immediately trigger
	// the cache of the self-increasing column to be insufficient and thus go to the store to allocate
	// a new cache, which causes the load to block all due to this allocation being slow. The idea of
	// our optimization here is to reduce the number of allocations as much as possible, add an atomic
	// counter, check how many concurrent requests are waiting when allocating (of course this is
	// imprecise, but it doesn't matter), and then allocate more than one at a time.
	concurrencyApply atomic.Uint64
	allocateCount    atomic.Uint64
	committed        bool
	retired          bool
}

func newColumnCache(
	ctx context.Context,
	sid string,
	tableID uint64,
	col AutoColumn,
	cfg Config,
	committed bool,
	allocator valueAllocator,
	txnOp client.TxnOperator,
) (*columnCache, error) {
	item := &columnCache{
		logger:    getLogger(sid).Named("incrservice"),
		col:       col,
		cfg:       cfg,
		allocator: allocator,
		overflow:  col.Offset == math.MaxUint64,
		ranges:    &ranges{step: col.Step, values: make([]uint64, 0, 1)},
		committed: committed,
	}
	item.preAllocate(ctx, tableID, cfg.CountPerAllocate, txnOp)
	item.Lock()
	defer item.Unlock()
	if err := item.waitPrevAllocatingLocked(ctx); err != nil {
		return nil, err
	}
	return item, nil
}

func (col *columnCache) current(ctx context.Context) (uint64, error) {
	col.Lock()
	defer col.Unlock()
	if err := col.waitPrevAllocatingLocked(ctx); err != nil {
		return 0, err
	}
	if v := col.ranges.current(); v != 0 {
		return v, nil
	}
	if col.terminal {
		return col.terminalValue, nil
	}
	return 0, nil
}

func (col *columnCache) insertAutoValues(
	ctx context.Context,
	tableID uint64,
	vec *vector.Vector,
	rows int,
	txnOp client.TxnOperator) (uint64, error) {
	options := AutoIncrementOptionsFromContext(ctx)
	switch vec.GetType().Oid {
	case types.T_int8:
		return insertAutoValues[int8](
			ctx,
			tableID,
			vec,
			rows,
			math.MaxInt8,
			col,
			func(v uint64) error {
				if v == 0 {
					v = math.MaxInt8 + 1
				}
				return moerr.NewOutOfRangef(
					ctx,
					"tinyint",
					"value %v",
					v)
			},
			txnOp,
			options)
	case types.T_int16:
		return insertAutoValues[int16](
			ctx,
			tableID,
			vec,
			rows,
			math.MaxInt16,
			col,
			func(v uint64) error {
				if v == 0 {
					v = math.MaxInt16 + 1
				}
				return moerr.NewOutOfRangef(
					ctx,
					"smallint",
					"value %v",
					v)
			},
			txnOp,
			options)
	case types.T_int32:
		return insertAutoValues[int32](
			ctx,
			tableID,
			vec, rows,
			math.MaxInt32,
			col,
			func(v uint64) error {
				if v == 0 {
					v = math.MaxInt32 + 1
				}
				return moerr.NewOutOfRangef(
					ctx,
					"int",
					"value %v",
					v)
			},
			txnOp,
			options)
	case types.T_int64:
		return insertAutoValues[int64](
			ctx,
			tableID,
			vec,
			rows,
			math.MaxInt64,
			col,
			func(v uint64) error {
				if v == 0 {
					v = math.MaxInt64 + 1
				}
				return moerr.NewOutOfRangef(
					ctx,
					"bigint",
					"value %v",
					v)
			},
			txnOp,
			options)
	case types.T_uint8:
		return insertAutoValues[uint8](
			ctx,
			tableID,
			vec,
			rows,
			math.MaxUint8,
			col,
			func(v uint64) error {
				if v == 0 {
					v = math.MaxUint8 + 1
				}
				return moerr.NewOutOfRangef(
					ctx,
					"tinyint unsigned",
					"value %v",
					v)
			},
			txnOp,
			options)
	case types.T_uint16:
		return insertAutoValues[uint16](
			ctx,
			tableID,
			vec,
			rows,
			math.MaxUint16,
			col,
			func(v uint64) error {
				if v == 0 {
					v = math.MaxUint16 + 1
				}
				return moerr.NewOutOfRangef(
					ctx,
					"smallint unsigned",
					"value %v",
					v)
			},
			txnOp,
			options)
	case types.T_uint32:
		return insertAutoValues[uint32](
			ctx,
			tableID,
			vec,
			rows,
			math.MaxUint32,
			col,
			func(v uint64) error {
				if v == 0 {
					v = math.MaxUint32 + 1
				}
				return moerr.NewOutOfRangef(
					ctx,
					"int unsigned",
					"value %v",
					v)
			},
			txnOp,
			options)
	case types.T_uint64:
		return insertAutoValues[uint64](
			ctx,
			tableID,
			vec,
			rows,
			math.MaxUint64,
			col,
			func(v uint64) error {
				return moerr.NewOutOfRange(
					ctx,
					"bigint unsigned",
					"auto_incrment column constant value overflows bigint unsigned",
				)
			},
			txnOp,
			options)
	default:
		return 0, moerr.NewInvalidInputf(ctx, "invalid auto_increment type '%v'", vec.GetType().Oid)
	}
}

func (col *columnCache) lockDo(fn func()) {
	col.Lock()
	defer col.Unlock()
	fn()
}

func (col *columnCache) updateTo(
	ctx context.Context,
	tableID uint64,
	manualValue uint64,
	txnOp client.TxnOperator) error {
	col.Lock()

	contains := col.ranges.updateTo(manualValue)
	if col.terminal && manualValue >= col.terminalValue {
		col.terminal = false
		col.terminalValue = 0
		col.terminalTS = timestamp.Timestamp{}
		col.overflow = true
		contains = true
	}
	// mark col next() is overflow
	if manualValue == math.MaxUint64 {
		col.terminal = false
		col.terminalValue = 0
		col.terminalTS = timestamp.Timestamp{}
		col.overflow = true
	}
	col.Unlock()

	if contains {
		return nil
	}

	return col.allocator.updateMinValue(
		ctx,
		tableID,
		col.col.ColName,
		manualValue,
		txnOp)
}

func (col *columnCache) applyAutoValues(
	ctx context.Context,
	tableID uint64,
	rows int,
	skipped *ranges,
	filter func(i int) bool,
	apply func(int, uint64) error,
	txnOp client.TxnOperator,
	options AutoIncrementOptions) error {
	options = NormalizeAutoIncrementOptions(options.Increment, options.Offset)
	cul := col.concurrencyApply.Load()
	col.concurrencyApply.Add(1)
	col.Lock()
	defer col.Unlock()

	if err := col.waitPrevAllocatingLocked(ctx); err != nil {
		return err
	}

	for i := 0; i < rows; i++ {
		if filter(i) {
			continue
		}

		// Values displaced by explicit manual inserts remain usable only when
		// they belong to this statement's session series.  A non-unit
		// increment must not accidentally consume a value from the skipped
		// portion which has a different residue.
		if skipped != nil {
			if value := skipped.nextFor(options); value != 0 {
				if err := apply(i, value); err != nil {
					return err
				}
				continue
			}
		}

		for allocations := 0; ; allocations++ {
			if allocations >= maxAutoIncrementAllocationsPerRow {
				return moerr.NewInternalErrorf(
					ctx,
					"AUTO_INCREMENT could not find a value in the requested session series after %d allocations",
					maxAutoIncrementAllocationsPerRow,
				)
			}
			if col.overflow {
				return apply(i, 0)
			}

			value := col.ranges.nextFor(options)
			if value != 0 {
				if err := apply(i, value); err != nil {
					return err
				}
				break
			}

			if col.terminal {
				if isAutoIncrementValue(col.terminalValue, options) {
					value = col.terminalValue
					col.terminal = false
					col.terminalValue = 0
					col.terminalTS = timestamp.Timestamp{}
					col.overflow = true
					if err := apply(i, value); err != nil {
						return err
					}
				} else {
					return apply(i, 0)
				}
				break
			}

			allocationCount, err := autoIncrementAllocationCount(ctx, rows, options)
			if err != nil {
				return err
			}
			if err = col.allocateLockedWithOptions(
				ctx, tableID, allocationCount, cul, txnOp, options); err != nil {
				return err
			}
		}
	}
	return nil
}

func (col *columnCache) preAllocate(
	ctx context.Context,
	tableID uint64,
	count int,
	txnOp client.TxnOperator) {
	// Statement-scoped non-default series reserve on demand in applyAutoValues;
	// prefetching a default block here would consume values from the shared
	// allocator which may not belong to that series.
	if !AutoIncrementOptionsFromContext(ctx).isDefault() {
		return
	}
	col.Lock()
	defer col.Unlock()
	if col.retired {
		return
	}

	if col.ranges.left() >= count || col.terminal {
		return
	}

	if col.allocating ||
		col.overflow {
		return
	}
	col.allocating = true
	col.allocatingC = make(chan error, 1)
	if col.cfg.CountPerAllocate > count {
		count = col.cfg.CountPerAllocate
	}
	err := col.allocator.asyncAllocate(
		ctx,
		tableID,
		col.col.ColName,
		count,
		txnOp,
		func(from, to uint64, lastAllocateAt timestamp.Timestamp, err error) {
			if err == nil {
				col.applyAllocate(from, to, lastAllocateAt, err)
			} else {
				col.applyAllocate(0, 0, timestamp.Timestamp{}, err)
			}
		})
	if err != nil {
		col.applyAllocateLocked(0, 0, timestamp.Timestamp{}, err)
	}
}

func (col *columnCache) allocateLocked(
	ctx context.Context,
	tableID uint64,
	count int,
	beforeApplyCount uint64,
	txnOp client.TxnOperator) error {
	return col.allocateLockedWithOptions(
		ctx, tableID, count, beforeApplyCount, txnOp,
		NormalizeAutoIncrementOptions(1, 1))
}

func (col *columnCache) allocateLockedWithOptions(
	ctx context.Context,
	tableID uint64,
	count int,
	beforeApplyCount uint64,
	txnOp client.TxnOperator,
	options AutoIncrementOptions) error {
	if err := col.waitPrevAllocatingLocked(ctx); err != nil {
		return err
	}
	if col.retired {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}

	if options.isDefault() && col.cfg.CountPerAllocate > count {
		count = col.cfg.CountPerAllocate
	}
	concurrent := col.concurrencyApply.Load()
	if concurrent < beforeApplyCount {
		return moerr.NewInternalError(ctx, "AUTO_INCREMENT concurrency accounting moved backwards")
	}
	concurrent -= beforeApplyCount
	if concurrent == 0 {
		concurrent = 1
	}
	maxInt := uint64(^uint(0) >> 1)
	if count <= 0 || uint64(count) > maxInt/concurrent {
		return moerr.NewOutOfRangef(
			ctx,
			"AUTO_INCREMENT",
			"allocation request overflows the supported range: count=%d concurrency=%d",
			count,
			concurrent,
		)
	}
	n := int(concurrent)

	col.allocating = true
	col.allocatingC = make(chan error, 1)

	var from, to uint64
	var allocateAt timestamp.Timestamp
	var err error
	for i := 0; i < maxRetryTimes; i++ {
		from, to, allocateAt, err = col.allocator.allocate(
			ctx,
			tableID,
			col.col.ColName,
			count*n,
			txnOp)
		col.allocateCount.Add(1)
		if err == nil {
			break
		}

		col.logger.Error("allocator increment value failed",
			zap.Error(err),
			zap.Uint64("table", col.col.TableID),
			zap.String("col", col.col.ColName))
	}
	col.applyAllocateLocked(from, to, allocateAt, err)
	return err
}

func (col *columnCache) maybeAllocate(ctx context.Context, tableID uint64, txnOp client.TxnOperator) error {
	options := AutoIncrementOptionsFromContext(ctx)
	// A non-default statement reserves the exact underlying span it needs in
	// applyAutoValues.  Background prefetch here would reserve a default-sized
	// block which may contain no values for this session's residue and would
	// create avoidable high-water jumps.
	if !options.isDefault() {
		return nil
	}
	col.Lock()
	committed := col.committed
	low := col.ranges.left() <= col.cfg.LowCapacity && !col.terminal
	retired := col.retired
	col.Unlock()
	if low && committed && !retired {
		accountId, err := defines.GetAccountId(ctx)
		if err != nil {
			return err
		}
		col.preAllocate(defines.AttachAccountId(context.Background(), accountId),
			tableID,
			col.cfg.CountPerAllocate,
			txnOp)
	}
	return nil
}

func (col *columnCache) retire() {
	col.Lock()
	col.retired = true
	col.Unlock()
}

func (col *columnCache) applyAllocate(
	from uint64,
	to uint64,
	allocateAt timestamp.Timestamp,
	err error) {
	col.Lock()
	defer col.Unlock()

	col.applyAllocateLocked(from, to, allocateAt, err)
}

func (col *columnCache) applyAllocateLocked(
	from uint64,
	to uint64,
	allocateAt timestamp.Timestamp,
	err error) {
	if err != nil {
		select {
		case col.allocatingC <- err:
		default:
		}
	}

	// A wrapped exclusive upper bound means the allocation reached the end of
	// uint64. Keep its final value separately because max+step is not representable.
	if to < from {
		terminalValue := to - col.col.Step
		if from < terminalValue {
			col.ranges.addWithTimestamp(from, terminalValue, allocateAt)
		}
		col.terminal = true
		col.terminalValue = terminalValue
		col.terminalTS = allocateAt
	} else if to > from {
		col.ranges.addWithTimestamp(from, to, allocateAt)
	}
	close(col.allocatingC)
	col.allocating = false
}

func (col *columnCache) oldestAllocateAtLocked() timestamp.Timestamp {
	if !col.ranges.empty() {
		return col.ranges.oldestAllocateAt()
	}
	return col.terminalTS
}

func (col *columnCache) waitPrevAllocatingLocked(ctx context.Context) error {
	for {
		if !col.allocating {
			return nil
		}
		c := col.allocatingC
		// we must unlock here, because we may wait for a long time. And Lock will added
		// before return, because the caller holds the lock and call this method and use
		// defer to unlock.
		col.Unlock()
		select {
		case <-ctx.Done():
			col.Lock()
			return ctx.Err()
		case err := <-c:
			if err != nil {
				col.Lock()
				return err
			}
		}
		col.Lock()
	}
}

func (col *columnCache) close() error {
	return nil
}

func autoIncrementAllocationCount(
	ctx context.Context,
	rows int,
	options AutoIncrementOptions) (int, error) {
	if rows <= 0 || options.isDefault() {
		return rows, nil
	}
	maxInt := uint64(^uint(0) >> 1)
	if options.Increment > maxInt/uint64(rows) {
		return 0, moerr.NewOutOfRangef(
			ctx,
			"AUTO_INCREMENT",
			"statement reservation overflows the supported range: rows=%d increment=%d",
			rows,
			options.Increment,
		)
	}
	return rows * int(options.Increment), nil
}

func isAutoIncrementValue(value uint64, options AutoIncrementOptions) bool {
	options = NormalizeAutoIncrementOptions(options.Increment, options.Offset)
	return value >= options.Offset &&
		(value-options.Offset)%options.Increment == 0
}

func insertAutoValues[T constraints.Integer](
	ctx context.Context,
	tableID uint64,
	vec *vector.Vector,
	rows int,
	max T,
	col *columnCache,
	outOfRangeError func(v uint64) error,
	txnOp client.TxnOperator,
	options AutoIncrementOptions) (uint64, error) {
	options = NormalizeAutoIncrementOptions(options.Increment, options.Offset)
	// all values are filled after insert
	defer func() {
		vec.SetNulls(nil)
		col.maybeAllocate(ctx, tableID, txnOp)
	}()

	vs := vector.MustFixedColWithTypeCheck[T](vec)
	autoCount := vec.GetNulls().Count()
	lastInsertValue := uint64(0)

	// has manual values, we reuse skipped auto values, and update cache max value to store
	var skipped *ranges
	if autoCount < rows {
		skipped = &ranges{step: col.col.Step}
		manuals := roaring64.NewBitmap()
		maxValue := uint64(0)
		col.lockDo(func() {
			for i, v := range vs {
				// vector maybe has some invalid value, must use null bitmap to check the manual value
				if !nulls.Contains(vec.GetNulls(), uint64(i)) && v > 0 {
					manuals.Add(uint64(v))
				}
			}
			if manuals.GetCardinality() > 0 {
				// use a bitmap to store the manually inserted values and iterate through these manual
				// values in order to skip the automatic values.
				iter := manuals.Iterator()
				for {
					if !iter.HasNext() {
						break
					}
					maxValue = iter.Next()
					col.ranges.setManual(maxValue, skipped)
				}
			}
		})
		if maxValue > 0 {
			if err := col.updateTo(
				ctx,
				tableID,
				maxValue,
				txnOp); err != nil {
				return 0, err
			}
		}
	}
	if options.isDefault() {
		col.preAllocate(ctx, tableID, rows, txnOp)
	}
	err := col.applyAutoValues(
		ctx,
		tableID,
		rows,
		skipped,
		func(i int) bool {
			filter := autoCount < rows &&
				!nulls.Contains(vec.GetNulls(), uint64(i))
			if filter && skipped != nil {
				skipped.updateTo(uint64(vs[i]))
			}
			return filter
		},
		func(i int, v uint64) error {
			if v > uint64(max) ||
				v == 0 {
				return outOfRangeError(v)
			}
			vs[i] = T(v)
			if lastInsertValue == 0 {
				// LAST_INSERT_ID() reports the first automatically generated
				// value of a multi-row INSERT, not the last value filled in the
				// batch. Auto-increment values are never zero here.
				lastInsertValue = v
			}
			return nil
		},
		txnOp,
		options)
	if err != nil {
		return 0, err
	}
	return lastInsertValue, err
}
