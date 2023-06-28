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

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/zap"
	"golang.org/x/exp/constraints"
)

type columnCache struct {
	sync.RWMutex
	logger      *log.MOLogger
	col         AutoColumn
	cfg         Config
	ranges      *ranges
	allocator   valueAllocator
	allocating  bool
	allocatingC chan struct{}
	overflow    bool
	// For the load scenario, if the machine is good enough, there will be very many goroutines to
	// concurrently fetch the value of the self-increasing column, which will immediately trigger
	// the cache of the self-increasing column to be insufficient and thus go to the store to allocate
	// a new cache, which causes the load to block all due to this allocation being slow. The idea of
	// our optimization here is to reduce the number of allocations as much as possible, add an atomic
	// counter, check how many concurrent requests are waiting when allocating (of course this is
	// imprecise, but it doesn't matter), and then allocate more than one at a time.
	concurrencyApply atomic.Uint64
	allocateCount    atomic.Uint64
}

func newColumnCache(
	ctx context.Context,
	tableID uint64,
	col AutoColumn,
	cfg Config,
	allocator valueAllocator,
	txnOp client.TxnOperator) (*columnCache, error) {
	item := &columnCache{
		logger:    getLogger(),
		col:       col,
		cfg:       cfg,
		allocator: allocator,
		overflow:  col.Offset == math.MaxUint64,
		ranges:    &ranges{step: col.Step, values: make([]uint64, 0, 1)},
	}
	item.preAllocate(ctx, tableID, cfg.CountPerAllocate, txnOp)
	item.Lock()
	defer item.Unlock()
	if err := item.waitPrevAllocatingLocked(ctx); err != nil {
		return nil, err
	}
	return item, nil
}

func (col *columnCache) current(
	ctx context.Context,
	tableID uint64) (uint64, error) {
	col.Lock()
	defer col.Unlock()
	if err := col.waitPrevAllocatingLocked(ctx); err != nil {
		return 0, err
	}
	return col.ranges.current(), nil
}

func (col *columnCache) insertAutoValues(
	ctx context.Context,
	tableID uint64,
	vec *vector.Vector,
	rows int,
	txnOp client.TxnOperator) (uint64, error) {
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
				return moerr.NewOutOfRange(
					ctx,
					"tinyint",
					"value %v",
					v)
			},
			txnOp)
	case types.T_int16:
		return insertAutoValues[int16](
			ctx,
			tableID,
			vec,
			rows,
			math.MaxInt16,
			col,
			func(v uint64) error {
				return moerr.NewOutOfRange(
					ctx,
					"smallint",
					"value %v",
					v)
			},
			txnOp)
	case types.T_int32:
		return insertAutoValues[int32](
			ctx,
			tableID,
			vec, rows,
			math.MaxInt32,
			col,
			func(v uint64) error {
				return moerr.NewOutOfRange(
					ctx,
					"int",
					"value %v",
					v)
			},
			txnOp)
	case types.T_int64:
		return insertAutoValues[int64](
			ctx,
			tableID,
			vec,
			rows,
			math.MaxInt64,
			col,
			func(v uint64) error {
				return moerr.NewOutOfRange(
					ctx,
					"bigint",
					"value %v",
					v)
			},
			txnOp)
	case types.T_uint8:
		return insertAutoValues[uint8](
			ctx,
			tableID,
			vec,
			rows,
			math.MaxUint8,
			col,
			func(v uint64) error {
				return moerr.NewOutOfRange(
					ctx,
					"tinyint unsigned",
					"value %v",
					v)
			},
			txnOp)
	case types.T_uint16:
		return insertAutoValues[uint16](
			ctx,
			tableID,
			vec,
			rows,
			math.MaxUint16,
			col,
			func(v uint64) error {
				return moerr.NewOutOfRange(
					ctx,
					"smallint unsigned",
					"value %v",
					v)
			},
			txnOp)
	case types.T_uint32:
		return insertAutoValues[uint32](
			ctx,
			tableID,
			vec,
			rows,
			math.MaxUint32,
			col,
			func(v uint64) error {
				return moerr.NewOutOfRange(
					ctx,
					"int unsigned",
					"value %v",
					v)
			},
			txnOp)
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
			txnOp)
	default:
		return 0, moerr.NewInvalidInput(ctx, "invalid auto_increment type '%v'", vec.GetType().Oid)
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
	count int,
	manualValue uint64,
	txnOp client.TxnOperator) error {
	col.Lock()

	contains := col.ranges.updateTo(manualValue)
	// mark col next() is overflow
	if manualValue == math.MaxUint64 {
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
	apply func(int, uint64) error) error {
	cul := col.concurrencyApply.Load()
	col.concurrencyApply.Add(1)
	col.Lock()
	defer col.Unlock()

	if err := col.waitPrevAllocatingLocked(ctx); err != nil {
		return err
	}

	wait := func() (bool, error) {
		if col.overflow {
			return true, nil
		}

		if col.ranges.empty() {
			if err := col.allocateLocked(ctx, tableID, rows, cul); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	for i := 0; i < rows; i++ {
		if filter(i) {
			continue
		}
		if skipped != nil &&
			skipped.left() > 0 {
			if err := apply(i, skipped.next()); err != nil {
				return err
			}
			continue
		}
		overflow, err := wait()
		if err != nil {
			return err
		}
		if overflow {
			return apply(i, 0)
		}
		if err := apply(i, col.ranges.next()); err != nil {
			return err
		}
	}
	return nil
}

func (col *columnCache) preAllocate(
	ctx context.Context,
	tableID uint64,
	count int,
	txnOp client.TxnOperator) {
	col.Lock()
	defer col.Unlock()

	if col.ranges.left() >= count {
		return
	}

	if col.allocating ||
		col.overflow {
		return
	}
	col.allocating = true
	col.allocatingC = make(chan struct{})
	if col.cfg.CountPerAllocate > count {
		count = col.cfg.CountPerAllocate
	}
	col.allocator.asyncAllocate(
		ctx,
		tableID,
		col.col.ColName,
		count,
		txnOp,
		func(from, to uint64, err error) {
			if err == nil {
				col.applyAllocate(from, to)
			} else {
				col.applyAllocate(0, 0)
			}
		})
}

func (col *columnCache) allocateLocked(
	ctx context.Context,
	tableID uint64,
	count int,
	beforeApplyCount uint64) error {
	if err := col.waitPrevAllocatingLocked(ctx); err != nil {
		return err
	}

	col.allocating = true
	col.allocatingC = make(chan struct{})
	if col.cfg.CountPerAllocate > count {
		count = col.cfg.CountPerAllocate
	}
	n := int(col.concurrencyApply.Load() - beforeApplyCount)
	if n == 0 {
		n = 1
	}
	for {
		from, to, err := col.allocator.allocate(
			ctx,
			tableID,
			col.col.ColName,
			count*n,
			nil)
		if err == nil {
			col.allocateCount.Add(1)
			col.applyAllocateLocked(from, to)
			return nil
		}
	}
}

func (col *columnCache) maybeAllocate(tableID uint64) {
	col.Lock()
	low := col.ranges.left() <= col.cfg.LowCapacity
	col.Unlock()
	if low {
		col.preAllocate(context.Background(), tableID, col.cfg.CountPerAllocate, nil)
	}
}

func (col *columnCache) applyAllocate(
	from uint64,
	to uint64) {
	col.Lock()
	defer col.Unlock()

	col.applyAllocateLocked(from, to)
}

func (col *columnCache) applyAllocateLocked(
	from uint64,
	to uint64) {
	if to > from {
		col.ranges.add(from, to)
		if col.logger.Enabled(zap.DebugLevel) {
			col.logger.Debug("new range added",
				zap.String("col", col.col.ColName),
				zap.Uint64("from", from),
				zap.Uint64("to", to))
		}
	}
	close(col.allocatingC)
	col.allocating = false
}

func (col *columnCache) waitPrevAllocatingLocked(ctx context.Context) error {
	for {
		if !col.allocating {
			return nil
		}
		c := col.allocatingC
		// we must unlock here, becase we may wait for a long time. And Lock will added
		// before return, because the caller holds the lock and call this method and use
		// defer to unlock.
		col.Unlock()
		select {
		case <-ctx.Done():
			col.Lock()
			return ctx.Err()
		case <-c:
		}
		col.Lock()
	}
}

func (col *columnCache) close() error {
	col.Lock()
	defer col.Unlock()
	return col.waitPrevAllocatingLocked(context.Background())
}

func insertAutoValues[T constraints.Integer](
	ctx context.Context,
	tableID uint64,
	vec *vector.Vector,
	rows int,
	max T,
	col *columnCache,
	outOfRangeError func(v uint64) error,
	txnOp client.TxnOperator) (uint64, error) {
	// all values are filled after insert
	defer func() {
		vec.SetNulls(nil)
		col.maybeAllocate(tableID)
	}()

	vs := vector.MustFixedCol[T](vec)
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
				rows,
				maxValue,
				txnOp); err != nil {
				return 0, err
			}
		}
	}
	col.preAllocate(ctx, tableID, rows, nil)
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
			lastInsertValue = v
			return nil
		})
	if err != nil {
		return 0, err
	}
	return lastInsertValue, err
}
