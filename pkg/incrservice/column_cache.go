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
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"go.uber.org/zap"
	"golang.org/x/exp/constraints"
)

type columnCache struct {
	sync.RWMutex
	logger      *log.MOLogger
	key         string
	capacity    int
	ranges      *ranges
	allocator   valueAllocator
	allocating  bool
	allocatingC chan struct{}
}

func newColumnCache(
	ctx context.Context,
	key string,
	capacity int,
	step int,
	allocator valueAllocator) *columnCache {
	item := &columnCache{
		logger:    getLogger(),
		key:       key,
		capacity:  capacity,
		allocator: allocator,
		ranges:    &ranges{step: uint64(step), values: make([]uint64, 0, 1)},
	}
	item.preAllocate(ctx, capacity)
	return item
}

func (col *columnCache) insertAutoValues(
	ctx context.Context,
	vec *vector.Vector,
	rows int) error {
	switch vec.GetType().Oid {
	case types.T_int8:
		insertAutoValues[int8](ctx, vec, rows, col)
	case types.T_int16:
		insertAutoValues[int16](ctx, vec, rows, col)
	case types.T_int32:
		insertAutoValues[int32](ctx, vec, rows, col)
	case types.T_int64:
		insertAutoValues[int64](ctx, vec, rows, col)
	case types.T_uint8:
		insertAutoValues[uint8](ctx, vec, rows, col)
	case types.T_uint16:
		insertAutoValues[uint16](ctx, vec, rows, col)
	case types.T_uint32:
		insertAutoValues[uint32](ctx, vec, rows, col)
	case types.T_uint64:
		insertAutoValues[uint64](ctx, vec, rows, col)
	default:
		return moerr.NewInvalidInput(ctx, "invalid auto_increment type '%v'", vec.GetType().Oid)
	}
	return nil
}

func (col *columnCache) applyAutoValues(
	ctx context.Context,
	rows int,
	apply func(int, uint64)) error {
	col.Lock()
	defer col.Unlock()
	if err := col.waitPrevAllocatingLocked(ctx); err != nil {
		return err
	}

	for i := 0; i < rows; i++ {
		if col.ranges.empty() {
			if err := col.allocateLocked(ctx, rows); err != nil {
				return err
			}
		}
		apply(i, col.ranges.next())
	}
	return nil
}

func (col *columnCache) applyAutoValuesWithManual(
	ctx context.Context,
	rows int,
	manualValues *roaring64.Bitmap,
	filter func(i int) bool,
	apply func(int, uint64)) error {
	col.Lock()
	defer col.Unlock()
	if err := col.waitPrevAllocatingLocked(ctx); err != nil {
		return err
	}

	skipRows := uint64(0)
	manualRowsCount := manualValues.GetCardinality()
	wait := func() error {
		if col.ranges.empty() {
			if err := col.allocateLocked(ctx, rows); err != nil {
				return err
			}
		}
		return nil
	}
	for i := 0; i < rows; i++ {
		if filter(i) {
			continue
		}
		// all manual values are skipped, insert directly
		if skipRows >= manualRowsCount {
			if err := wait(); err != nil {
				return err
			}
			apply(i, col.ranges.next())
			continue
		}

		// skip manual values
		for {
			if err := wait(); err != nil {
				return err
			}
			v := col.ranges.next()
			if !manualValues.Contains(v) {
				apply(i, v)
				break
			}
			skipRows++
		}
	}
	return nil
}

func (col *columnCache) preAllocate(
	ctx context.Context,
	count int) {
	col.Lock()
	defer col.Unlock()

	if col.ranges.left() >= count {
		return
	}
	if col.allocating {
		return
	}

	col.allocating = true
	col.allocatingC = make(chan struct{})
	if col.capacity > count {
		count = col.capacity
	}
	col.allocator.asyncAlloc(
		ctx,
		col.key,
		count,
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
	count int) error {
	if err := col.waitPrevAllocatingLocked(ctx); err != nil {
		return err
	}
	col.allocating = true
	col.allocatingC = make(chan struct{})
	if col.capacity > count {
		count = col.capacity
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			from, to, err := col.allocator.alloc(
				ctx,
				col.key,
				count)
			if err == nil {
				col.applyAllocateLocked(from, to)
				return nil
			}
		}
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
				zap.String("key", col.key),
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
		col.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c:
		}
		col.Lock()
	}
}

func insertAutoValues[T constraints.Integer](
	ctx context.Context,
	vec *vector.Vector,
	rows int,
	col *columnCache) error {
	// all values are filled after insert
	defer vec.SetNulls(nil)

	col.preAllocate(ctx, rows)
	vs := vector.MustFixedCol[T](vec)
	autoCount := nulls.Length(vec.GetNulls())
	manualCount := rows - autoCount

	// no manual insert values
	if autoCount == rows {
		return col.applyAutoValues(
			ctx,
			rows,
			func(i int, v uint64) {
				vs[i] = T(v)
			})
	}

	// all values are manual insert values
	if autoCount == 0 {
		return nil
	}

	// add all manual values to bitmap for skip auto_increment values
	manualValues := roaring64.New()
	added := 0
	for _, v := range vs {
		if v > 0 {
			manualValues.Add(uint64(v))
			added++
			if added == manualCount {
				break
			}
		}
	}
	return col.applyAutoValuesWithManual(
		ctx,
		rows,
		manualValues,
		func(i int) bool {
			return vs[i] > 0
		},
		func(i int, v uint64) {
			vs[i] = T(v)
		})
}
