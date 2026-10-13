// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

func autoIncrementTypeLimit(typ types.Type) (uint64, bool) {
	switch typ.Oid {
	case types.T_int8:
		return math.MaxInt8, true
	case types.T_uint8:
		return math.MaxUint8, true
	case types.T_int16:
		return math.MaxInt16, true
	case types.T_uint16:
		return math.MaxUint16, true
	case types.T_int32:
		if typ.IsMediumInt() {
			return 1<<23 - 1, true
		}
		return math.MaxInt32, true
	case types.T_uint32:
		if typ.IsMediumInt() {
			return 1<<24 - 1, true
		}
		return math.MaxUint32, true
	case types.T_int64:
		return math.MaxInt64, true
	case types.T_uint64:
		return math.MaxUint64, true
	default:
		return 0, false
	}
}

// consumeUnusedAutoValues advances only ranges owned by this cache. Unlike row
// allocation it stops before consuming the type's terminal value. Saturation is
// success, while actual allocator, retirement and cancellation errors propagate.
func (col *columnCache) consumeUnusedAutoValuesLocked(ctx context.Context, tableID, count, limit uint64, txnOp client.TxnOperator, options AutoIncrementOptions) error {
	allocationsWithoutProgress := 0
	for count > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		if col.retired {
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		if col.overflow {
			return nil
		}
		var saturated bool
		previous := count
		count, saturated = col.ranges.consumeUnused(count, limit, options)
		if count == 0 || saturated || col.terminal {
			return nil
		}
		if count < previous {
			allocationsWithoutProgress = 0
		}
		if allocationsWithoutProgress >= maxAutoIncrementAllocationsPerRow {
			return moerr.NewInternalError(ctx, "AUTO_INCREMENT unused reservation made no progress")
		}
		allocationsWithoutProgress++
		// Bound the request before converting to int or multiplying. No vector
		// proportional to count and no per-value allocator transaction is needed.
		maxCount := uint64(math.MaxInt) / options.Increment
		span := min(count, maxCount) * options.Increment
		if span == 0 {
			return moerr.NewOutOfRange(ctx, "AUTO_INCREMENT", "reservation increment exceeds allocation range")
		}
		if err := col.allocateLocked(ctx, tableID, int(span), col.concurrencyApply.Load(), txnOp); err != nil {
			return err
		}
	}
	return nil
}

// consumeUnused removes series members strictly below limit in whole runs. A
// clipped cursor stays at limit, rather than advancing to the next series member
// past it. A later real allocation still applies its own series and range check.
func (r *ranges) consumeUnused(count, limit uint64, options AutoIncrementOptions) (uint64, bool) {
	for count > 0 && len(r.values) > 0 {
		from, to := r.values[0], r.values[1]
		if from >= limit {
			return count, true
		}
		end := min(to, limit)
		value, period, ok := nextValueInRange(from, end, r.step, options.Increment, options.Offset)
		if ok {
			available := uint64(1)
			if period != 0 {
				available += (end - 1 - value) / period
			}
			take := min(count, available)
			count -= take
			// The last successor can overflow, but the consumed values cannot.
			delta, overflow := multiplyUint64(take, period)
			next, carry := addUint64(value, delta)
			if period != 0 && !overflow && !carry && next < end {
				r.values[0] = next
				return count, false
			}
		}
		if to >= limit {
			// Never move a shared cursor backwards, including when limit is not
			// part of the session series. Preserve this range's timestamp.
			next := limit
			if residue := (limit - from) % r.step; residue != 0 {
				var overflow bool
				next, overflow = addUint64(limit, r.step-residue)
				if overflow || next >= to {
					r.removeAt(0)
					return count, true
				}
			}
			r.values[0] = next
			return count, true
		}
		r.removeAt(0)
	}
	return count, false
}
