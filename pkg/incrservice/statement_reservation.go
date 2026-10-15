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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// AutoIncrementRun transfers an already consumed cache range to one statement.
// Count rather than an exclusive end represents the integer terminal safely.
type AutoIncrementRun struct {
	First, Count, Step uint64
	AllocatedAt        timestamp.Timestamp
}

// AutoIncrementReservation is a non-row InsertValues request. The caller owns
// Runs even on error: partially acquired values must never return to the cache.
type AutoIncrementReservation struct {
	ColumnIndex int
	Type        types.Type
	Count       uint64
	Runs        []AutoIncrementRun
}

type autoIncrementReservationKey struct{}

func WithAutoIncrementReservation(ctx context.Context, request *AutoIncrementReservation) context.Context {
	return context.WithValue(ctx, autoIncrementReservationKey{}, request)
}

func AutoIncrementReservationFromContext(ctx context.Context) (*AutoIncrementReservation, bool) {
	request, ok := ctx.Value(autoIncrementReservationKey{}).(*AutoIncrementReservation)
	return request, ok
}

func (col *columnCache) reserveAutoValues(ctx context.Context, tableID uint64, request *AutoIncrementReservation, limit uint64, txn client.TxnOperator) error {
	options := AutoIncrementOptionsFromContext(ctx)
	col.Lock()
	defer col.Unlock()
	if err := col.waitPrevAllocatingLocked(ctx); err != nil {
		return err
	}
	if col.col.Step == 0 {
		return moerr.NewInvalidInput(ctx, "invalid AUTO_INCREMENT reservation step")
	}
	left := request.Count
	noProgress := 0
	atLimit := false
	for left > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		if col.retired {
			return moerr.NewTxnNeedRetryWithDefChanged(ctx)
		}
		if col.overflow {
			break
		}
		if !col.ranges.empty() {
			from, to := col.ranges.values[0], col.ranges.values[1]
			// Unused reservation may reach the bound, but never owns it. A
			// reservation starting at the bound is a real request for that row.
			end := to
			if from < limit {
				end = min(end, limit)
			}
			first, step, ok := nextValueInRange(from, end, col.ranges.step, options.Increment, options.Offset)
			if !ok {
				if to >= limit && end == limit {
					break
				}
				col.ranges.removeAt(0)
				continue
			}
			if first > limit || first == math.MaxUint64 {
				break
			}
			count := uint64(1)
			if step != 0 {
				count += (min(end-1, limit) - first) / step
			}
			count = min(left, count)
			allocatedAt := col.ranges.oldestAllocateAt()
			// Coalesce contiguous leases even when AUTO_ID_CACHE=1. The
			// oldest timestamp conservatively covers every value in the run.
			merged := false
			if len(request.Runs) > 0 {
				prev := &request.Runs[len(request.Runs)-1]
				delta, overflow := multiplyUint64(prev.Count, prev.Step)
				next, carry := addUint64(prev.First, delta)
				if !overflow && !carry && prev.Step == step && next == first {
					prev.Count += count
					if allocatedAt.Less(prev.AllocatedAt) {
						prev.AllocatedAt = allocatedAt
					}
					merged = true
				}
			}
			if !merged {
				request.Runs = append(request.Runs, AutoIncrementRun{First: first, Count: count, Step: step, AllocatedAt: allocatedAt})
			}
			last := first + (count-1)*step
			next, overflow := addUint64(last, col.col.Step)
			if overflow || next >= to {
				col.ranges.removeAt(0)
			} else {
				col.ranges.values[0] = next
			}
			left -= count
			noProgress = 0
			if end == limit || last == limit {
				atLimit = last == limit || step == 0 || end-last <= step
				if left > 0 && end == limit {
					col.ranges.consumeUnused(left, limit, options)
				}
				break
			}
			continue
		}
		if col.terminal {
			break
		} // uint64's all-ones value is not a generated ID.
		if noProgress >= maxAutoIncrementAllocationsPerRow {
			return moerr.NewInternalError(ctx, "AUTO_INCREMENT reservation made no progress")
		}
		noProgress++
		span := min(left, uint64(math.MaxInt)/options.Increment) * options.Increment
		if span == 0 {
			return moerr.NewOutOfRange(ctx, "AUTO_INCREMENT", "reservation increment exceeds allocation range")
		}
		if err := col.allocateLocked(ctx, tableID, int(span), col.concurrencyApply.Load(), txn); err != nil {
			return err
		}
	}
	if len(request.Runs) == 0 {
		return moerr.NewOutOfRange(ctx, "AUTO_INCREMENT", "no next value is representable")
	}
	// Near the integer bound the reservation high-water mark saturates one
	// session offset before arithmetic overflow. These extra unused slots belong
	// to the cache, not to the statement's reusable run.
	first := request.Runs[0].First
	span, overflow := multiplyUint64(request.Count, options.Increment)
	free := limit - first
	if !atLimit && (overflow || span >= limit || free <= options.Offset || free-options.Offset <= span) {
		if err := col.consumeUnusedAutoValuesLocked(ctx, tableID, 1, limit, txn, options); err != nil {
			return err
		}
	}
	return nil
}
