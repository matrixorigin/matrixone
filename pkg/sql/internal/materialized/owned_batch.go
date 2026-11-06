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

package materialized

import (
	"context"
	"math"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// copyBatch keeps vector bytes under physical allocation leases and Go
// metadata under a separate reservation. CTE copies retain their prior policy.
func copyBatch(ctx context.Context, input *batch.Batch, mp *mpool.MPool, config SpillConfig) (*batch.Batch, Reservation, error) {
	if config.Allocation == nil {
		copied, err := input.Dup(mp)
		return copied, nil, err
	}
	if err := context.Cause(ctx); err != nil {
		return nil, nil, err
	}
	size, err := copyMetadataSize(input)
	if err != nil {
		return nil, nil, err
	}
	charge, err := config.Budget.ReserveMemory(size)
	if err != nil {
		return nil, nil, err
	}
	if charge == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("materialized copy metadata reservation is nil")
	}
	copied := batch.NewOffHeapEmpty()
	err = copied.SetAllocationAccount(config.Allocation)
	if err == nil {
		err = copied.CopyFrom(ctx, input, mp)
	}
	if err != nil {
		copied.Clean(mp)
		charge.Release()
		return nil, nil, err
	}
	return copied, charge, nil
}

// Batch.CopyFrom temporarily owns three batch headers and one fresh set of
// vector descriptors. Attribute strings remain live with their copied headers;
// ExtraBuf is copied. The factor covers Go allocation rounding, and the stored
// entry allowance covers the source's growing slice without a parallel ledger.
func copyMetadataSize(input *batch.Batch) (uint64, error) {
	total := uint64(3*reflect.TypeFor[batch.Batch]().Size() + 4*reflect.TypeFor[storedBatch]().Size() + 128)
	add := func(count, width uint64) error {
		if count > (math.MaxUint64/2-total)/width {
			return moerr.NewInternalErrorNoCtx("materialized copy metadata size overflow")
		}
		total += count * width
		return nil
	}
	if err := add(uint64(len(input.Vecs)), uint64(reflect.TypeFor[vector.Vector]().Size()+reflect.TypeFor[*vector.Vector]().Size())); err != nil {
		return 0, err
	}
	if err := add(uint64(len(input.Attrs)), uint64(reflect.TypeFor[string]().Size())); err != nil {
		return 0, err
	}
	for _, attr := range input.Attrs {
		if err := add(uint64(len(attr)), 1); err != nil {
			return 0, err
		}
	}
	if err := add(uint64(len(input.ExtraBuf)), 1); err != nil {
		return 0, err
	}
	return 2 * total, nil
}
