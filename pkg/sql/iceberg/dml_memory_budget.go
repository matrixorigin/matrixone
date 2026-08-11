// Copyright 2026 Matrix Origin
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

package iceberg

import (
	"context"
	"math"
	"strconv"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

// dmlMemoryBudget bounds Go-heap collector state and mpool-backed replacement
// batches with one counter. A zero limit is intentionally unlimited so the
// small, directly constructed coordinators used by embedders remain compatible;
// the runtime factory always supplies the validated configured limit.
type dmlMemoryBudget struct {
	mu    sync.Mutex
	limit int64
	used  int64
}

func newDMLMemoryBudget(limit, initial int64) *dmlMemoryBudget {
	return &dmlMemoryBudget{limit: limit, used: initial}
}

func (b *dmlMemoryBudget) reserve(ctx context.Context, bytes int64) error {
	if b == nil || bytes <= 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.limit > 0 && (bytes > b.limit-b.used) {
		return api.ToMOErr(ctx, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg DML memory limit exceeded", map[string]string{
			"used_bytes":      strconv.FormatInt(b.used, 10),
			"requested_bytes": strconv.FormatInt(bytes, 10),
			"limit_bytes":     strconv.FormatInt(b.limit, 10),
		}))
	}
	b.used = saturatingDMLAdd(b.used, bytes)
	return nil
}

// Reserve and Release intentionally expose only the accounting contract needed
// by the lower-level delete-file encoder. Keeping allocation policy here lets
// SQL DML and Iceberg encoders share one hard boundary without importing SQL
// packages from pkg/iceberg. Direct library callers may leave the budget nil.
func (b *dmlMemoryBudget) Reserve(ctx context.Context, bytes int64) error {
	return b.reserve(ctx, bytes)
}

func (b *dmlMemoryBudget) Release(bytes int64) {
	b.release(bytes)
}

func (b *dmlMemoryBudget) release(bytes int64) {
	if b == nil || bytes <= 0 {
		return
	}
	b.mu.Lock()
	b.used -= bytes
	if b.used < 0 {
		b.used = 0
	}
	b.mu.Unlock()
}

func (b *dmlMemoryBudget) usedBytes() int64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// retainedDMLBatchBytes deliberately over-counts vector and slice headers. The
// budget is a safety boundary, not a profiler; conservative accounting keeps
// allocator overhead and the short-lived Commit request views inside the cap.
func retainedDMLBatchBytes(bat *batch.Batch) int64 {
	if bat == nil {
		return 0
	}
	return saturatingDMLAdd(int64(bat.Size()), int64(128+len(bat.Vecs)*64+len(bat.Attrs)*16))
}

func retainedDMLCollectorBatchBytes(bat *batch.Batch, equalityFields int) int64 {
	if bat == nil {
		return 0
	}
	perRow := int64(160 + equalityFields*48)
	return saturatingDMLAdd(retainedDMLBatchBytes(bat), saturatingDMLMul(int64(bat.RowCount()), perRow))
}

func saturatingDMLAdd(left, right int64) int64 {
	if left >= math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func saturatingDMLMul(left, right int64) int64 {
	if left <= 0 || right <= 0 {
		return 0
	}
	if left > math.MaxInt64/right {
		return math.MaxInt64
	}
	return left * right
}
