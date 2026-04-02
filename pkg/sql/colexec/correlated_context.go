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

package colexec

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

type correlatedBatchesCtxKey struct{}

type correlatedBatches struct {
	batches []*batch.Batch
	row     int
}

func WithCorrelatedBatches(ctx context.Context, batches []*batch.Batch, row int) context.Context {
	return context.WithValue(ctx, correlatedBatchesCtxKey{}, correlatedBatches{
		batches: batches,
		row:     row,
	})
}

func GetCorrelatedBatches(ctx context.Context) ([]*batch.Batch, int, bool) {
	if ctx == nil {
		return nil, 0, false
	}
	value, ok := ctx.Value(correlatedBatchesCtxKey{}).(correlatedBatches)
	if !ok {
		return nil, 0, false
	}
	return value.batches, value.row, true
}
