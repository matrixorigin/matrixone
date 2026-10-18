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

package defines

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaterializedViewCapability(t *testing.T) {
	ctx := WithMaterializedViewWrite(context.Background(), 7, 99, []byte("transaction"))
	require.True(t, CanWriteMaterializedView(ctx, 7, 99, []byte("transaction")))
	require.False(t, CanWriteMaterializedView(ctx, 8, 99, []byte("transaction")))
	require.False(t, CanWriteMaterializedView(ctx, 7, 100, []byte("transaction")))
	require.False(t, CanWriteMaterializedView(ctx, 7, 99, []byte("different")))
	require.False(t, CanWriteMaterializedView(ctx, 7, 99, nil))
	require.False(t, MaterializedViewWriteActive(context.Background()))
}
func TestMaterializedViewOutputBudget(t *testing.T) {
	ctx := WithMaterializedViewBudget(context.Background(), 99, 100)
	require.NoError(t, ChargeMaterializedViewOutput(ctx, 100, 1000))
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Go(func() {
			if ChargeMaterializedViewOutput(ctx, 99, 10) == nil {
				accepted.Add(1)
			}
		})
	}
	wg.Wait()
	require.Equal(t, int64(10), accepted.Load())
	require.Error(t, ChargeMaterializedViewOutput(ctx, 99, 1))
	require.NoError(t, ChargeMaterializedViewOutput(context.Background(), 99, 1000))
}
