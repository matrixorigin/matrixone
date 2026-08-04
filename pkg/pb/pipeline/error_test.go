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

package pipeline

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestMPoolCapacityErrorPreservesWireIdentity(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 4)
	require.NoError(t, err)
	account, err := registry.Open(2 << 20)
	require.NoError(t, err)
	pool, err := mpool.NewMPool("pipeline-mpool-capacity", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(pool)

	first, err := pool.AllocAccounted(768<<10, account, 1, 1)
	require.NoError(t, err)
	defer pool.Free(first)
	_, capacityErr := pool.AllocAccounted(768<<10, account, 1, 1)
	require.Error(t, capacityErr)
	require.IsType(t, new(moerr.Error), capacityErr)
	require.True(t, mpool.IsMPoolCapacityFailure(capacityErr))

	message := new(Message)
	message.SetMoError(context.Background(), capacityErr)

	wireErr, ok := message.TryToGetMoErr()
	require.True(t, ok)
	require.True(t, moerr.IsMoErrCode(wireErr, moerr.ErrMPoolCapacity))
	require.Contains(t, wireErr.Error(), "allocation owner=1 site=1")
}
