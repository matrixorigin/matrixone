// Copyright 2021 - 2024 Matrix Origin
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

package disttae

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/stretchr/testify/assert"
)

func TestGlobalStats_ShouldUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("basic", func(t *testing.T) {
		MinUpdateInterval = time.Millisecond * 10
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)
		assert.NotNil(t, gs)
		k1 := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}
		assert.True(t, gs.shouldUpdate(k1))
		assert.False(t, gs.shouldUpdate(k1))
		gs.doneUpdate(k1, true)
		time.Sleep(MinUpdateInterval)
		assert.True(t, gs.shouldUpdate(k1))
	})

	t.Run("parallel", func(t *testing.T) {
		MinUpdateInterval = time.Second * 10
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gs := NewGlobalStats(ctx, nil, nil)
		assert.NotNil(t, gs)
		k1 := statsinfo.StatsInfoKey{
			DatabaseID: 100,
			TableID:    101,
		}
		var count atomic.Int32
		var wg sync.WaitGroup
		updateFn := func() {
			defer wg.Done()
			if !gs.shouldUpdate(k1) {
				return
			}
			count.Add(1)
			gs.doneUpdate(k1, true)
		}
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go updateFn()
		}
		wg.Wait()
		assert.Equal(t, 1, int(count.Load()))
	})
}

func TestGlobalStats_ClearTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gs := NewGlobalStats(ctx, nil, nil)
	for i := 0; i < 10; i++ {
		gs.notifyLogtailUpdate(uint64(2000 + i))
	}
	assert.Equal(t, 10, len(gs.logtailUpdate.mu.updated))
	gs.clearTables()
	assert.Equal(t, 0, len(gs.logtailUpdate.mu.updated))
}
