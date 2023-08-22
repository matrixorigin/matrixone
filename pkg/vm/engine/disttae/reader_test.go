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

package disttae

import (
	"context"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/require"
)

func TestGatherStats(t *testing.T) {
	r := new(blockReader)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.ctx = ctx
	r.ctx, _, _ = r.prepareGatherStats()

	hitNum, readNum := rand.Int63(), rand.Int63()
	perfcounter.Update(r.ctx, func(c *perfcounter.CounterSet) {
		c.FileService.Cache.Read.Add(readNum)
		c.FileService.Cache.Hit.Add(hitNum)
		c.FileService.Cache.Memory.Read.Add(readNum)
		c.FileService.Cache.Memory.Hit.Add(hitNum)
	}, nil)

	r.gatherStats(0, 0)

	hit, total := objectio.BlkReadStats.BlkCacheHitStats.Export()
	if hitNum < readNum {
		require.Equal(t, hit, int64(0))
		require.Equal(t, total, int64(1))
	} else {
		require.Equal(t, hit, int64(1))
		require.Equal(t, total, int64(1))
	}

	hit, total = objectio.BlkReadStats.EntryCacheHitStats.Export()
	require.Equal(t, hit, hitNum)
	require.Equal(t, total, readNum)
}
