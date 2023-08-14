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

package blockio

import (
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/stretchr/testify/assert"
)

func TestIoPipeline_Prefetch(t *testing.T) {
	wait := sync.WaitGroup{}
	wait.Add(1)

	queueSize := 10
	batchSize := 0
	p := new(IoPipeline)

	p.prefetch.queue = sm.NewNonBlockingQueue(queueSize, batchSize, func(items ...any) {
		wait.Wait()
	})

	p.stats.prefetchDropStats.Reset()
	p.prefetch.queue.Start()

	for i := 0; i < queueSize+1; i++ {
		err := p.doPrefetch(buildPrefetchParams(nil, nil))
		assert.Nil(t, err)
		assert.Equal(t, int64(0), p.stats.prefetchDropStats.Load())
		time.Sleep(time.Millisecond * 10)
	}

	err := p.doPrefetch(buildPrefetchParams(nil, nil))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), p.stats.prefetchDropStats.Load())

	wait.Done()
	time.Sleep(time.Millisecond * 100)

}
