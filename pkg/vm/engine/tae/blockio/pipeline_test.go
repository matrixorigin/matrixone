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
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/stretchr/testify/assert"
)

func makeIOPipelineOptions(depth int) Option {
	return func(p *IoPipeline) {
		p.options.queueDepth = depth
	}
}

func makeFileService(t *testing.T) fileservice.FileService {
	dir := os.TempDir()
	fs, err := fileservice.NewLocalFS(
		context.TODO(), "local", dir, fileservice.DisabledCacheConfig, nil)
	assert.Nil(t, err)
	return fs
}

func makeLocation() objectio.Location {
	uuid, _ := types.BuildUuid()
	name := objectio.BuildObjectName(&uuid, 1)
	extent := objectio.NewExtent(1, 1, 1, 1)
	return objectio.BuildLocation(name, extent, 1, 1)
}

func makeTaskJob() *tasks.Job {
	job := _jobPool.Get().(*tasks.Job)
	job.Init(context.TODO(), "0", tasks.JTInvalid, func(context.Context) *tasks.JobResult {
		return nil
	})
	return job
}

func TestNewIOPipeline(t *testing.T) {
	p := NewIOPipeline(makeIOPipelineOptions(0))
	p.Start()
	assert.Equal(t, p.active.Load(), true)
	// waiting pipeline's queue initial done
	time.Sleep(time.Millisecond * 100)

	service := makeFileService(t)
	location := makeLocation()

	// step 1: all queue can accept item
	para := buildPrefetchParams(service, location)
	item, err := p.prefetch.queue.Enqueue(para)
	assert.Nil(t, err)
	assert.NotNil(t, item)

	item, err = p.fetch.queue.Enqueue(makeTaskJob())
	assert.Nil(t, err)
	assert.NotNil(t, item)

	// step 2: shut down all queue
	p.prefetch.queue.Stop()
	item, err = p.prefetch.queue.Enqueue(para)
	assert.Equal(t, err, sm.ErrClose)
	assert.NotNil(t, item)

	p.fetch.queue.Stop()
	item, err = p.fetch.queue.Enqueue(makeTaskJob())
	assert.Equal(t, err, sm.ErrClose)
	assert.NotNil(t, item)

	// step 3: recreate queue to make sure pipeline.close() will not try to
	// close a closed channel
	p.fetch.queue = sm.NewSafeQueue(0, 0, nil)
	p.prefetch.queue = sm.NewSafeQueue(0, 0, nil)

	// step 4: close pipeline
	p.Stop()

}

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
