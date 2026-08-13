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

package ioutil

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	rt "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

type inlineJobScheduler struct{}

func (inlineJobScheduler) Schedule(job *tasks.Job) error {
	job.Run()
	return nil
}

func (inlineJobScheduler) Stop() {}

type rejectingJobQueue struct{}

func (rejectingJobQueue) Start() {}
func (rejectingJobQueue) Stop()  {}
func (rejectingJobQueue) Enqueue(item any) (any, error) {
	return item, sm.ErrClose
}

func TestSchedulerPrefetchDoesNotCompleteAcceptedJobTwice(t *testing.T) {
	pipeline := &IoPipeline{waitQ: rejectingJobQueue{}}
	pipeline.prefetch.scheduler = inlineJobScheduler{}

	job := new(tasks.Job)
	job.Init(context.Background(), "completed-before-wait-queue", tasks.JTInvalid,
		func(context.Context) *tasks.JobResult {
			return &tasks.JobResult{}
		})

	require.NotPanics(t, func() {
		pipeline.schedulerPrefetch(job)
	})
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

func TestStartAfterStopCreatesActivePipeline(t *testing.T) {
	const sid = "pipeline-restart"
	rt.SetupServiceBasedRuntime(sid, rt.DefaultRuntime())

	Start(sid)
	first := MustGetPipeline(sid)
	require.True(t, first.active.Load())

	Stop(sid)
	require.False(t, first.active.Load())

	Start(sid)
	second := MustGetPipeline(sid)
	t.Cleanup(func() { Stop(sid) })
	require.NotSame(t, first, second)
	require.True(t, second.active.Load())
}

func TestPrefetchRejectsInvalidLocationBeforeEnqueue(t *testing.T) {
	validLocation := makeLocation()
	params, err := BuildPrefetchParams(nil, validLocation)
	require.NoError(t, err)
	require.Equal(t, validLocation, params.key)
	delegateCalls := 0
	pipeline := &IoPipeline{
		prefetchFunc: func(PrefetchParams) error {
			delegateCalls++
			return nil
		},
	}
	require.NoError(t, pipeline.Prefetch(params))
	require.Equal(t, 1, delegateCalls)

	invalidLocations := []struct {
		name     string
		location objectio.Location
	}{
		{name: "missing encoding"},
		{
			name: "truncated encoding",
			location: append(
				objectio.Location{1}, make(objectio.Location, objectio.LocationLen-2)...,
			),
		},
		{name: "zero object name", location: make(objectio.Location, objectio.LocationLen)},
	}
	prefetchers := []struct {
		name     string
		prefetch func(objectio.Location) error
	}{
		{
			name: "file",
			prefetch: func(location objectio.Location) error {
				return Prefetch(t.Name(), nil, location)
			},
		},
		{
			name: "metadata",
			prefetch: func(location objectio.Location) error {
				return PrefetchMeta(t.Name(), nil, location)
			},
		},
		{
			name: "pipeline admission",
			prefetch: func(location objectio.Location) error {
				return pipeline.Prefetch(PrefetchParams{key: location})
			},
		},
	}

	for _, location := range invalidLocations {
		t.Run(location.name, func(t *testing.T) {
			for _, prefetcher := range prefetchers {
				t.Run(prefetcher.name, func(t *testing.T) {
					err := prefetcher.prefetch(location.location)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
				})
			}
		})
	}
	require.Equal(t, 1, delegateCalls)
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
