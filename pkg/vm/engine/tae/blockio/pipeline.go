// Copyright 2021 Matrix Origin
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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	w "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

var (
	_jobPool = sync.Pool{
		New: func() any {
			return new(tasks.Job)
		},
	}
	_readerPool = sync.Pool{
		New: func() any {
			return new(objectio.ObjectReader)
		},
	}
)

func getJob(
	ctx context.Context,
	id string,
	typ tasks.JobType,
	exec tasks.JobExecutor) *tasks.Job {
	job := _jobPool.Get().(*tasks.Job)
	job.Init(ctx, id, typ, exec)
	return job
}

func putJob(job *tasks.Job) {
	job.Reset()
	_jobPool.Put(job)
}

func getReader(
	fs fileservice.FileService,
	location objectio.Location) *objectio.ObjectReader {
	job := _readerPool.Get().(*objectio.ObjectReader)
	job.Init(location, fs)
	return job
}

func putReader(reader *objectio.ObjectReader) {
	reader.Reset()
	_readerPool.Put(reader)
}

// At present, the read and write operations of all modules of mo-service use blockio.
// I have started/stopped IoPipeline when mo is initialized/stopped, but in order to
// be compatible with the UT of each module, I must add readColumns and noopPrefetch.

// Most UT cases do not call Start(), so in order to be compatible with these cases,
// the pipeline uses readColumns and noopPrefetch.In order to avoid the data race of UT,
// I did not switch pipeline.fetchFun and pipeline.prefetchFunc when
// I stopped, so I need to execute ResetPipeline again

var pipeline *IoPipeline

type IOJobFactory func(context.Context, fetchParams) *tasks.Job

func init() {
	pipeline = NewIOPipeline()
}

func Start() {
	pipeline.Start()
	pipeline.fetchFun = pipeline.doFetch
	pipeline.prefetchFunc = pipeline.doPrefetch
}

func Stop() {
	pipeline.Stop()
}

func ResetPipeline() {
	pipeline = NewIOPipeline()
}

func makeName(location string) string {
	return fmt.Sprintf("%s-%d", location, time.Now().UTC().Nanosecond())
}

// load data job
func jobFactory(
	ctx context.Context,
	params fetchParams,
) *tasks.Job {
	return getJob(
		ctx,
		makeName(params.reader.GetName()),
		JTLoad,
		func(_ context.Context) (res *tasks.JobResult) {
			// TODO
			res = &tasks.JobResult{}
			ioVectors, err := readColumns(ctx, params)
			if err == nil {
				res.Res = ioVectors
			} else {
				res.Err = err
			}
			return
		},
	)
}

func fetchReader(params prefetchParams) (reader *objectio.ObjectReader) {
	if params.reader != nil {
		reader = params.reader
	} else {
		reader = getReader(params.fs, params.key)
	}
	return
}

// prefetch data job
func prefetchJob(ctx context.Context, params prefetchParams) *tasks.Job {
	reader := fetchReader(params)
	return getJob(
		ctx,
		makeName(reader.GetName()),
		JTLoad,
		func(_ context.Context) (res *tasks.JobResult) {
			// TODO
			res = &tasks.JobResult{}
			if params.dataType == objectio.CkpMetaStart {
				ioVectors, err := reader.ReadMultiSubBlocks(ctx, params.ids, nil)
				if err != nil {
					res.Err = err
					return
				}
				res.Res = ioVectors
			} else {
				ioVectors, err := reader.ReadMultiBlocks(ctx,
					params.ids, nil)
				if err != nil {
					res.Err = err
					return
				}
				res.Res = ioVectors
			}
			if params.reader == nil {
				putReader(reader)
			}
			return
		},
	)
}

// prefetch metadata job
func prefetchMetaJob(ctx context.Context, params prefetchParams) *tasks.Job {
	reader := fetchReader(params)
	return getJob(
		ctx,
		makeName(reader.GetName()),
		JTLoad,
		func(_ context.Context) (res *tasks.JobResult) {
			// TODO
			res = &tasks.JobResult{}
			logutil.Infof("sdfsdfs2222")
			ioVectors, err := reader.ReadMeta(ctx, nil)
			if err != nil {
				res.Err = err
				logutil.Infof("sdfsdfs")
				return
			}
			res.Res = ioVectors
			if params.reader == nil {
				putReader(reader)
			}
			return
		},
	)
}

type FetchFunc = func(ctx context.Context, params fetchParams) (any, error)
type PrefetchFunc = func(params prefetchParams) error

func readColumns(ctx context.Context, params fetchParams) (any, error) {
	return params.reader.ReadOneBlock(ctx, params.idxes, params.typs, params.blk, nil)
}

func noopPrefetch(params prefetchParams) error {
	// Synchronous prefetch does not need to do anything
	return nil
}

type IoPipeline struct {
	options struct {
		fetchParallism    int
		prefetchParallism int
	}
	// load queue
	fetch struct {
		queue     sm.Queue
		scheduler tasks.JobScheduler
	}

	// prefetch queue
	prefetch struct {
		queue     sm.Queue
		scheduler tasks.JobScheduler
	}

	waitQ      sm.Queue
	jobFactory IOJobFactory

	active    atomic.Bool
	onceStart sync.Once
	onceStop  sync.Once

	fetchFun     FetchFunc
	prefetchFunc PrefetchFunc

	stats        *objectio.Stats
	statsManager *stopper.Stopper
}

func NewIOPipeline(
	opts ...Option,
) *IoPipeline {
	p := new(IoPipeline)
	for _, opt := range opts {
		opt(p)
	}
	p.fillDefaults()

	p.waitQ = sm.NewSafeQueue(
		100000,
		100,
		p.onWait)

	p.prefetch.queue = sm.NewSafeQueue(
		100000,
		64,
		p.onPrefetch)
	p.prefetch.scheduler = tasks.NewParallelJobScheduler(p.options.prefetchParallism)

	p.fetch.queue = sm.NewSafeQueue(
		100000,
		64,
		p.onFetch)
	p.fetch.scheduler = tasks.NewParallelJobScheduler(p.options.fetchParallism)

	p.fetchFun = readColumns
	p.prefetchFunc = noopPrefetch

	p.statsManager = stopper.NewStopper("PrintStatsRunner")
	return p
}

func (p *IoPipeline) fillDefaults() {
	if p.options.fetchParallism <= 0 {
		p.options.fetchParallism = runtime.NumCPU() * 4
	}
	if p.options.prefetchParallism <= 0 {
		p.options.prefetchParallism = runtime.NumCPU() * 4
	}
	if p.jobFactory == nil {
		p.jobFactory = jobFactory
	}

	if p.stats == nil {
		p.stats = objectio.NewStats()
	}
}

func (p *IoPipeline) Start() {
	p.onceStart.Do(func() {
		p.active.Store(true)
		p.waitQ.Start()
		p.fetch.queue.Start()
		p.prefetch.queue.Start()
		if err := p.statsManager.RunNamedTask("print-stats-job", p.crontask); err != nil {
			panic(err)
		}
	})
}

func (p *IoPipeline) Stop() {
	p.onceStop.Do(func() {
		p.statsManager.Stop()
		p.active.Store(false)

		p.prefetch.queue.Stop()
		p.fetch.queue.Stop()

		p.prefetch.scheduler.Stop()
		p.fetch.scheduler.Stop()

		p.waitQ.Stop()
	})
}

func (p *IoPipeline) Fetch(
	ctx context.Context,
	params fetchParams,
) (res any, err error) {
	return p.fetchFun(ctx, params)
}

func (p *IoPipeline) doAsyncFetch(
	ctx context.Context,
	params fetchParams,
) (job *tasks.Job, err error) {
	job = p.jobFactory(
		ctx,
		params,
	)
	if _, err = p.fetch.queue.Enqueue(job); err != nil {
		job.DoneWithErr(err)
		putJob(job)
		job = nil
	}
	return
}

func (p *IoPipeline) Prefetch(params prefetchParams) (err error) {
	return p.prefetchFunc(params)
}

func (p *IoPipeline) doFetch(
	ctx context.Context,
	params fetchParams,
) (res any, err error) {
	job, err := p.doAsyncFetch(ctx, params)
	if err != nil {
		return
	}
	result := job.WaitDone()
	res, err = result.Res, result.Err
	putJob(job)
	return
}

func (p *IoPipeline) doPrefetch(params prefetchParams) (err error) {
	if _, err = p.prefetch.queue.Enqueue(params); err != nil {
		return
	}
	return
}

func (p *IoPipeline) onFetch(jobs ...any) {
	for _, j := range jobs {
		job := j.(*tasks.Job)
		if err := p.fetch.scheduler.Schedule(job); err != nil {
			job.DoneWithErr(err)
		}
	}
}

func (p *IoPipeline) schedulerPrefetch(job *tasks.Job) {
	if err := p.prefetch.scheduler.Schedule(job); err != nil {
		job.DoneWithErr(err)
		logutil.Debugf("err is %v", err.Error())
		putJob(job)
	} else {
		if _, err := p.waitQ.Enqueue(job); err != nil {
			job.DoneWithErr(err)
			logutil.Debugf("err is %v", err.Error())
			putJob(job)
		}
	}
}

func (p *IoPipeline) onPrefetch(items ...any) {
	if len(items) == 0 {
		return
	}
	if !p.active.Load() {
		return
	}
	processes := make([]prefetchParams, 0)
	for _, item := range items {
		option := item.(prefetchParams)
		if len(option.ids) == 0 {
			job := prefetchMetaJob(
				context.Background(),
				item.(prefetchParams),
			)
			p.schedulerPrefetch(job)
			continue
		}
		processes = append(processes, option)
	}
	if len(processes) == 0 {
		return
	}
	merged := mergePrefetch(processes)
	for _, option := range merged {
		job := prefetchJob(
			context.Background(),
			option,
		)
		p.schedulerPrefetch(job)
	}
}

func (p *IoPipeline) onWait(jobs ...any) {
	for _, j := range jobs {
		job := j.(*tasks.Job)
		res := job.WaitDone()
		if res == nil {
			logutil.Infof("job is %v", job.String())
		}
		if res.Err != nil {
			logutil.Warnf("Prefetch %s err: %s", job.ID(), res.Err)
		}
		putJob(job)
	}
}

func (p *IoPipeline) crontask(ctx context.Context) {
	hb := w.NewHeartBeaterWithFunc(time.Second*10, func() {
		logutil.Info(p.stats.ExportString())
	}, nil)
	hb.Start()
	<-ctx.Done()
	hb.Stop()
}
