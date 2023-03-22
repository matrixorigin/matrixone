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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

var (
	_jobPool = sync.Pool{
		New: func() any {
			return new(tasks.Job)
		},
	}
)

// type pipeline interface {
// 	Start()
// 	Stop()
// 	Prefetch(location string) error
// 	Fetch(ctx context.Context, location string) (any, error)
// 	AsyncFetch(ctx context.Context, location string) (tasks.Job, error)
// }

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

var pipeline *IoPipeline

type IOJobFactory func(context.Context, fetch) *tasks.Job

func init() {
	pipeline = NewIOPipeline(nil)
}

func Start() {
	pipeline.Start()
}

func Stop() {
	pipeline.Stop()
}

func makeName(location string) string {
	return fmt.Sprintf("%s-%d", location, time.Now().UTC().Nanosecond())
}

func jobFactory(
	ctx context.Context,
	proc fetch,
) *tasks.Job {
	return getJob(
		ctx,
		makeName(proc.name),
		JTLoad,
		func(_ context.Context) (res *tasks.JobResult) {
			// TODO
			res = &tasks.JobResult{}
			ioVectors, err := proc.reader.Read(ctx, proc.meta, proc.idxes, proc.ids, nil, LoadZoneMapFunc, LoadColumnFunc)
			if err != nil {
				res.Err = err
				return
			}
			res.Res = ioVectors
			return
		},
	)
}

func prefetchJob(ctx context.Context, pref prefetch) *tasks.Job {
	return getJob(
		ctx,
		makeName(pref.name),
		JTLoad,
		func(_ context.Context) (res *tasks.JobResult) {
			// TODO
			res = &tasks.JobResult{}
			ioVectors, err := pref.reader.ReadBlocks(ctx,
				pref.meta, pref.ids, nil, LoadZoneMapFunc, LoadColumnFunc)
			if err != nil {
				res.Err = err
				return
			}
			res.Res = ioVectors
			return
		},
	)
}

func prefetchMetaJob(ctx context.Context, pref prefetch) *tasks.Job {
	return getJob(
		ctx,
		makeName(pref.name),
		JTLoad,
		func(_ context.Context) (res *tasks.JobResult) {
			// TODO
			res = &tasks.JobResult{}
			ioVectors, err := pref.reader.ReadMeta(ctx,
				[]objectio.Extent{pref.meta}, nil, LoadZoneMapFunc)
			if err != nil {
				res.Err = err
				return
			}
			res.Res = ioVectors
			return
		},
	)
}

type IoPipeline struct {
	options struct {
		fetchParallism    int
		prefetchParallism int
	}
	fetch struct {
		queue     sm.Queue
		scheduler tasks.JobScheduler
	}

	prefetch struct {
		queue     sm.Queue
		scheduler tasks.JobScheduler
	}

	fs         *objectio.ObjectFS
	waitQ      sm.Queue
	jobFactory IOJobFactory

	active    atomic.Bool
	onceStart sync.Once
	onceStop  sync.Once
}

func NewIOPipeline(
	fs *objectio.ObjectFS,
	opts ...Option,
) *IoPipeline {
	p := new(IoPipeline)
	for _, opt := range opts {
		opt(p)
	}
	p.fillDefaults()
	p.fs = fs

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
}

func (p *IoPipeline) Start() {
	p.onceStart.Do(func() {
		p.active.Store(true)
		p.waitQ.Start()
		p.fetch.queue.Start()
		p.prefetch.queue.Start()
	})
}

func (p *IoPipeline) Stop() {
	p.onceStop.Do(func() {
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
	proc fetch,
) (res any, err error) {
	job, err := p.AsyncFetch(ctx, proc)
	if err != nil {
		return
	}
	result := job.WaitDone()
	res, err = result.Res, result.Err
	return
}

func (p *IoPipeline) AsyncFetch(
	ctx context.Context,
	proc fetch,
) (job *tasks.Job, err error) {
	job = p.jobFactory(
		ctx,
		proc,
	)
	if _, err = p.fetch.queue.Enqueue(job); err != nil {
		job.DoneWithErr(err)
		putJob(job)
		job = nil
	}
	return
}

func (p *IoPipeline) Prefetch(ctx prefetch) (err error) {
	if _, err = p.prefetch.queue.Enqueue(ctx); err != nil {
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
		logutil.Infof("err is %v", err.Error())
		putJob(job)
	} else {
		if _, err := p.waitQ.Enqueue(job); err != nil {
			job.DoneWithErr(err)
			logutil.Infof("err is %v", err.Error())
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
	processes := make([]prefetch, 0)
	for _, item := range items {
		option := item.(prefetch)
		if len(option.ids) == 0 {
			job := prefetchMetaJob(
				context.Background(),
				item.(prefetch),
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
	logutil.Infof("items is %d, merged is %d", len(items), len(merged))
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
		if res.Err != nil {
			logutil.Warnf("Prefetch %s err: %s", job.ID(), res.Err)
		}
		putJob(job)
	}
}
