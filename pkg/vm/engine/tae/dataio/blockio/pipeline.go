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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

var (
	errCancelJobOnStop = moerr.NewInternalErrorNoCtx("cancel job on stop")
)

var Pipeline *IoPipeline

type IOJobFactory func(context.Context, *objectio.ObjectFS, proc) *tasks.Job

// type Pipeline interface {
// 	Start()
// 	Stop()
// 	Prefetch(location string) error
// 	Fetch(ctx context.Context, location string) (any, error)
// 	AsyncFetch(ctx context.Context, location string) (tasks.Job, error)
// }

func Start() {
	if Pipeline == nil {
		Pipeline = NewIOPipeline(nil)
	}
	Pipeline.Start()
}

func Stop() {
	Pipeline.Stop()
}

func makeName(location string) string {
	return fmt.Sprintf("%s-%d", location, time.Now().UTC().Nanosecond())
}

func jobFactory(
	ctx context.Context,
	fs *objectio.ObjectFS,
	proc proc,
) *tasks.Job {
	return tasks.NewJob(
		makeName(proc.name),
		JTLoad,
		ctx,
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

func prefetchJob(ctx context.Context,
	fs *objectio.ObjectFS, pCtx prefetchCtx) *tasks.Job {
	return tasks.NewJob(
		makeName(pCtx.name),
		JTLoad,
		ctx,
		func(_ context.Context) (res *tasks.JobResult) {
			// TODO
			res = &tasks.JobResult{}
			ioVectors, err := pCtx.reader.ReadBlocks(ctx, pCtx.meta, pCtx.ids, nil, LoadZoneMapFunc, LoadColumnFunc)
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
		p.options.prefetchParallism*10,
		p.onWait)

	p.prefetch.queue = sm.NewSafeQueue(
		100000,
		p.options.prefetchParallism*2,
		p.onPrefetch)
	p.prefetch.scheduler = tasks.NewParallelJobScheduler(p.options.prefetchParallism)

	p.fetch.queue = sm.NewSafeQueue(
		100000,
		p.options.fetchParallism*2,
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
	proc proc,
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
	proc proc,
) (job *tasks.Job, err error) {
	job = p.jobFactory(
		ctx,
		p.fs,
		proc,
	)
	if _, err = p.fetch.queue.Enqueue(job); err != nil {
		job.DoneWithErr(err)
	}
	return
}

func (p *IoPipeline) Prefetch(ctx prefetchCtx) (err error) {
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

func (p *IoPipeline) onPrefetch(items ...any) {
	if len(items) == 0 {
		return
	}
	logutil.Infof("items is %d", len(items))
	processes := make([]prefetchCtx, 0)
	for _, item := range items {
		p := item.(prefetchCtx)
		processes = append(processes, p)
	}
	if !p.active.Load() {
		return
	}
	merged := mergePrefetch(processes)
	for _, object := range merged {
		/*ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()*/
		job := prefetchJob(
			context.Background(),
			p.fs,
			object,
		)
		if err := p.prefetch.scheduler.Schedule(job); err != nil {
			job.DoneWithErr(err)
			logutil.Infof("err is %v", err.Error())
		} else {
			if _, err := p.waitQ.Enqueue(job); err != nil {
				job.DoneWithErr(err)
				logutil.Infof("err is %v", err.Error())
			}
		}
	}
}

func (p *IoPipeline) onWait(jobs ...any) {
	for _, j := range jobs {
		job := j.(*tasks.Job)
		res := job.WaitDone()
		if res.Err != nil {
			logutil.Warnf("Prefetch %s err: %s", job.ID(), res.Err)
		}
		//bat := res.Res.(*fileservice.IOVector)
	}
}
