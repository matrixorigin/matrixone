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

	rt "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common/utils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	w "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
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

type IOJobFactory func(context.Context, fetchParams) *tasks.Job

func Start(sid string) {
	r := rt.ServiceRuntime(sid)
	_, ok := r.GetGlobalVariables("blockio")
	if ok {
		return
	}

	pipeline := NewIOPipeline()
	pipeline.Start()
	pipeline.fetchFun = pipeline.doFetch
	pipeline.prefetchFunc = pipeline.doPrefetch
	r.SetGlobalVariables("blockio", pipeline)
}

func Stop(sid string) {
	v, ok := rt.ServiceRuntime(sid).GetGlobalVariables("blockio")
	if !ok {
		return
	}
	v.(*IoPipeline).Stop()
}

func MustGetPipeline(sid string) *IoPipeline {
	v, ok := rt.ServiceRuntime(sid).GetGlobalVariables("blockio")
	if !ok {
		panic("blockio not started for " + sid)
	}
	return v.(*IoPipeline)
}

func GetPipeline(sid string) *IoPipeline {
	v, ok := rt.ServiceRuntime(sid).GetGlobalVariables("blockio")
	if !ok {
		return nil
	}
	return v.(*IoPipeline)
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

func fetchReader(params PrefetchParams) (reader *objectio.ObjectReader) {
	reader = getReader(params.fs, params.key)
	return
}

// prefetch data job
func prefetchJob(ctx context.Context, params PrefetchParams) *tasks.Job {
	reader := fetchReader(params)
	return getJob(
		ctx,
		makeName(reader.GetName()),
		JTLoad,
		func(_ context.Context) (res *tasks.JobResult) {
			// TODO
			res = &tasks.JobResult{}
			err := reader.GetFs().PrefetchFile(ctx, params.key.Name().String())
			if err != nil {
				res.Err = err
				return
			}
			// no further reads
			putReader(reader)
			return
		},
	)
}

// prefetch metadata job
func prefetchMetaJob(ctx context.Context, params PrefetchParams) *tasks.Job {
	name := params.key.Name().String()
	return getJob(
		ctx,
		makeName(name),
		JTLoad,
		func(_ context.Context) (res *tasks.JobResult) {
			res = &tasks.JobResult{}
			_, err := objectio.FastLoadObjectMeta(ctx, &params.key, true, params.fs)
			if err != nil {
				res.Err = err
				return
			}
			return
		},
	)
}

type FetchFunc = func(ctx context.Context, params fetchParams) (any, error)
type PrefetchFunc = func(params PrefetchParams) error

func readColumns(ctx context.Context, params fetchParams) (any, error) {
	return params.reader.ReadOneBlock(ctx, params.idxes, params.typs, params.blk, nil)
}

func noopPrefetch(params PrefetchParams) error {
	// Synchronous prefetch does not need to do anything
	return nil
}

type IoPipeline struct {
	options struct {
		fetchParallism    int
		prefetchParallism int
		queueDepth        int
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

	sensors struct {
		prefetchDepth *utils.NumericSensor[int64]
	}

	stats struct {
		prefetchDropStats stats.Counter
	}
	printer *stopper.Stopper
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
		p.options.queueDepth,
		100,
		p.onWait)

	// the prefetch queue is supposed to be an unblocking queue
	p.prefetch.queue = sm.NewNonBlockingQueue(p.options.queueDepth, 64, p.onPrefetch)
	p.prefetch.scheduler = tasks.NewParallelJobScheduler(p.options.prefetchParallism)

	p.fetch.queue = sm.NewSafeQueue(
		p.options.queueDepth,
		64,
		p.onFetch)
	p.fetch.scheduler = tasks.NewParallelJobScheduler(p.options.fetchParallism)

	p.fetchFun = readColumns
	p.prefetchFunc = noopPrefetch

	p.printer = stopper.NewStopper("IOPrinter")
	return p
}

func (p *IoPipeline) fillDefaults() {
	procs := runtime.GOMAXPROCS(0)
	if p.options.fetchParallism <= 0 {
		p.options.fetchParallism = procs * 4
	}
	if p.options.prefetchParallism <= 0 {
		p.options.prefetchParallism = procs * 4
	}
	if p.options.queueDepth <= 0 {
		p.options.queueDepth = 100000
	}
	if p.jobFactory == nil {
		p.jobFactory = jobFactory
	}

	if p.sensors.prefetchDepth == nil {
		name := utils.MakeSensorName("IO", "PrefetchDepth")
		sensor := utils.NewNumericSensor[int64](
			name,
			utils.WithGetStateSensorOption(
				func(v int64) utils.SensorState {
					if float64(v) < 0.6*float64(p.options.queueDepth) {
						return utils.SensorStateGreen
					} else if float64(v) < 0.8*float64(p.options.queueDepth) {
						return utils.SensorStateYellow
					} else {
						return utils.SensorStateRed
					}
				},
			),
		)
		utils.RegisterSensor(sensor)
		p.sensors.prefetchDepth = sensor
	}
}

func (p *IoPipeline) Start() {
	p.onceStart.Do(func() {
		p.active.Store(true)
		p.waitQ.Start()
		p.fetch.queue.Start()
		p.prefetch.queue.Start()
		if err := p.printer.RunNamedTask("io-printer-job", p.crontask); err != nil {
			panic(err)
		}
	})
}

func (p *IoPipeline) Stop() {
	p.onceStop.Do(func() {
		p.printer.Stop()
		p.active.Store(false)

		p.prefetch.queue.Stop()
		p.fetch.queue.Stop()

		p.prefetch.scheduler.Stop()
		p.fetch.scheduler.Stop()

		p.waitQ.Stop()
		if p.sensors.prefetchDepth != nil {
			utils.UnregisterSensor(p.sensors.prefetchDepth)
			p.sensors.prefetchDepth = nil
		}
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

func (p *IoPipeline) Prefetch(params PrefetchParams) (err error) {
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

func (p *IoPipeline) doPrefetch(params PrefetchParams) (err error) {
	if _, err = p.prefetch.queue.Enqueue(params); err == sm.ErrFull {
		p.stats.prefetchDropStats.Add(1)
	}
	// prefetch doesn't care about what type of err has occurred
	return nil
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

	var metaProcesses, filesProcesses []PrefetchParams
	for _, item := range items {
		option := item.(PrefetchParams)
		switch option.typ {
		case PrefetchMetaType:
			if len(metaProcesses) == 0 {
				metaProcesses = make([]PrefetchParams, 0)
			}
			metaProcesses = append(metaProcesses, option)
			job := prefetchMetaJob(
				context.Background(),
				item.(PrefetchParams),
			)
			p.schedulerPrefetch(job)
		case PrefetchFileType:
			if len(filesProcesses) == 0 {
				filesProcesses = make([]PrefetchParams, 0)
			}
			filesProcesses = append(filesProcesses, option)
		default:
			logutil.Errorf("unknown prefetch type %v", option.typ)
		}
	}
	schedulerJobs := func(
		processes []PrefetchParams,
		onJob func(context.Context, PrefetchParams) *tasks.Job,
	) {
		merged := mergePrefetch(processes)
		for _, option := range merged {
			job := onJob(context.Background(), option)
			p.schedulerPrefetch(job)
		}
	}
	if len(metaProcesses) > 0 {
		go schedulerJobs(metaProcesses, prefetchMetaJob)
	}
	if len(filesProcesses) > 0 {
		go schedulerJobs(filesProcesses, prefetchJob)
	}
}

func (p *IoPipeline) onWait(jobs ...any) {
	for _, j := range jobs {
		job := j.(*tasks.Job)
		res := job.WaitDone()
		if res == nil {
			logutil.Infof("job is %v", job.String())
			putJob(job)
			return
		}
		if res.Err != nil {
			logutil.Warnf("Prefetch %s err: %s", job.ID(), res.Err)
		}
		putJob(job)
	}
}

func (p *IoPipeline) crontask(ctx context.Context) {
	hb := w.NewHeartBeaterWithFunc(time.Second*10, func() {
	}, nil)
	hb.Start()
	<-ctx.Done()
	hb.Stop()
}

func RunPipelineTest(
	fn func(),
) {
	rt.RunTest(
		"",
		func(rt rt.Runtime) {
			Start("")
			defer Stop("")
			fn()
		},
	)
}
