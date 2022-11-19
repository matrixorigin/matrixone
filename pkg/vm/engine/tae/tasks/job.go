package tasks

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type JobExecutor = func(context.Context) *JobResult

type JobResult struct {
	Err error
	Res any
}

type Job struct {
	id      string
	wg      *sync.WaitGroup
	ctx     context.Context
	exec    JobExecutor
	result  *JobResult
	startTs time.Time
	endTs   time.Time
}

func NewJob(id string, ctx context.Context, exec JobExecutor) *Job {
	e := &Job{
		id:   id,
		ctx:  ctx,
		exec: exec,
		wg:   new(sync.WaitGroup),
	}
	e.wg.Add(1)
	return e
}

func (job *Job) Run() {
	defer job.wg.Done()
	job.startTs = time.Now()
	defer func() {
		job.endTs = time.Now()
		logutil.Info("run-job", common.AnyField("id", job.id),
			common.ErrorField(job.result.Err),
			common.DurationField(job.endTs.Sub(job.startTs)))
	}()
	result := job.exec(job.ctx)
	job.result = result
}

func (job *Job) WaitDone() *JobResult {
	job.wg.Wait()
	return job.result
}

func (job *Job) GetResult() *JobResult {
	job.wg.Wait()
	return job.result
}

func (job *Job) Close() {
	job.result = nil
}
