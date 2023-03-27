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

package tasks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/panjf2000/ants/v2"
)

type JobType = uint16

const (
	JTAny             JobType = iota
	JTCustomizedStart         = 100

	JTInvalid = 10000
)

var jobTypeNames = map[JobType]string{
	JTAny: "AnyJob",
}

func RegisterJobType(jt JobType, jn string) {
	_, ok := jobTypeNames[jt]
	if ok {
		panic(any(moerr.NewInternalErrorNoCtx("duplicate job type: %d", jt)))
	}
	jobTypeNames[jt] = jn
}

func JobName(jt JobType) string {
	n, ok := jobTypeNames[jt]
	if !ok {
		panic(any(moerr.NewInternalErrorNoCtx("specified job type: %d not found", jt)))
	}
	return n
}

type JobScheduler interface {
	Schedule(job *Job) error
	Stop()
}

type JobExecutor = func(context.Context) *JobResult

type JobResult struct {
	Err error
	Res any
}

var SerialJobScheduler = new(simpleJobSceduler)

type simpleJobSceduler struct{}

func (s *simpleJobSceduler) Stop() {}
func (s *simpleJobSceduler) Schedule(job *Job) (err error) {
	job.Run()
	return
}

type parallelJobScheduler struct {
	pool *ants.Pool
}

func NewParallelJobScheduler(parallism int) *parallelJobScheduler {
	pool, err := ants.NewPool(parallism)
	if err != nil {
		panic(any(err))
	}
	return &parallelJobScheduler{
		pool: pool,
	}
}

func (s *parallelJobScheduler) Stop() {
	s.pool.Release()
	s.pool = nil
}

func (s *parallelJobScheduler) Schedule(job *Job) (err error) {
	err = s.pool.Submit(job.Run)
	return
}

type Job struct {
	id      string
	typ     JobType
	wg      sync.WaitGroup
	ctx     context.Context
	exec    JobExecutor
	result  *JobResult
	startTs time.Time
	endTs   time.Time
}

func (job *Job) Run() {
	defer job.wg.Done()
	job.startTs = time.Now()
	defer func() {
		job.endTs = time.Now()
		/*logutil.Debug("run-job", common.AnyField("name", job.String()),
		common.ErrorField(job.result.Err),
		common.DurationField(job.endTs.Sub(job.startTs)))*/
	}()
	result := job.exec(job.ctx)
	job.result = result
}

func (job *Job) ID() string {
	return job.id
}

func (job *Job) String() string {
	return fmt.Sprintf("Job[%s]-[%s]", JobName(job.typ), job.id)
}

func (job *Job) Type() JobType {
	return job.typ
}

func (job *Job) WaitDone() *JobResult {
	job.wg.Wait()
	return job.result
}

func (job *Job) GetResult() *JobResult {
	job.wg.Wait()
	return job.result
}

func (job *Job) DoneWithErr(err error) {
	defer job.wg.Done()
	job.result = &JobResult{
		Err: err,
	}
}

func (job *Job) Close() {
	job.result = nil
}

func (job *Job) Reset() {
	job.result = nil
	job.typ = JTInvalid
	job.id = ""
	job.ctx = nil
	job.wg = sync.WaitGroup{}
	job.exec = nil
}

func (job *Job) Init(
	ctx context.Context,
	id string,
	typ JobType,
	exec JobExecutor) {
	job.id = id
	job.ctx = ctx
	job.exec = exec
	job.typ = typ
	job.wg.Add(1)
}
