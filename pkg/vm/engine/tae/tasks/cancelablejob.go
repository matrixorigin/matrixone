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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

var DuplicateJobErr = moerr.NewInternalErrorNoCtx("duplicate job")

type CancelableJobs struct {
	sync.RWMutex
	cronJobs map[string]*CancelableJob
}

func NewCancelableJobs() *CancelableJobs {
	return &CancelableJobs{
		cronJobs: make(map[string]*CancelableJob),
	}
}

func (jobs *CancelableJobs) AddJob(
	name string,
	interval time.Duration,
	fn CancelableFunc,
	logLevel int,
) (err error) {
	job := NewCancelableCronJob(
		name,
		interval,
		fn,
		true,
		logLevel,
	)
	jobs.Lock()
	defer jobs.Unlock()
	if _, found := jobs.cronJobs[name]; found {
		return DuplicateJobErr
	}
	jobs.cronJobs[name] = job
	job.Start()
	return nil
}

func (jobs *CancelableJobs) GetJob(name string) *CancelableJob {
	jobs.RLock()
	defer jobs.RUnlock()
	return jobs.cronJobs[name]
}

func (jobs *CancelableJobs) RemoveJob(name string) {
	jobs.Lock()
	defer jobs.Unlock()
	if job, found := jobs.cronJobs[name]; found {
		job.Stop()
		delete(jobs.cronJobs, name)
	}
}

func (jobs *CancelableJobs) JobCount() int {
	jobs.RLock()
	defer jobs.RUnlock()
	return len(jobs.cronJobs)
}

func (jobs *CancelableJobs) Reset() {
	jobs.Lock()
	defer jobs.Unlock()
	for _, job := range jobs.cronJobs {
		job.Stop()
	}
	jobs.cronJobs = make(map[string]*CancelableJob)
}

func (jobs *CancelableJobs) ForeachJob(fn func(string, *CancelableJob) bool) {
	jobs.RLock()
	defer jobs.RUnlock()
	for name, job := range jobs.cronJobs {
		if !fn(name, job) {
			break
		}
	}
}

type CancelableFunc = func(context.Context)

type CancelableJob struct {
	name         string
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	fn           CancelableFunc
	logLevel     int
	isCronJob    bool
	recoverPanic bool
	interval     time.Duration
	onceStart    sync.Once
	onceStop     sync.Once
}

func NewCancelableJob(
	name string, fn CancelableFunc, logLevel int,
) *CancelableJob {
	ctl := new(CancelableJob)
	ctl.name = fmt.Sprintf("Job[%s-%s]", name, uuid.Must(uuid.NewV7()))
	ctl.fn = fn
	ctl.logLevel = logLevel
	ctl.ctx, ctl.cancel = context.WithCancel(context.Background())
	return ctl
}

func NewCancelableCronJob(
	name string,
	interval time.Duration,
	fn CancelableFunc,
	recoverPanic bool,
	logLevel int,
) *CancelableJob {
	ctl := new(CancelableJob)
	ctl.fn = fn
	ctl.isCronJob = true
	ctl.recoverPanic = recoverPanic
	ctl.logLevel = logLevel
	ctl.interval = interval
	ctl.name = fmt.Sprintf("CronJob[%s-%s-%s]", name, uuid.Must(uuid.NewV7()), interval)
	ctl.ctx, ctl.cancel = context.WithCancel(context.Background())
	return ctl
}

func (ctl *CancelableJob) Name() string {
	return ctl.name
}

func (ctl *CancelableJob) Start() {
	if ctl.isCronJob {
		ctl.onceStart.Do(func() {
			ctl.wg.Add(1)
			ticker := time.NewTicker(ctl.interval)
			if ctl.logLevel > 0 {
				logutil.Info(
					"Start-CronJob",
					zap.String("name", ctl.name),
					zap.Duration("interval", ctl.interval),
				)
			}
			go func() {
				start := time.Now()
				defer ctl.wg.Done()
				for {
					select {
					case <-ctl.ctx.Done():
						if ctl.logLevel > 0 {
							logutil.Info(
								"Stop-CronJob",
								zap.String("name", ctl.name),
								zap.Duration("duration", time.Since(start)),
							)
						}
						return
					case <-ticker.C:
						func() {
							if ctl.recoverPanic {
								defer func() {
									if r := recover(); r != nil {
										logutil.Error(
											"Panic-In-CronJob",
											zap.String("name", ctl.name),
											zap.Any("reason", r),
										)
									}
								}()
							}
							ctl.fn(ctl.ctx)
						}()
					}
				}
			}()
		})
	} else {
		ctl.onceStart.Do(func() {
			ctl.wg.Add(1)
			go func() {
				start := time.Now()
				if ctl.logLevel > 0 {
					logutil.Info(
						"Start-Job",
						zap.String("name", ctl.name),
					)
				}
				defer func() {
					ctl.wg.Done()
					if ctl.logLevel > 0 {
						logutil.Info(
							"Stop-Job",
							zap.String("name", ctl.name),
							zap.Duration("duration", time.Since(start)),
						)
					}
				}()
				ctl.fn(ctl.ctx)
			}()
		})
	}
}

func (ctl *CancelableJob) Stop() {
	ctl.onceStop.Do(func() {
		ctl.cancel()
		ctl.wg.Wait()
	})
}
