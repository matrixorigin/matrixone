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

package gc

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

// manager is initialized with some cron jobs
// it doesn't support dynamically adding or removing jobs.
type Manager struct {
	// cron jobs
	jobs cronJobs
	// name index of cron jobs for dedup
	nameIdx map[string]*cronJob

	// main loop stopper
	loopStopper *stopper.Stopper

	// job process queue
	processQueue sm.Queue

	onceStart sync.Once
	onceStop  sync.Once
}

func NewManager(options ...Option) *Manager {
	mgr := &Manager{
		nameIdx: make(map[string]*cronJob),
	}
	for _, opt := range options {
		opt(mgr)
	}
	mgr.loopStopper = stopper.NewStopper("gc-loop")
	mgr.processQueue = sm.NewSafeQueue(10000, 20, mgr.process)
	return mgr
}

func (mgr *Manager) addJob(
	name string,
	interval time.Duration,
	job Job,
) {
	if _, found := mgr.nameIdx[name]; found {
		panic(moerr.NewInternalErrorNoCtx("duplicate gc job: %s", name))
	}
	cj := &cronJob{
		name:     name,
		interval: interval,
		job:      job,
	}
	mgr.nameIdx[name] = cj
	mgr.jobs = append(mgr.jobs, cj)
}

func (mgr *Manager) process(jobs ...any) {
	jobSet := make(map[string]bool)
	var dedupJobs []*cronJob
	for _, job := range jobs {
		cj := job.(*cronJob)
		if _, found := jobSet[cj.name]; found {
			continue
		} else {
			jobSet[cj.name] = true
			dedupJobs = append(dedupJobs, cj)
		}
	}
	if len(jobSet) == 0 {
		return
	}
	for _, cj := range dedupJobs {
		logutil.Debugf("processing %s", cj.String())
		if err := cj.job(context.Background()); err != nil {
			logutil.Errorf("process gc job %s: %v", cj.name, err)
		}
	}
}

// main run loop
//  1. init all gc cron jobs
//  2. loop
//     2.1 sort all cron jobs by next time
//     2.2 create a timer using the jobs' minimum next time
//     2.3
//     2.3.1 wait timer timeout. enqueue jobs with the next time before the
//     timer's timeout time into the process queue. reschdule the job
//     2.3.2 wait context timeout. exit the loop
func (mgr *Manager) loop(ctx context.Context) {
	// init all job next time
	now := time.Now()
	for _, job := range mgr.jobs {
		job.init(now)
	}

	// use the minimum job interval as the timer duration
	var timer *time.Timer

	resetTimer := func() {
		if mgr.jobs.Len() == 0 {
			timer = time.NewTimer(time.Second * time.Duration(math.MaxInt32))
		} else {
			dur := mgr.jobs[0].next.Sub(now)
			timer = time.NewTimer(dur)
		}
	}

	for {
		// sort all jobs by next time
		sort.Sort(mgr.jobs)

		// reset timer
		resetTimer()

		select {
		case <-timer.C:
			now = time.Now()
			for _, job := range mgr.jobs {
				// if job next time is after now, skip this run
				if job.after(now) {
					break
				}
				if _, err := mgr.processQueue.Enqueue(job); err != nil {
					logutil.Errorf("enqueue gc job %s: %s", job.name, err)
					break
				}
				job.reschedule(now)
			}

		// stop this loop
		case <-ctx.Done():
			logutil.Info("gc-loop is going to exit")
			return
		}
	}
}

func (mgr *Manager) Start() {
	mgr.onceStart.Do(func() {
		mgr.processQueue.Start()
		if err := mgr.loopStopper.RunNamedTask(
			"run-gc-loop",
			mgr.loop,
		); err != nil {
			panic(err)
		}
	})
}

func (mgr *Manager) Stop() {
	mgr.onceStop.Do(func() {
		mgr.loopStopper.Stop()
		mgr.processQueue.Stop()
	})
}
