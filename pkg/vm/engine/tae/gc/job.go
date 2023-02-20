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
	"bytes"
	"context"
	"fmt"
	"time"
)

type Job = func(context.Context) error

type cronJob struct {
	// cron job name
	name string

	// cron job interval
	interval time.Duration

	// previous run time
	prev time.Time
	// next run time
	next time.Time

	// the underlying job
	job Job
}

func (cj *cronJob) init(t time.Time) {
	cj.next = t
}

func (cj *cronJob) reschedule(t time.Time) {
	cj.prev = cj.next
	cj.next = t.Add(cj.interval)
}

func (cj *cronJob) after(t time.Time) bool {
	return cj.next.After(t)
}

func (cj *cronJob) String() string {
	return fmt.Sprintf("job=%s, inv=%s",
		cj.name, cj.interval)
	// return fmt.Sprintf("job=%s, inv=%s, prev=%s, next=%s\n",
	// 	cj.name, cj.interval, cj.prev, cj.next)
}

type cronJobs []*cronJob

func (jobs cronJobs) Len() int      { return len(jobs) }
func (jobs cronJobs) Swap(i, j int) { jobs[i], jobs[j] = jobs[j], jobs[i] }
func (jobs cronJobs) Less(i, j int) bool {
	return jobs[i].next.Before(jobs[j].next)
}

func (jobs cronJobs) String() string {
	w := new(bytes.Buffer)
	for i, job := range jobs {
		_, _ = w.WriteString(fmt.Sprintf("%d. %s\n", i+1, job.String()))
	}
	return w.String()
}
