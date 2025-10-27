// Copyright 2024 Matrix Origin
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

package iscp

import (
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	TriggerType_Invalid uint16 = iota
	TriggerType_Default
	TriggerType_AlwaysUpdate
	TriggerType_Timed
)

const (
	DefaultMaxChangeInterval = time.Minute * 30
)

func (triggerSpec *TriggerSpec) GetType() uint16 {
	return triggerSpec.JobType

}
func (triggerSpec *TriggerSpec) Init() {}
func (triggerSpec *TriggerSpec) Check(
	otherConsumers []*JobEntry,
	consumer *JobEntry,
	now types.TS,
) (
	ok bool, from, to types.TS, shareIteration bool,
) {
	switch triggerSpec.JobType {
	case TriggerType_Default:
		return checkDefaultJobConfig(otherConsumers, consumer, now)
	case TriggerType_AlwaysUpdate:
		return checkAlwaysUpdateJobConfig(otherConsumers, consumer, now)
	case TriggerType_Timed:
		return checkTimedJobConfig(&triggerSpec.Schedule, otherConsumers, consumer, now)
	default:
		panic(fmt.Sprintf("invalid trigger type: %d", triggerSpec.JobType))
	}
}

// DefaultJobConfig is the default implementation of JobConfig.
// For the default job type, only other default jobs are considered.
// If the current job's watermark lags behind any other default job on the same table, update it.
// If there are no lagging jobs on the same table, all jobs update.
func checkDefaultJobConfig(
	otherConsumers []*JobEntry,
	consumer *JobEntry,
	now types.TS,
) (
	ok bool, from, to types.TS, shareIteration bool,
) {
	defaultConsumers := make([]*JobEntry, 0)
	for _, c := range otherConsumers {
		if c.jobSpec.GetType() == TriggerType_Default {
			defaultConsumers = append(defaultConsumers, c)
		}
	}
	maxTS := types.TS{}
	minTS := types.MaxTs()
	for _, c := range defaultConsumers {
		if c.watermark.GT(&maxTS) {
			maxTS = c.watermark
		}
		if c.watermark.LT(&minTS) {
			minTS = c.watermark
		}
	}
	// lag behind any other default job
	if consumer.watermark.LT(&maxTS) {
		from = consumer.watermark.Next()
		to = maxTS
		if maxTS.Physical()-from.Physical() > DefaultMaxChangeInterval.Nanoseconds() {
			to = types.BuildTS(from.Physical()+DefaultMaxChangeInterval.Nanoseconds(), 0)
		}
		return true, from, to, true
	} else {
		// no lagging jobs on the same table
		if minTS.EQ(&maxTS) {
			from = maxTS.Next()
			to = now
			if to.Physical()-from.Physical() > DefaultMaxChangeInterval.Nanoseconds() {
				to = types.BuildTS(from.Physical()+DefaultMaxChangeInterval.Nanoseconds(), 0)
			}
			return true, from, to, true
		}
		return false, types.TS{}, types.TS{}, false
	}
}

// Always update the watermark for each job.
// Each job has its own iteration.
func checkAlwaysUpdateJobConfig(
	otherConsumers []*JobEntry,
	consumer *JobEntry,
	now types.TS,
) (
	ok bool, from, to types.TS, shareIteration bool,
) {
	return true, consumer.watermark.Next(), now, false
}

// TimedJobConfig is a job configuration that only updates when the time difference
// between now and watermark exceeds a specified duration.
// Check if the time difference between now and watermark exceeds the update interval.
// If ShareIteration is true, all timed jobs with the same configuration will share iteration.
func checkTimedJobConfig(
	schedule *Schedule,
	otherConsumers []*JobEntry,
	consumer *JobEntry,
	now types.TS,
) (
	ok bool, from, to types.TS, shareIteration bool,
) {
	// Calculate time difference in nanoseconds
	timeDiff := now.Physical() - consumer.watermark.Physical()

	// Only update if the time difference exceeds the update interval
	if timeDiff < schedule.Interval.Nanoseconds() {
		return false, types.TS{}, types.TS{}, false
	}

	// If sharing iteration, find other timed jobs with the same configuration
	if schedule.Share {
		timedConsumers := make([]*JobEntry, 0)
		for _, other := range otherConsumers {
			if other.jobSpec.GetType() == TriggerType_Timed && other.jobSpec.Interval == schedule.Interval {
				timedConsumers = append(timedConsumers, other)
			}
		}

		// Find the minimum watermark among all timed jobs with the same interval
		minTS := consumer.watermark
		maxTS := consumer.watermark
		for _, tc := range timedConsumers {
			if tc.watermark.LT(&minTS) {
				minTS = tc.watermark
			}
			if tc.watermark.GT(&maxTS) {
				maxTS = tc.watermark
			}
		}
		if consumer.watermark.LT(&maxTS) {
			return true, consumer.watermark.Next(), maxTS, true
		} else {
			if minTS.EQ(&maxTS) {
				return true, maxTS, now, true
			}
			return false, types.TS{}, types.TS{}, false
		}
	}

	// Individual iteration for each job
	return true, consumer.watermark.Next(), now, false
}
