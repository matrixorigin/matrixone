// Copyright 2022 Matrix Origin
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
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	PermanentErrorThreshold = 10000
)

func NewJobEntry(
	cnUUID string,
	tableDef *plan.TableDef,
	tableInfo *TableEntry,
	sinkerConfig *ConsumerInfo,
	jobConfig JobConfig,
	watermark types.TS,
	iterationErr error,
) (*JobEntry, error) {
	consumer, err := NewConsumer(cnUUID, tableDef, sinkerConfig)
	if err != nil {
		return nil, err
	}
	jobEntry := &JobEntry{
		tableInfo:    tableInfo,
		jobName:      sinkerConfig.JobName,
		consumer:     consumer,
		consumerType: sinkerConfig.ConsumerType,
		jobConfig:    jobConfig,
		watermark:    watermark,
		err:          iterationErr,
		consumerInfo: sinkerConfig,
		state:        JobState_Finished,
	}
	jobEntry.init()
	return jobEntry, nil
}

func (jobEntry *JobEntry) init() {
	if jobEntry.watermark.IsEmpty() {
		// in the 1st iteration, toTS is determined by txn.SnapshotTS()
		iter := &Iteration{
			table: jobEntry.tableInfo,
			jobs:  []*JobEntry{jobEntry},
			to:    types.TS{},
			from:  types.TS{},
		}
		jobEntry.tableInfo.exec.worker.Submit(iter)
	} else {
		jobEntry.inited.Store(true)
	}
}

func (jobEntry *JobEntry) IsInitedAndFinished() bool {
	return jobEntry.inited.Load() && !jobEntry.PermanentError() && jobEntry.state == JobState_Finished
}

func (jobEntry *JobEntry) OnIterationFinished(iter *Iteration, offset int) {
	if !jobEntry.inited.Load() {
		if iter.err[offset] != nil {
			jobEntry.err = iter.err[offset]
			jobEntry.tableInfo.exec.worker.Submit(
				&Iteration{
					table: jobEntry.tableInfo,
					jobs:  []*JobEntry{jobEntry},
					to:    types.TS{},
					from:  types.TS{},
				},
			)
		} else {
			jobEntry.watermark = iter.to
			jobEntry.inited.Store(true)
		}
		return
	}
	if jobEntry.state != JobState_Running {
		panic("logic error")
	}
	if iter.err[offset] != nil {
		jobEntry.err = iter.err[offset]
	} else {
		jobEntry.watermark = iter.to
	}
	jobEntry.state = JobState_Finished
}

func (jobEntry *JobEntry) UpdateWatermark(from, to types.TS) {
	if from.GE(&to) {
		return
	}
	if !jobEntry.watermark.EQ(&from) {
		panic("logic error")
	}
	jobEntry.watermark = to
}

func (jobEntry *JobEntry) getConsumerInfoStr() string {
	consumerInfoStr, err := jobEntry.consumerInfo.Marshal()
	if err != nil {
		panic(err)
	}
	return string(consumerInfoStr)
}

// TODO
func (jobEntry *JobEntry) PermanentError() bool {
	return toErrorCode(jobEntry.err) > PermanentErrorThreshold
}

func (jobEntry *JobEntry) StringLocked() string {
	initStr := ""
	if !jobEntry.inited.Load() {
		initStr = "-N"
	}
	stateStr := "I"
	if jobEntry.state == JobState_Running {
		stateStr = "R"
	}
	if jobEntry.state == JobState_Finished {
		stateStr = "F"
	}
	return fmt.Sprintf(
		"Index[%s%s]%d,%s,%v[%v]",
		jobEntry.jobName,
		initStr,
		jobEntry.consumerType,
		jobEntry.watermark.ToString(),
		jobEntry.err,
		stateStr,
	)
}

func (jobEntry *JobEntry) fillInAsyncIndexLogInsertSQL(firstSinker bool, w *bytes.Buffer) error {
	if !firstSinker {
		w.WriteString(",")
	}
	jobConfigStr, err := jobEntry.jobConfig.Marshal()
	if err != nil {
		return err
	}
	_, err = w.WriteString(fmt.Sprintf(" (%d, %d,'', '%s','%s', '%s',  0, '', '', '%s', NULL)",
		jobEntry.tableInfo.accountID,
		jobEntry.tableInfo.tableID,
		jobEntry.jobName,
		string(jobConfigStr),
		jobEntry.watermark.ToString(),
		jobEntry.getConsumerInfoStr(),
	))
	return err
}

func (jobEntry *JobEntry) fillInAsyncIndexLogDeleteSQL(firstSinker bool, w *bytes.Buffer) error {
	if !firstSinker {
		w.WriteString(" OR")
	}
	_, err := w.WriteString(
		fmt.Sprintf(" (account_id = %d AND table_id = %d AND job_name = '%s' and drop_at is null)",
			jobEntry.tableInfo.accountID,
			jobEntry.tableInfo.tableID,
			jobEntry.jobName,
		))
	return err
}
