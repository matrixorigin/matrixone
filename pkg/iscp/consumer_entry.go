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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	PermanentErrorThreshold = 10000
)

func NewJobEntry(
	tableInfo *TableEntry,
	jobName string,
	jobSpec *JobSpec,
	watermark types.TS,
	state int8,
) *JobEntry {
	jobEntry := &JobEntry{
		tableInfo: tableInfo,
		jobName:   jobName,
		jobSpec:   &jobSpec.TriggerSpec,
		watermark: watermark,
		state:     state,
	}
	jobEntry.init()
	return jobEntry
}

func (jobEntry *JobEntry) init() {
	if jobEntry.watermark.IsEmpty() {
		// in the 1st iteration, toTS is determined by txn.SnapshotTS()
		iterCtx := &IterationContext{
			accountID: jobEntry.tableInfo.accountID,
			tableID:   jobEntry.tableInfo.tableID,
			jobNames:  []string{jobEntry.jobName},
			toTS:      types.TS{},
			fromTS:    types.TS{},
		}
		jobEntry.tableInfo.exec.worker.Submit(iterCtx)
	} else {
		jobEntry.inited.Store(true)
	}
}

func (jobEntry *JobEntry) IsInitedAndFinished() bool {
	return jobEntry.inited.Load() && jobEntry.state == ISCPJobState_Completed
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

func (jobEntry *JobEntry) StringLocked() string {
	initStr := ""
	if !jobEntry.inited.Load() {
		initStr = "-N"
	}
	stateStr := "I"
	switch jobEntry.state {
	case ISCPJobState_Running:
		stateStr = "R"
	case ISCPJobState_Completed:
		stateStr = "F"
	case ISCPJobState_Error:
		stateStr = "E"
	case ISCPJobState_Canceled:
		stateStr = "C"
	}
	return fmt.Sprintf(
		"Index[%s%s]%s,%v[%v]",
		jobEntry.jobName,
		initStr,
		jobEntry.watermark.ToString(),
		jobEntry.jobSpec,
		stateStr,
	)
}
