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
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/zap"
)

const (
	PermanentErrorThreshold = 10000
)

func NewJobEntry(
	tableInfo *TableEntry,
	jobName string,
	jobSpec *JobSpec,
	jobID uint64,
	watermark types.TS,
	state int8,
) *JobEntry {
	jobEntry := &JobEntry{
		tableInfo: tableInfo,
		jobName:   jobName,
		jobID:     jobID,
		jobSpec:   &jobSpec.TriggerSpec,
		watermark: watermark,
		state:     state,
	}
	return jobEntry
}

func (jobEntry *JobEntry) update(jobSpec *JobSpec, watermark types.TS, state int8) {
	if jobSpec.GetType() != jobEntry.jobSpec.GetType() && watermark.LT(&jobEntry.watermark) {
		jobEntry.jobSpec = &jobSpec.TriggerSpec
		return
	}
	if jobEntry.persistedWatermark.EQ(&watermark) {
		return
	}
	if watermark.LT(&jobEntry.watermark) {
		panic(fmt.Sprintf("watermark %v < %v, current state %d, incoming state %d, job %v", watermark.ToString(), jobEntry.watermark.ToString(), jobEntry.state, state, jobEntry.jobName))
	}
	jobEntry.jobSpec = &jobSpec.TriggerSpec
	jobEntry.persistedWatermark = watermark
	jobEntry.watermark = watermark
	jobEntry.state = state
}

func (jobEntry *JobEntry) IsInitedAndFinished() bool {
	return jobEntry.state == ISCPJobState_Completed
}

func (jobEntry *JobEntry) UpdateWatermark(
	from, to types.TS,
	watermarkFlushThreshold time.Duration,
) {
	if from.GE(&to) {
		return
	}
	if !jobEntry.watermark.EQ(&from) {
		panic("logic error")
	}
	jobEntry.watermark = to
}

func (jobEntry *JobEntry) tryFlushWatermark(
	ctx context.Context,
	txn client.TxnOperator,
	threshold time.Duration,
) (needFlush bool, err error) {
	if jobEntry.state != ISCPJobState_Completed ||
		jobEntry.watermark.Physical()-jobEntry.persistedWatermark.Physical() < threshold.Nanoseconds() {
		return
	}
	needFlush = true
	emptyStatus := &JobStatus{}
	statusJson, err := MarshalJobStatus(emptyStatus)
	if err != nil {
		return
	}
	sql := cdc.CDCSQLBuilder.ISCPLogUpdateResultSQL(
		jobEntry.tableInfo.accountID,
		jobEntry.tableInfo.tableID,
		jobEntry.jobName,
		jobEntry.jobID,
		jobEntry.watermark,
		statusJson,
		ISCPJobState_Completed,
	)
	_, err = ExecWithResult(
		ctx,
		sql,
		jobEntry.tableInfo.exec.cnUUID,
		txn,
	)
	if err != nil {
		logutil.Error(
			"ISCP-Task flush watermark failed",
			zap.String("job", jobEntry.jobName),
			zap.String("table", jobEntry.tableInfo.tableName),
			zap.String("account", jobEntry.tableInfo.dbName),
			zap.Uint64("tableID", jobEntry.tableInfo.tableID),
			zap.Uint64("jobID", jobEntry.jobID),
			zap.String("watermark", jobEntry.watermark.ToString()),
			zap.String("persistedWatermark", jobEntry.persistedWatermark.ToString()),
			zap.Error(err),
		)
		return
	}
	jobEntry.persistedWatermark = jobEntry.watermark
	return
}

func (jobEntry *JobEntry) StringLocked() string {
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
		"Index[%s]%s,%v[%v]",
		jobEntry.jobName,
		jobEntry.watermark.ToString(),
		jobEntry.jobSpec,
		stateStr,
	)
}
