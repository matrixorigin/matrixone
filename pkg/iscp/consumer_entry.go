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
	dropAt types.Timestamp,
) *JobEntry {
	jobEntry := &JobEntry{
		tableInfo:          tableInfo,
		jobName:            jobName,
		jobID:              jobID,
		jobSpec:            &jobSpec.TriggerSpec,
		watermark:          watermark,
		persistedWatermark: watermark,
		state:              state,
		dropAt:             dropAt,
	}
	return jobEntry
}

func (jobEntry *JobEntry) update(
	ctx context.Context,
	jobSpec *JobSpec,
	jobStatus *JobStatus,
	watermark types.TS,
	state int8,
	dropAt types.Timestamp,
) {
	jobEntry.jobSpec = &jobSpec.TriggerSpec
	jobEntry.dropAt = dropAt
	if jobEntry.state == ISCPJobState_Error {
		return
	}
	needApply := false
	if jobEntry.currentLSN < jobStatus.LSN {
		needApply = true
	}
	if jobEntry.currentLSN == jobStatus.LSN && jobEntry.state < state {
		needApply = true
	}
	if needApply {
		if jobEntry.watermark.GT(&watermark) {
			errMsg := fmt.Sprintf("watermark %v > %v, current state %d, incoming state %d, job %d-%v-%d",
				watermark.ToString(), jobEntry.watermark.ToString(), jobEntry.state, state, jobEntry.tableInfo.tableID, jobEntry.jobName, jobEntry.jobID)
			FlushPermanentErrorMessage(
				ctx,
				jobEntry.tableInfo.exec.cnUUID,
				jobEntry.tableInfo.exec.txnEngine,
				jobEntry.tableInfo.exec.cnTxnClient,
				jobEntry.tableInfo.accountID,
				jobEntry.tableInfo.tableID,
				[]string{jobEntry.jobName},
				[]uint64{jobEntry.jobID},
				[]uint64{jobStatus.LSN},
				[]*JobStatus{jobStatus},
				watermark,
				errMsg,
			)
		}
		jobEntry.currentLSN = jobStatus.LSN
		jobEntry.persistedWatermark = watermark
		jobEntry.watermark = watermark
		jobEntry.state = state
	}
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
		FlushPermanentErrorMessage(
			jobEntry.tableInfo.exec.ctx,
			jobEntry.tableInfo.exec.cnUUID,
			jobEntry.tableInfo.exec.txnEngine,
			jobEntry.tableInfo.exec.cnTxnClient,
			jobEntry.tableInfo.accountID,
			jobEntry.tableInfo.tableID,
			[]string{jobEntry.jobName},
			[]uint64{jobEntry.jobID},
			[]uint64{jobEntry.currentLSN},
			[]*JobStatus{{}},
			jobEntry.watermark,
			fmt.Sprintf("update watermark failed, from %v, current %v", from.ToString(), jobEntry.watermark.ToString()),
		)
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
	emptyStatus := &JobStatus{LSN: jobEntry.currentLSN + 1}
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
	result, err := ExecWithResult(
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
			zap.String("database", jobEntry.tableInfo.dbName),
			zap.Uint64("tableID", jobEntry.tableInfo.tableID),
			zap.Uint64("jobID", jobEntry.jobID),
			zap.String("watermark", jobEntry.watermark.ToString()),
			zap.String("persistedWatermark", jobEntry.persistedWatermark.ToString()),
			zap.Error(err),
		)
		return
	}
	jobEntry.state = ISCPJobState_Pending
	result.Close()
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
		"Index[%s-%d]%s,%v[%v]%v",
		jobEntry.jobName,
		jobEntry.jobID,
		jobEntry.watermark.ToString(),
		jobEntry.jobSpec,
		stateStr,
		jobEntry.dropAt,
	)
}
