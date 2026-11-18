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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	return NewJobEntryWithStatus(
		tableInfo, jobName, jobSpec, nil, jobID, watermark, state, dropAt,
	)
}

// NewJobEntryWithStatus restores an entry from both catalog state and its
// durable progress. NewJobEntry remains as the source-compatible constructor
// for callers that do not have persisted status.
func NewJobEntryWithStatus(
	tableInfo *TableEntry,
	jobName string,
	jobSpec *JobSpec,
	jobStatus *JobStatus,
	jobID uint64,
	watermark types.TS,
	state int8,
	dropAt types.Timestamp,
) *JobEntry {
	var currentLSN uint64
	if jobStatus != nil {
		currentLSN = jobStatus.LSN
	}
	jobEntry := &JobEntry{
		tableInfo:          tableInfo,
		jobName:            jobName,
		jobID:              jobID,
		jobSpec:            &jobSpec.TriggerSpec,
		watermark:          watermark,
		persistedWatermark: watermark,
		state:              state,
		dropAt:             dropAt,
		currentLSN:         currentLSN,
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
		jobEntry.currentLSN = jobStatus.LSN
		jobEntry.persistedWatermark = watermark
		jobEntry.watermark = watermark
		jobEntry.state = state
	}
}

func (jobEntry *JobEntry) IsInitedAndFinished() bool {
	return jobEntry.state == ISCPJobState_Completed
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
	sql := cdc.CDCSQLBuilder.ISCPLogAdvanceWatermarkSQL(
		jobEntry.tableInfo.accountID,
		jobEntry.tableInfo.tableID,
		jobEntry.jobName,
		jobEntry.jobID,
		jobEntry.watermark,
		jobEntry.currentLSN,
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
	if result.AffectedRows != 1 {
		result.Close()
		return true, moerr.NewInternalErrorNoCtxf("iscp flush watermark: update affected %d rows for job %s (id=%d), expected 1", result.AffectedRows, jobEntry.jobName, jobEntry.jobID)
	}
	result.Close()
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
