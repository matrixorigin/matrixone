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
	stage := int8(JobStage_Running)
	if jobStatus != nil {
		currentLSN = jobStatus.LSN
		stage = jobStatus.Stage
	}
	jobEntry := &JobEntry{
		tableInfo:          tableInfo,
		jobName:            jobName,
		jobID:              jobID,
		jobSpec:            &jobSpec.TriggerSpec,
		watermark:          watermark,
		persistedWatermark: watermark,
		state:              state,
		stage:              stage,
		dropAt:             dropAt,
		currentLSN:         currentLSN,
		// Only the trigger spec is retained, so the consumer class is recorded
		// here: it selects the watermark flush threshold below, and it is the
		// one thing about the consumer this entry still needs to know.
		isIndexJob: jobSpec.ConsumerInfo.ConsumerType == int8(ConsumerType_IndexSync),
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
) error {
	if jobEntry.state == ISCPJobState_Error {
		// Lifecycle progress is terminal, but drop/recreate log records still need
		// to update the metadata used by GC and generation management.
		jobEntry.jobSpec = &jobSpec.TriggerSpec
		jobEntry.dropAt = dropAt
		return nil
	}
	nextStage := max(jobEntry.stage, jobStatus.Stage)
	needApply := false
	if jobEntry.currentLSN < jobStatus.LSN {
		needApply = true
	}
	if jobEntry.currentLSN == jobStatus.LSN && jobEntry.state < state {
		needApply = true
	}
	if needApply {
		if jobEntry.watermark.GT(&watermark) {
			// The durable row is already terminal, so no second conditional write is
			// needed (and job_state != Error would reject it). Accept the terminal
			// version without moving the last known-good watermark backwards.
			if state == ISCPJobState_Error {
				jobEntry.jobSpec = &jobSpec.TriggerSpec
				jobEntry.dropAt = dropAt
				jobEntry.stage = nextStage
				jobEntry.currentLSN = jobStatus.LSN
				jobEntry.state = ISCPJobState_Error
				return nil
			}
			errMsg := fmt.Sprintf("watermark %v > %v, current state %d, incoming state %d, job %d-%v-%d",
				jobEntry.watermark.ToString(), watermark.ToString(), jobEntry.state, state, jobEntry.tableInfo.tableID, jobEntry.jobName, jobEntry.jobID)
			err := FlushPermanentErrorMessage(
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
				types.MaxTs(),
				errMsg,
				// The regressing row is already durable. Fence that exact version;
				// using the older in-memory LSN can never match it.
				[]uint64{jobStatus.LSN},
			)
			if err != nil {
				return err
			}
			// Preserve the last known-good watermark. Only the LSN and terminal
			// state advance to reflect the durable fence.
			jobEntry.jobSpec = &jobSpec.TriggerSpec
			jobEntry.dropAt = dropAt
			jobEntry.stage = nextStage
			jobEntry.currentLSN = jobStatus.LSN
			jobEntry.state = ISCPJobState_Error
			return nil
		}
		jobEntry.currentLSN = jobStatus.LSN
		jobEntry.persistedWatermark = watermark
		jobEntry.watermark = watermark
		jobEntry.state = state
	}
	// Job metadata and Stage can change without a progress/state transition.
	jobEntry.jobSpec = &jobSpec.TriggerSpec
	jobEntry.dropAt = dropAt
	jobEntry.stage = nextStage
	return nil
}

func (jobEntry *JobEntry) IsInitedAndFinished() bool {
	return jobEntry.state == ISCPJobState_Completed
}

func (jobEntry *JobEntry) UpdateWatermark(
	from, to types.TS,
	watermarkFlushThreshold time.Duration,
) error {
	if from.GE(&to) {
		return nil
	}
	expectedFrom := jobEntry.watermark.Next()
	if !expectedFrom.EQ(&from) {
		err := FlushPermanentErrorMessage(
			jobEntry.tableInfo.exec.ctx,
			jobEntry.tableInfo.exec.cnUUID,
			jobEntry.tableInfo.exec.txnEngine,
			jobEntry.tableInfo.exec.cnTxnClient,
			jobEntry.tableInfo.accountID,
			jobEntry.tableInfo.tableID,
			[]string{jobEntry.jobName},
			[]uint64{jobEntry.jobID},
			[]uint64{jobEntry.currentLSN},
			[]*JobStatus{{Stage: jobEntry.stage}},
			types.MaxTs(),
			fmt.Sprintf("update watermark failed, from %v, current %v", from.ToString(), expectedFrom.ToString()),
			[]uint64{jobEntry.currentLSN},
		)
		if err != nil {
			return err
		}
		// The catalog is now durably terminal. Reflect that state locally and do
		// not move progress across the discontinuity that caused the fence.
		jobEntry.state = ISCPJobState_Error
		return nil
	}
	jobEntry.watermark = to
	return nil
}

// flushThreshold is how far the in-memory watermark must run ahead of the
// persisted one before it is worth a catalog write. Index jobs use their own,
// much shorter threshold: their watermark is READ by the optimizer to decide
// whether the index may back a mandatory filter, so a stale persisted value
// costs query plans, not just restart work.
func (jobEntry *JobEntry) flushThreshold(general time.Duration) time.Duration {
	if !jobEntry.isIndexJob || jobEntry.tableInfo == nil ||
		jobEntry.tableInfo.exec == nil || jobEntry.tableInfo.exec.option == nil {
		return general
	}
	if idx := jobEntry.tableInfo.exec.option.IndexFlushWatermarkInterval; idx > 0 {
		return idx
	}
	return general
}

func (jobEntry *JobEntry) tryFlushWatermark(
	ctx context.Context,
	txn client.TxnOperator,
	threshold time.Duration,
) (needFlush bool, err error) {
	threshold = jobEntry.flushThreshold(threshold)
	if jobEntry.state != ISCPJobState_Completed ||
		jobEntry.watermark.Physical()-jobEntry.persistedWatermark.Physical() < threshold.Nanoseconds() {
		return
	}
	needFlush = true
	// Advancing a watermark is a progress update, not a lifecycle transition.
	// Update LSN in place so Stage and any future status fields survive.
	sql := cdc.CDCSQLBuilder.ISCPLogAdvanceWatermarkSQL(
		jobEntry.tableInfo.accountID,
		jobEntry.tableInfo.tableID,
		jobEntry.jobName,
		jobEntry.jobID,
		jobEntry.watermark,
		jobEntry.currentLSN+1,
		jobEntry.stage,
		ISCPJobState_Completed,
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
	defer result.Close()
	if result.AffectedRows != 1 {
		err = newISCPStatusCASLostError(
			"iscp flush watermark", jobEntry.jobName, jobEntry.jobID, result.AffectedRows)
		return
	}
	jobEntry.state = ISCPJobState_Pending
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
