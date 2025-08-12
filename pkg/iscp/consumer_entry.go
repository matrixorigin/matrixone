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
	"errors"
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

func (jobEntry *JobEntry) update(jobSpec *JobSpec, watermark types.TS, state int8) {
	jobEntry.jobSpec = &jobSpec.TriggerSpec
	jobEntry.persistedWatermark = watermark
	jobEntry.watermark = watermark
	jobEntry.state = state
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

	if jobEntry.persistedWatermark.Physical()-jobEntry.watermark.Physical() > watermarkFlushThreshold.Nanoseconds() {
		ctx := context.Background()
		sql := cdc.CDCSQLBuilder.ISCPLogUpdateResultSQL(
			jobEntry.tableInfo.accountID,
			jobEntry.tableInfo.tableID,
			jobEntry.jobName,
			jobEntry.watermark,
			"",
			ISCPJobState_Completed,
		)
		nowTs := jobEntry.tableInfo.exec.txnEngine.LatestLogtailAppliedTime()
		createByOpt := client.WithTxnCreateBy(
			0,
			"",
			"iscp iteration",
			0)
		txnWriter, err := jobEntry.tableInfo.exec.cnTxnClient.New(
			ctx,
			nowTs,
			createByOpt,
		)
		if err != nil {
			return
		}
		defer func() {
			if err != nil {
				err = errors.Join(err, txnWriter.Rollback(ctx))
			} else {
				err = txnWriter.Commit(ctx)
			}
			if err != nil {
				logutil.Error(
					"ISCP-Task flush watermark failed",
					zap.Uint32("tenantID", jobEntry.tableInfo.accountID),
					zap.Uint64("tableID", jobEntry.tableInfo.tableID),
					zap.Strings("jobNames", []string{jobEntry.jobName}),
					zap.Error(err),
				)
			}
		}()
		_, err = ExecWithResult(
			ctx,
			sql,
			jobEntry.tableInfo.exec.cnUUID,
			txnWriter,
		)
		if err != nil {
			return
		}
	}
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
