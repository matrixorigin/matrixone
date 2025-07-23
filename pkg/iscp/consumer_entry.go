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

type SinkerState int8

const (
	SinkerState_Invalid SinkerState = iota
	SinkerState_Running
	SinkerState_Finished
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
		indexName:    sinkerConfig.IndexName,
		consumer:     consumer,
		consumerType: sinkerConfig.ConsumerType,
		jobConfig:    jobConfig,
		watermark:    watermark,
		err:          iterationErr,
		consumerInfo: sinkerConfig,
	}
	jobEntry.init()
	return jobEntry, nil
}

func (jobEntry *JobEntry) init() {
	if jobEntry.watermark.IsEmpty() {
		// in the 1st iteration, toTS is determined by txn.SnapshotTS()
		iter := &Iteration{
			table:   jobEntry.tableInfo,
			sinkers: []*JobEntry{jobEntry},
			to:      types.TS{},
			from:    types.TS{},
		}
		jobEntry.tableInfo.exec.worker.Submit(iter)
	} else {
		jobEntry.inited.Store(true)
	}
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
	return fmt.Sprintf("Index[%s%s]%d,%s,%v", jobEntry.indexName, initStr, jobEntry.consumerType, jobEntry.watermark.ToString(), jobEntry.err)
}

func (jobEntry *JobEntry) fillInAsyncIndexLogInsertSQL(firstSinker bool, w *bytes.Buffer) error {
	if !firstSinker {
		w.WriteString(",")
	}
	_, err := w.WriteString(fmt.Sprintf(" (%d, %d,'', '%s',%d, '%s',  0, '', '', '%s', NULL)",
		jobEntry.tableInfo.accountID,
		jobEntry.tableInfo.tableID,
		jobEntry.indexName,
		jobEntry.consumerType,
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
			jobEntry.indexName,
		))
	return err
}
