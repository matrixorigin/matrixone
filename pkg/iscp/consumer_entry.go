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
	"encoding/json"
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
	watermark types.TS,
	iterationErr error,
) (*JobEntry, error) {
	consumer, err := NewConsumer(cnUUID, tableDef, sinkerConfig)
	if err != nil {
		return nil, err
	}
	sinkerEntry := &JobEntry{
		tableInfo:    tableInfo,
		indexName:    sinkerConfig.IndexName,
		consumer:     consumer,
		consumerType: sinkerConfig.ConsumerType,
		watermark:    watermark,
		err:          iterationErr,
		consumerInfo: sinkerConfig,
	}
	sinkerEntry.init()
	return sinkerEntry, nil
}

func (sinkerEntry *JobEntry) init() {
	if sinkerEntry.watermark.IsEmpty() {
		// in the 1st iteration, toTS is determined by txn.SnapshotTS()
		iter := &Iteration{
			table:   sinkerEntry.tableInfo,
			sinkers: []*JobEntry{sinkerEntry},
			to:      types.TS{},
			from:    types.TS{},
		}
		sinkerEntry.tableInfo.exec.worker.Submit(iter)
	} else {
		sinkerEntry.inited.Store(true)
	}
}

func (sinkerEntry *JobEntry) getConsumerInfoStr() string {
	consumerInfoStr, err := json.Marshal(sinkerEntry.consumerInfo)
	if err != nil {
		panic(err)
	}
	return string(consumerInfoStr)
}

// TODO
func (sinkerEntry *JobEntry) PermanentError() bool {
	return toErrorCode(sinkerEntry.err) > PermanentErrorThreshold
}

func (sinkerEntry *JobEntry) StringLocked() string {
	initStr := ""
	if !sinkerEntry.inited.Load() {
		initStr = "-N"
	}
	return fmt.Sprintf("Index[%s%s]%d,%s,%v", sinkerEntry.indexName, initStr, sinkerEntry.consumerType, sinkerEntry.watermark.ToString(), sinkerEntry.err)
}

func (sinkerEntry *JobEntry) fillInAsyncIndexLogInsertSQL(firstSinker bool, w *bytes.Buffer) error {
	if !firstSinker {
		w.WriteString(",")
	}
	_, err := w.WriteString(fmt.Sprintf(" (%d, %d, '%s', '%s', 0, '', '', '%s', NULL)",
		sinkerEntry.tableInfo.accountID,
		sinkerEntry.tableInfo.tableID,
		sinkerEntry.indexName,
		sinkerEntry.watermark.ToString(),
		sinkerEntry.getConsumerInfoStr(),
	))
	return err
}

func (sinkerEntry *JobEntry) fillInAsyncIndexLogDeleteSQL(firstSinker bool, w *bytes.Buffer) error {
	if !firstSinker {
		w.WriteString(" OR")
	}
	_, err := w.WriteString(
		fmt.Sprintf(" (account_id = %d AND table_id = %d AND index_name = '%s' and drop_at is null)",
			sinkerEntry.tableInfo.accountID,
			sinkerEntry.tableInfo.tableID,
			sinkerEntry.indexName,
		))
	return err
}
