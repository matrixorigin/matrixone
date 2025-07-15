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

package idxcdc

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type TableState int8

const (
	TableState_Invalid TableState = iota
	TableState_Init
	TableState_Running
	TableState_Finished
)

type TableInfo_2 struct {
	exec      *CDCTaskExecutor
	tableDef  *plan.TableDef
	accountID uint32
	dbID      uint64
	tableID   uint64
	tableName string
	dbName    string
	state     TableState
	sinkers   []*SinkerEntry
	mu        sync.RWMutex
}

func tableInfoLess(a, b *TableInfo_2) bool {
	return a.tableID < b.tableID
}

func NewTableInfo_2(
	exec *CDCTaskExecutor,
	accountID uint32,
	dbID, tableID uint64,
	dbName, tableName string,
	tableDef *plan.TableDef,
) *TableInfo_2 {
	return &TableInfo_2{
		exec:      exec,
		accountID: accountID,
		tableDef:  tableDef,
		sinkers:   make([]*SinkerEntry, 0),
		dbID:      dbID,
		tableID:   tableID,
		dbName:    dbName,
		tableName: tableName,
		state:     TableState_Finished,
		mu:        sync.RWMutex{},
	}
}
func (t *TableInfo_2) AddSinker(
	sinkConfig *ConsumerInfo,
	watermark types.TS,
	iterationErr error,
) (existed bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, sinker := range t.sinkers {
		if sinker.indexName == sinkConfig.IndexName {
			return false, nil
		}
	}
	sinkerEntry, err := NewSinkerEntry(t.exec.cnUUID, t.tableDef, t, sinkConfig, watermark, iterationErr)
	if err != nil {
		return false, err
	}
	t.sinkers = append(t.sinkers, sinkerEntry)
	return true, nil
}

// for UT
func (t *TableInfo_2) GetWatermark(indexName string) (watermark types.TS, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, sinker := range t.sinkers {
		if sinker.indexName == indexName {
			return sinker.watermark, true
		}
	}
	return types.TS{}, false
}

func (t *TableInfo_2) IsEmpty() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.sinkers) == 0
}

func (t *TableInfo_2) DeleteSinker(
	ctx context.Context,
	indexName string,
) (isEmpty bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, sinker := range t.sinkers {
		if sinker.indexName == indexName {
			t.sinkers = append(t.sinkers[:i], t.sinkers[i+1:]...)
			return len(t.sinkers) == 0, nil
		}
	}
	return false, moerr.NewInternalError(ctx, "sinker not found")
}

func (t *TableInfo_2) IsInitedAndFinished() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	hasActiveSinker := false
	for _, sinker := range t.sinkers {
		if !sinker.PermanentError() && sinker.inited.Load() {
			hasActiveSinker = true
			break
		}
	}
	return hasActiveSinker && t.state == TableState_Finished
}

func (t *TableInfo_2) GetMinWaterMark() types.TS {
	t.mu.RLock()
	defer t.mu.RUnlock()
	minWatermark := types.MaxTs()
	for _, sinker := range t.sinkers {
		if !sinker.inited.Load() {
			continue
		}
		if sinker.PermanentError() {
			continue
		}
		if sinker.watermark.LT(&minWatermark) {
			minWatermark = sinker.watermark
		}
	}
	return minWatermark
}

func (t *TableInfo_2) GetMaxWaterMark() types.TS {
	t.mu.RLock()
	defer t.mu.RUnlock()
	maxWatermark := types.TS{}
	for _, sinker := range t.sinkers {
		if sinker.watermark.GT(&maxWatermark) {
			maxWatermark = sinker.watermark
		}
	}
	return maxWatermark
}

func (t *TableInfo_2) GetMaxWaterMarkLocked() types.TS {
	maxWatermark := types.TS{}
	for _, sinker := range t.sinkers {
		if sinker.watermark.GT(&maxWatermark) {
			maxWatermark = sinker.watermark
		}
	}
	return maxWatermark
}

func (t *TableInfo_2) GetSyncTask(ctx context.Context, toTS types.TS) *Iteration {
	t.mu.Lock()
	defer t.mu.Unlock()
	dirtySinker := t.getNewSinkersLocked()
	maxTS := t.GetMaxWaterMarkLocked()
	if dirtySinker != nil {
		t.state = TableState_Running
		return &Iteration{
			table:   t,
			sinkers: []*SinkerEntry{dirtySinker},
			to:      maxTS,
			from:    dirtySinker.watermark,
		}
	}
	t.state = TableState_Running
	return &Iteration{
		table:   t,
		sinkers: t.sinkers,
		to:      toTS,
		from:    maxTS,
	}
}

// TODO
func toErrorCode(err error) int {
	return 0
}

func (t *TableInfo_2) UpdateWatermark(from, to types.TS) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, sinker := range t.sinkers {
		if sinker.watermark.GE(&to) {
			panic("logic error")
		}
		if sinker.watermark.GE(&from) {
			sinker.watermark = to
		}
	}
}

func (t *TableInfo_2) fillInAsyncIndexLogInsertSQL(firstTable bool, w *bytes.Buffer) (err error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for i, sinker := range t.sinkers {
		err = sinker.fillInAsyncIndexLogInsertSQL(i == 0 && firstTable, w)
		if err != nil {
			return err
		}
	}
	return
}

func (t *TableInfo_2) fillInAsyncIndexLogDeleteSQL(firstTable bool, w *bytes.Buffer) (err error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for i, sinker := range t.sinkers {
		err = sinker.fillInAsyncIndexLogDeleteSQL(i == 0 && firstTable, w)
		if err != nil {
			return err
		}
	}
	return
}

func (t *TableInfo_2) OnIterationFinished(iter *Iteration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// init sinker
	if len(iter.sinkers) == 1 && !iter.sinkers[0].inited.Load() {
		sinker := iter.sinkers[0]
		if iter.err[0] != nil {
			sinker.err = iter.err[0]
			t.exec.worker.Submit(
				&Iteration{
					table:   t,
					sinkers: []*SinkerEntry{sinker},
					to:      types.TS{},
					from:    types.TS{},
			},
		)
		} else {
			iter.sinkers[0].inited.Store(true)
			sinker.watermark = iter.to
		}
		return
	}
	if t.state != TableState_Running {
		panic("logic error")
	}
	// dirty sinkers
	maxTS := t.GetMaxWaterMarkLocked()
	if maxTS.EQ(&iter.to) {
		if len(iter.sinkers) != 1 {
			panic("logic error")
		}
		sinker := iter.sinkers[0]
		if iter.err[0] != nil {
			sinker.err = iter.err[0]
		} else {
			sinker.watermark = iter.to
		}
		t.state = TableState_Finished
		return
	}
	// all sinkers
	if !maxTS.EQ(&iter.from) {
		panic("logic error")
	}
	t.state = TableState_Finished
	for i, sinker := range iter.sinkers {
		if iter.err[i] != nil {
			sinker.err = iter.err[i]
		} else {
			sinker.watermark = iter.to
		}
	}
}

func (t *TableInfo_2) getNewSinkersLocked() *SinkerEntry {
	maxTS := t.GetMaxWaterMarkLocked()
	for _, sinker := range t.sinkers {
		if !sinker.inited.Load() {
			continue
		}
		if sinker.watermark.LT(&maxTS) {
			return sinker
		}
	}
	return nil
}

func (t *TableInfo_2) String() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	tableStr := fmt.Sprintf("\tTable[%d,%s-%d,%s-%d]", t.accountID, t.dbName, t.dbID, t.tableName, t.tableID)
	stateStr := "I"
	if t.state == TableState_Running {
		stateStr = "R"
	}
	if t.state == TableState_Finished {
		stateStr = "F"
	}
	tableStr += stateStr
	tableStr += "\n"
	for _, sinker := range t.sinkers {
		tableStr += fmt.Sprintf("\t\t%s\n", sinker.StringLocked())
	}
	return tableStr
}
