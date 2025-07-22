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

func tableInfoLess(a, b *TableEntry) bool {
	if a.accountID != b.accountID {
		return a.accountID < b.accountID
	}
	return a.tableID < b.tableID
}

func NewTableEntry(
	exec *ISCPTaskExecutor,
	accountID uint32,
	dbID, tableID uint64,
	dbName, tableName string,
	tableDef *plan.TableDef,
) *TableEntry {
	return &TableEntry{
		exec:      exec,
		accountID: accountID,
		tableDef:  tableDef,
		sinkers:   make([]*JobEntry, 0),
		dbID:      dbID,
		tableID:   tableID,
		dbName:    dbName,
		tableName: tableName,
		state:     TableState_Finished,
		mu:        sync.RWMutex{},
	}
}
func (t *TableEntry) AddSinker(
	sinkConfig *ConsumerInfo,
	watermark types.TS,
	iterationErr error,
) (ok bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, sinker := range t.sinkers {
		if sinker.indexName == sinkConfig.IndexName {
			return false, nil
		}
	}
	sinkerEntry, err := NewJobEntry(t.exec.cnUUID, t.tableDef, t, sinkConfig, watermark, iterationErr)
	if err != nil {
		return false, err
	}
	t.sinkers = append(t.sinkers, sinkerEntry)
	return true, nil
}

// for UT
func (t *TableEntry) GetWatermark(indexName string) (watermark types.TS, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, sinker := range t.sinkers {
		if sinker.indexName == indexName {
			return sinker.watermark, true
		}
	}
	return types.TS{}, false
}

func (t *TableEntry) IsEmpty() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.sinkers) == 0
}

func (t *TableEntry) DeleteSinker(
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
	return false, moerr.NewInternalErrorNoCtx("sinker not found")
}

func (t *TableEntry) IsInitedAndFinished() bool {
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

func (t *TableEntry) GetMinWaterMark() types.TS {
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

func (t *TableEntry) GetMaxWaterMarkLocked() types.TS {
	maxWatermark := types.TS{}
	for _, sinker := range t.sinkers {
		if sinker.watermark.GT(&maxWatermark) {
			maxWatermark = sinker.watermark
		}
	}
	return maxWatermark
}

func (t *TableEntry) getCandidateLocked() []*JobEntry {
	candidates := make([]*JobEntry, 0, len(t.sinkers))
	for _, sinker := range t.sinkers {
		if !sinker.inited.Load() {
			continue
		}
		if sinker.PermanentError() {
			continue
		}
		candidates = append(candidates, sinker)
	}
	return candidates
}

func (t *TableEntry) GetSyncTask(ctx context.Context, toTS types.TS) *Iteration {
	t.mu.Lock()
	defer t.mu.Unlock()
	candidates := t.getCandidateLocked()
	if len(candidates) == 0 {
		panic("logic error")
	}
	dirtySinker := t.getNewSinkersLocked(candidates)
	maxTS := t.GetMaxWaterMarkLocked()
	if dirtySinker != nil {
		t.state = TableState_Running
		return &Iteration{
			table:   t,
			sinkers: []*JobEntry{dirtySinker},
			to:      maxTS,
			from:    dirtySinker.watermark,
		}
	}
	t.state = TableState_Running
	for _, sinker := range candidates {
		if sinker.watermark.GT(&toTS) {
			toTS = sinker.watermark
		}
	}
	return &Iteration{
		table:   t,
		sinkers: candidates,
		to:      toTS,
		from:    maxTS,
	}
}

// TODO
func toErrorCode(err error) int {
	if err != nil {
		return 1
	}
	return 0
}

func (t *TableEntry) UpdateWatermark(from, to types.TS) {
	if from.GE(&to) {
		return
	}
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

func (t *TableEntry) fillInAsyncIndexLogUpdateSQL(firstTable bool, insertWriter, deleteWriter *bytes.Buffer) (err error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for i, sinker := range t.sinkers {
		if sinker.watermark.IsEmpty() {
			continue
		}
		err = sinker.fillInAsyncIndexLogInsertSQL(i == 0 && firstTable, insertWriter)
		if err != nil {
			return err
		}
		err = sinker.fillInAsyncIndexLogDeleteSQL(i == 0 && firstTable, deleteWriter)
		if err != nil {
			return err
		}
	}
	return
}

func (t *TableEntry) OnIterationFinished(iter *Iteration) {
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
					sinkers: []*JobEntry{sinker},
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
	if maxTS.LT(&iter.from) {
		// if there're new sinkers, maxTS may be greater
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

func (t *TableEntry) getNewSinkersLocked(candidates []*JobEntry) *JobEntry {
	maxTS := t.GetMaxWaterMarkLocked()
	for _, sinker := range candidates {
		if !sinker.inited.Load() {
			continue
		}
		if sinker.watermark.LT(&maxTS) {
			return sinker
		}
	}
	return nil
}

func (t *TableEntry) String() string {
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
