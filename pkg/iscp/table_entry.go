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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
		mu:        sync.RWMutex{},
	}
}
func (t *TableEntry) AddSinker(
	sinkConfig *ConsumerInfo,
	jobConfig JobConfig,
	watermark types.TS,
	iterationErr error,
) (ok bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, sinker := range t.sinkers {
		if sinker.jobName == sinkConfig.JobName {
			return false, nil
		}
	}
	sinkerEntry, err := NewJobEntry(t.exec.cnUUID, t.tableDef, t, sinkConfig, jobConfig, watermark, iterationErr)
	if err != nil {
		return false, err
	}
	t.sinkers = append(t.sinkers, sinkerEntry)
	return true, nil
}

// for UT
func (t *TableEntry) GetWatermark(jobName string) (watermark types.TS, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, sinker := range t.sinkers {
		if sinker.jobName == jobName {
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
	jobName string,
) (isEmpty bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, sinker := range t.sinkers {
		if sinker.jobName == jobName {
			t.sinkers = append(t.sinkers[:i], t.sinkers[i+1:]...)
			return len(t.sinkers) == 0, nil
		}
	}
	return false, moerr.NewInternalErrorNoCtx("sinker not found")
}

func (t *TableEntry) getCandidate() (iter []*Iteration, minFromTS types.TS) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	candidates := make([]*JobEntry, 0, len(t.sinkers))
	for _, sinker := range t.sinkers {
		if !sinker.IsInitedAndFinished() {
			continue
		}
		candidates = append(candidates, sinker)
	}
	iterations := make([]*Iteration, 0, len(candidates))
	minFromTS = types.MaxTs()
	for _, sinker := range candidates {
		ok, from, to, share := sinker.jobConfig.Check(candidates, sinker, types.MaxTs())
		if !ok {
			continue
		}
		foundIteration := false
		if share {
			for _, iter := range iterations {
				if iter.from.EQ(&from) && iter.to.EQ(&to) {
					iter.sinkers = append(iter.sinkers, sinker)
					foundIteration = true
					break
				}
			}
		}
		if !foundIteration {
			iterations = append(iterations, &Iteration{
				table:   t,
				sinkers: []*JobEntry{sinker},
				from:    from,
				to:      to,
			})
			if from.LT(&minFromTS) {
				minFromTS = from
			}
		}
	}
	return iterations, minFromTS
}

// TODO
func toErrorCode(err error) int {
	if err != nil {
		return 1
	}
	return 0
}

func (t *TableEntry) UpdateWatermark(iter *Iteration) {
	if iter.from.GE(&iter.to) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, sinker := range iter.sinkers {
		sinker.UpdateWatermark(iter.from, iter.to)
	}
}

func (t *TableEntry) fillInISCPLogUpdateSQL(firstTable bool, insertWriter, deleteWriter *bytes.Buffer) (err error) {
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

func (t *TableEntry) String() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	tableStr := fmt.Sprintf("\tTable[%d,%s-%d,%s-%d]", t.accountID, t.dbName, t.dbID, t.tableName, t.tableID)
	tableStr += "\n"
	for _, sinker := range t.sinkers {
		tableStr += fmt.Sprintf("\t\t%s\n", sinker.StringLocked())
	}
	return tableStr
}
