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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/zap"
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
) *TableEntry {
	return &TableEntry{
		exec:      exec,
		accountID: accountID,
		jobs:      make(map[JobKey]*JobEntry),
		dbID:      dbID,
		tableID:   tableID,
		dbName:    dbName,
		tableName: tableName,
		mu:        sync.RWMutex{},
	}
}
func (t *TableEntry) AddOrUpdateSinker(
	jobName string,
	jobSpec *JobSpec,
	jobID uint64,
	watermark types.TS,
	state int8,
	dropAt types.Timestamp,
) (newCreate bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := JobKey{
		JobName: jobName,
		JobID:   jobID,
	}
	jobEntry, ok := t.jobs[key]
	if !ok || jobEntry.jobID < jobID {
		newCreate = true
		jobEntry = NewJobEntry(t, jobName, jobSpec, jobID, watermark, state, dropAt)
		t.jobs[key] = jobEntry
		return
	}
	if jobEntry.jobID > jobID {
		return
	}
	jobEntry.update(jobSpec, watermark, state, dropAt)
	return
}

// for UT
func (t *TableEntry) GetWatermark(jobName string) (watermark types.TS, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, sinker := range t.jobs {
		if sinker.jobName == jobName && sinker.dropAt == 0 {
			return sinker.watermark, true
		}
	}
	return types.TS{}, false
}

func (t *TableEntry) IsEmpty() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.jobs) == 0
}

func (t *TableEntry) gcInMemoryJob(threshold time.Duration) (isEmpty bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	jobsToDelete := make([]JobKey, 0)
	now := time.Now()
	for _, jobEntry := range t.jobs {
		loc := now.Location()
		if jobEntry.dropAt != 0 && uint64(now.Unix())-uint64(threshold) >= uint64(jobEntry.dropAt.ToDatetime(loc).UnixTimestamp(loc)) {
			jobsToDelete = append(
				jobsToDelete,
				JobKey{
					JobName: jobEntry.jobName,
					JobID:   jobEntry.jobID,
				},
			)
		}
	}
	for _, jobName := range jobsToDelete {
		delete(t.jobs, jobName)
	}
	if len(jobsToDelete) != 0 {
		logutil.Info(
			"ISCP-Task gc in memory job",
			zap.Uint64("table", t.tableID),
			zap.Any("jobsToDelete", jobsToDelete),
		)
	}
	return len(t.jobs) == 0
}

func (t *TableEntry) getCandidate() (iter []*IterationContext, minFromTS types.TS) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	candidates := make([]*JobEntry, 0, len(t.jobs))
	for _, sinker := range t.jobs {
		if !sinker.IsInitedAndFinished() {
			continue
		}
		if sinker.dropAt != 0 {
			continue
		}
		candidates = append(candidates, sinker)
	}
	iterations := make([]*IterationContext, 0, len(candidates))
	minFromTS = types.MaxTs()
	for _, sinker := range candidates {
		if sinker.watermark.IsEmpty() && sinker.state == ISCPJobState_Completed {
			iterations = append(iterations, &IterationContext{
				tableID:   t.tableID,
				accountID: t.accountID,
				jobNames:  []string{sinker.jobName},
				jobIDs:    []uint64{sinker.jobID},
				fromTS:    types.TS{},
				toTS:      types.TS{},
			})
			continue
		}
		ok, from, to, share := sinker.jobSpec.Check(candidates, sinker, types.MaxTs())
		if !ok {
			continue
		}
		foundIteration := false
		if share {
			for _, iter := range iterations {
				if iter.fromTS.EQ(&from) && iter.toTS.EQ(&to) {
					iter.jobNames = append(iter.jobNames, sinker.jobName)
					iter.jobIDs = append(iter.jobIDs, sinker.jobID)
					foundIteration = true
					break
				}
			}
		}
		if !foundIteration {
			iterations = append(iterations, &IterationContext{
				tableID:   t.tableID,
				accountID: t.accountID,
				jobNames:  []string{sinker.jobName},
				jobIDs:    []uint64{sinker.jobID},
				fromTS:    from,
				toTS:      to,
			})
			if from.LT(&minFromTS) {
				minFromTS = from
			}
		}
	}
	return iterations, minFromTS
}

func (t *TableEntry) UpdateWatermark(iter *IterationContext) {
	if iter.fromTS.GE(&iter.toTS) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, jobName := range iter.jobNames {
		jobEntry := t.jobs[JobKey{
			JobName: jobName,
			JobID:   iter.jobIDs[i],
		}]
		jobEntry.UpdateWatermark(iter.fromTS, iter.toTS, t.exec.option.FlushWatermarkInterval)
	}
}

func (t *TableEntry) tryFlushWatermark(
	ctx context.Context,
	txn client.TxnOperator,
	threshold time.Duration,
) (flushCount int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, jobEntry := range t.jobs {
		needFlush, err := jobEntry.tryFlushWatermark(ctx, txn, threshold)
		if needFlush && err == nil {
			flushCount++
		}
	}
	return flushCount
}

func (t *TableEntry) String() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	tableStr := fmt.Sprintf("\tTable[%d,%s-%d,%s-%d]", t.accountID, t.dbName, t.dbID, t.tableName, t.tableID)
	tableStr += "\n"
	for _, sinker := range t.jobs {
		tableStr += fmt.Sprintf("\t\t%s\n", sinker.StringLocked())
	}
	return tableStr
}
