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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	ctx context.Context,
	jobName string,
	jobSpec *JobSpec,
	jobStatus *JobStatus,
	jobID uint64,
	watermark types.TS,
	state int8,
	dropAt types.Timestamp,
) (newCreate bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := JobKey{
		JobName: jobName,
		JobID:   jobID,
	}
	jobEntry, ok := t.jobs[key]
	if !ok || jobEntry.jobID < jobID {
		newCreate = true
		jobEntry = NewJobEntryWithStatus(t, jobName, jobSpec, jobStatus, jobID, watermark, state, dropAt)
		t.jobs[key] = jobEntry
		return
	}
	if jobEntry.jobID > jobID {
		return
	}
	err = jobEntry.update(ctx, jobSpec, jobStatus, watermark, state, dropAt)
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

func (t *TableEntry) getJobState(jobName string) (lsn uint64, state int8, ok bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, job := range t.jobs {
		if job.jobName == jobName && job.dropAt == 0 {
			return job.currentLSN, job.state, true
		}
	}
	return 0, ISCPJobState_Invalid, false
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
		if t.exec != nil && t.exec.IsJobFenced(NewJobRuntimeKey(t.accountID, t.tableID, sinker.jobName, sinker.jobID)) {
			continue
		}
		candidates = append(candidates, sinker)
	}
	iterations := make([]*IterationContext, 0, len(candidates))
	shareableIterations := make([]*IterationContext, 0, len(candidates))
	minFromTS = types.MaxTs()
	for _, sinker := range candidates {
		if sinker.watermark.IsEmpty() && sinker.state == ISCPJobState_Completed {
			iterations = append(iterations, &IterationContext{
				tableID:   t.tableID,
				accountID: t.accountID,
				jobNames:  []string{sinker.jobName},
				jobIDs:    []uint64{sinker.jobID},
				lsn:       []uint64{sinker.currentLSN + 1},
				stages:    []int8{sinker.stage},
				fromTS:    types.TS{},
				toTS:      types.TS{},
			})
			continue
		}
		ok, from, to, share := sinker.jobSpec.Check(candidates, sinker, types.MaxTs())
		if !ok {
			continue
		}
		// InitSQL is executed for exactly one job at a time. Keep its iteration
		// out of the shared pool until the durable stage advances to Running.
		if sinker.stage == JobStage_Init {
			share = false
		}
		foundIteration := false
		if share {
			for _, iter := range shareableIterations {
				if iter.fromTS.EQ(&from) && iter.toTS.EQ(&to) {
					iter.jobNames = append(iter.jobNames, sinker.jobName)
					iter.jobIDs = append(iter.jobIDs, sinker.jobID)
					iter.lsn = append(iter.lsn, sinker.currentLSN+1)
					iter.stages = append(iter.stages, sinker.stage)
					foundIteration = true
					break
				}
			}
		}
		if !foundIteration {
			iter := &IterationContext{
				tableID:   t.tableID,
				accountID: t.accountID,
				jobNames:  []string{sinker.jobName},
				jobIDs:    []uint64{sinker.jobID},
				lsn:       []uint64{sinker.currentLSN + 1},
				stages:    []int8{sinker.stage},
				fromTS:    from,
				toTS:      to,
			}
			iterations = append(iterations, iter)
			if sinker.stage != JobStage_Init {
				shareableIterations = append(shareableIterations, iter)
			}
			if from.LT(&minFromTS) {
				minFromTS = from
			}
		}
	}
	return iterations, minFromTS
}

// markIterationPending records ownership only after a worker accepts the
// iteration. Validate the complete shared iteration before changing any job so
// the in-memory transition is all-or-nothing.
func (t *TableEntry) markIterationPending(iter *IterationContext) error {
	if iter == nil ||
		len(iter.jobNames) != len(iter.jobIDs) ||
		len(iter.jobNames) != len(iter.lsn) ||
		len(iter.jobNames) != len(iter.stages) {
		return moerr.NewInternalErrorNoCtx("invalid ISCP iteration")
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	jobs := make([]*JobEntry, len(iter.jobNames))
	for i, jobName := range iter.jobNames {
		job := t.jobs[JobKey{JobName: jobName, JobID: iter.jobIDs[i]}]
		if job == nil {
			return moerr.NewInternalErrorNoCtxf("ISCP job %s/%d no longer exists", jobName, iter.jobIDs[i])
		}
		if job.state != ISCPJobState_Completed || job.currentLSN+1 != iter.lsn[i] {
			return moerr.NewInternalErrorNoCtxf(
				"ISCP job %s/%d changed before admission: state %d, lsn %d, iteration lsn %d",
				jobName,
				iter.jobIDs[i],
				job.state,
				job.currentLSN,
				iter.lsn[i],
			)
		}
		jobs[i] = job
	}
	for i, job := range jobs {
		job.currentLSN = iter.lsn[i]
		job.state = ISCPJobState_Pending
	}
	return nil
}

func (t *TableEntry) UpdateWatermark(iter *IterationContext) error {
	if iter.fromTS.GE(&iter.toTS) {
		return nil
	}
	if len(iter.jobNames) != len(iter.jobIDs) {
		return moerr.NewInternalErrorNoCtx("invalid ISCP watermark iteration")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// Validate the complete shared iteration before advancing any member. Init
	// owns a lifecycle transition and may only be completed by the worker after
	// InitSQL succeeds; a clean-table maintenance pass cannot prove that.
	for i, jobName := range iter.jobNames {
		jobEntry := t.jobs[JobKey{
			JobName: jobName,
			JobID:   iter.jobIDs[i],
		}]
		if jobEntry == nil {
			return moerr.NewInternalErrorNoCtxf(
				"ISCP job %s/%d no longer exists", jobName, iter.jobIDs[i])
		}
		if jobEntry.stage == JobStage_Init {
			return moerr.NewInternalErrorNoCtxf(
				"ISCP job %s/%d cannot advance watermark before initialization",
				jobName, iter.jobIDs[i])
		}
		expectedFrom := jobEntry.watermark.Next()
		if !expectedFrom.EQ(&iter.fromTS) {
			// No member has been advanced yet, so fencing a discontinuity cannot
			// leave a shared iteration partially applied in memory.
			return jobEntry.UpdateWatermark(
				iter.fromTS, iter.toTS, t.exec.option.FlushWatermarkInterval)
		}
	}
	for i, jobName := range iter.jobNames {
		t.jobs[JobKey{JobName: jobName, JobID: iter.jobIDs[i]}].watermark = iter.toTS
	}
	return nil
}

type watermarkFlushReservation struct {
	jobKey            JobKey
	currentLSN        uint64
	previousPersisted types.TS
	flushedWatermark  types.TS
}

func (t *TableEntry) tryFlushWatermark(
	ctx context.Context,
	txn client.TxnOperator,
	threshold time.Duration,
) (reservations []watermarkFlushReservation, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for jobKey, jobEntry := range t.jobs {
		previousPersisted := jobEntry.persistedWatermark
		needFlush, flushErr := jobEntry.tryFlushWatermark(ctx, txn, threshold)
		if flushErr != nil {
			return reservations, flushErr
		}
		if needFlush {
			reservations = append(reservations, watermarkFlushReservation{
				jobKey:            jobKey,
				currentLSN:        jobEntry.currentLSN,
				previousPersisted: previousPersisted,
				flushedWatermark:  jobEntry.watermark,
			})
		}
	}
	return reservations, nil
}

func (t *TableEntry) rollbackWatermarkFlushes(reservations []watermarkFlushReservation) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, reservation := range reservations {
		jobEntry := t.jobs[reservation.jobKey]
		if jobEntry == nil ||
			jobEntry.currentLSN != reservation.currentLSN ||
			jobEntry.state != ISCPJobState_Pending ||
			!jobEntry.persistedWatermark.EQ(&reservation.flushedWatermark) {
			continue
		}
		jobEntry.state = ISCPJobState_Completed
		jobEntry.persistedWatermark = reservation.previousPersisted
	}
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
