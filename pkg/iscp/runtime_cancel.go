// Copyright 2024 Matrix Origin
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
	"sync"
)

var iscpExecutors sync.Map // map[cnUUID]*ISCPTaskExecutor

func RegisterExecutorRuntime(cnUUID string, exec *ISCPTaskExecutor) {
	if cnUUID == "" || exec == nil {
		return
	}
	iscpExecutors.Store(cnUUID, exec)
}

func UnregisterExecutorRuntime(cnUUID string, exec *ISCPTaskExecutor) {
	if cnUUID == "" || exec == nil {
		return
	}
	if current, ok := iscpExecutors.Load(cnUUID); ok && current == exec {
		iscpExecutors.Delete(cnUUID)
	}
}

func GetExecutorRuntime(cnUUID string) (*ISCPTaskExecutor, bool) {
	v, ok := iscpExecutors.Load(cnUUID)
	if !ok {
		var found *ISCPTaskExecutor
		count := 0
		iscpExecutors.Range(func(_, v any) bool {
			exec, ok := v.(*ISCPTaskExecutor)
			if !ok {
				return true
			}
			found = exec
			count++
			return count < 2
		})
		if count == 1 {
			return found, true
		}
		return nil, false
	}
	exec, ok := v.(*ISCPTaskExecutor)
	return exec, ok
}

func NewJobRuntimeKey(accountID uint32, tableID uint64, jobName string) JobRuntimeKey {
	return JobRuntimeKey{AccountID: accountID, TableID: tableID, JobName: jobName}
}

func (exec *ISCPTaskExecutor) ensureRuntimeMapsLocked() {
	if exec.fencedJobs == nil {
		exec.fencedJobs = make(map[JobRuntimeKey]struct{})
	}
	if exec.runningConsumers == nil {
		exec.runningConsumers = make(map[JobRuntimeKey]map[uint64]*RunningJobConsumer)
	}
}

func (exec *ISCPTaskExecutor) IsJobFenced(key JobRuntimeKey) bool {
	if exec == nil {
		return false
	}
	exec.runtimeMu.Lock()
	defer exec.runtimeMu.Unlock()
	_, ok := exec.fencedJobs[key]
	return ok
}

func (exec *ISCPTaskExecutor) CancelAndDrainJobConsumer(
	ctx context.Context,
	accountID uint32,
	tableID uint64,
	jobName string,
) error {
	if exec == nil {
		return nil
	}
	key := NewJobRuntimeKey(accountID, tableID, jobName)
	cancelErr := errors.New("iscp job consumer canceled")

	exec.runtimeMu.Lock()
	exec.ensureRuntimeMapsLocked()
	exec.fencedJobs[key] = struct{}{}
	handles := make([]*RunningJobConsumer, 0, len(exec.runningConsumers[key]))
	for _, h := range exec.runningConsumers[key] {
		handles = append(handles, h)
		if h.cancel != nil {
			h.cancel()
		}
		if h.cancelRetriever != nil {
			h.cancelRetriever(cancelErr)
		}
	}
	exec.runtimeMu.Unlock()

	for _, h := range handles {
		select {
		case <-h.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (exec *ISCPTaskExecutor) RegisterRunningConsumer(
	key JobRuntimeKey,
	jobID uint64,
	iterID uint64,
	cancel context.CancelFunc,
	cancelRetriever func(error),
) (*RunningJobConsumer, bool) {
	if exec == nil {
		return nil, true
	}
	exec.runtimeMu.Lock()
	defer exec.runtimeMu.Unlock()
	exec.ensureRuntimeMapsLocked()
	if _, fenced := exec.fencedJobs[key]; fenced {
		return nil, false
	}
	h := &RunningJobConsumer{
		key:             key,
		jobID:           jobID,
		iterID:          iterID,
		cancel:          cancel,
		cancelRetriever: cancelRetriever,
		done:            make(chan struct{}),
	}
	byJobID := exec.runningConsumers[key]
	if byJobID == nil {
		byJobID = make(map[uint64]*RunningJobConsumer)
		exec.runningConsumers[key] = byJobID
	}
	byJobID[jobID] = h
	return h, true
}

func (exec *ISCPTaskExecutor) UnregisterRunningConsumer(h *RunningJobConsumer) {
	if exec == nil || h == nil {
		return
	}
	exec.runtimeMu.Lock()
	if byJobID := exec.runningConsumers[h.key]; byJobID != nil {
		if byJobID[h.jobID] == h {
			delete(byJobID, h.jobID)
		}
		if len(byJobID) == 0 {
			delete(exec.runningConsumers, h.key)
		}
	}
	exec.runtimeMu.Unlock()
	close(h.done)
}

func (exec *ISCPTaskExecutor) filterFencedIteration(iter *IterationContext) *IterationContext {
	if exec == nil || iter == nil || len(iter.jobNames) == 0 {
		return iter
	}
	keepJobNames := make([]string, 0, len(iter.jobNames))
	keepJobIDs := make([]uint64, 0, len(iter.jobIDs))
	keepLSN := make([]uint64, 0, len(iter.lsn))
	for i, jobName := range iter.jobNames {
		key := NewJobRuntimeKey(iter.accountID, iter.tableID, jobName)
		if exec.IsJobFenced(key) {
			continue
		}
		keepJobNames = append(keepJobNames, jobName)
		keepJobIDs = append(keepJobIDs, iter.jobIDs[i])
		keepLSN = append(keepLSN, iter.lsn[i])
	}
	if len(keepJobNames) == 0 {
		return nil
	}
	if len(keepJobNames) == len(iter.jobNames) {
		return iter
	}
	return &IterationContext{
		accountID: iter.accountID,
		tableID:   iter.tableID,
		jobNames:  keepJobNames,
		jobIDs:    keepJobIDs,
		lsn:       keepLSN,
		fromTS:    iter.fromTS,
		toTS:      iter.toTS,
	}
}
