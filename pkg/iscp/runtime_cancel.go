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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
)

var (
	iscpExecutors sync.Map // map[cnUUID]*ISCPTaskExecutor
	cnJobFences   = struct {
		sync.Mutex
		fences map[cnJobFenceKey]JobFence
	}{
		fences: make(map[cnJobFenceKey]JobFence),
	}
)

const DefaultRollbackFenceTTL = 30 * time.Minute

type cnJobFenceKey struct {
	CNUUID string
	JobRuntimeKey
}

func RegisterExecutorRuntime(cnUUID string, exec *ISCPTaskExecutor) {
	if cnUUID == "" || exec == nil {
		return
	}
	now := time.Now()
	cnJobFences.Lock()
	cleanupExpiredCNJobFencesLocked(now)
	// Publish while holding the registry lock. A concurrent fence installation
	// is therefore either copied here or sees this published executor and
	// installs the executor-local copy through the request handler.
	exec.runtimeMu.Lock()
	exec.ensureRuntimeMapsLocked()
	clear(exec.fencedJobs)
	for fenceKey, fence := range cnJobFences.fences {
		if fenceKey.CNUUID == cnUUID {
			exec.fencedJobs[fenceKey.JobRuntimeKey] = fence
		}
	}
	iscpExecutors.Store(cnUUID, exec)
	exec.runtimeMu.Unlock()
	cnJobFences.Unlock()
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
		return nil, false
	}
	exec, ok := v.(*ISCPTaskExecutor)
	return exec, ok
}

func NewJobRuntimeKey(accountID uint32, tableID uint64, jobName string, jobID uint64) JobRuntimeKey {
	return JobRuntimeKey{AccountID: accountID, TableID: tableID, JobName: jobName, JobID: jobID}
}

// InstallCNJobFence records the fence outside a particular executor generation.
// A replacement executor published under the same CN UUID therefore observes
// the fence before admitting any consumer.
func InstallCNJobFence(cnUUID string, key JobRuntimeKey, ttl time.Duration) {
	if cnUUID == "" || ttl <= 0 {
		return
	}
	now := time.Now()
	cnJobFences.Lock()
	cleanupExpiredCNJobFencesLocked(now)
	cnJobFences.fences[cnJobFenceKey{CNUUID: cnUUID, JobRuntimeKey: key}] =
		JobFence{ExpireAt: now.Add(ttl)}
	cnJobFences.Unlock()
}

func RenewCNJobFence(cnUUID string, key JobRuntimeKey, ttl time.Duration) bool {
	if cnUUID == "" || ttl <= 0 {
		return false
	}
	now := time.Now()
	fenceKey := cnJobFenceKey{CNUUID: cnUUID, JobRuntimeKey: key}
	cnJobFences.Lock()
	defer cnJobFences.Unlock()
	cleanupExpiredCNJobFencesLocked(now)
	if _, ok := cnJobFences.fences[fenceKey]; !ok {
		return false
	}
	fence := JobFence{ExpireAt: now.Add(ttl)}
	if value, ok := iscpExecutors.Load(cnUUID); ok {
		exec, ok := value.(*ISCPTaskExecutor)
		if !ok || exec == nil {
			return false
		}
		exec.runtimeMu.Lock()
		defer exec.runtimeMu.Unlock()
		exec.ensureRuntimeMapsLocked()
		if !exec.isJobFencedLocked(key, now) {
			delete(cnJobFences.fences, fenceKey)
			return false
		}
		exec.fencedJobs[key] = fence
	}
	cnJobFences.fences[fenceKey] = fence
	return true
}

func RemoveCNJobFence(cnUUID string, key JobRuntimeKey) {
	if cnUUID == "" {
		return
	}
	cnJobFences.Lock()
	delete(cnJobFences.fences, cnJobFenceKey{CNUUID: cnUUID, JobRuntimeKey: key})
	if value, ok := iscpExecutors.Load(cnUUID); ok {
		if exec, ok := value.(*ISCPTaskExecutor); ok && exec != nil {
			exec.runtimeMu.Lock()
			delete(exec.fencedJobs, key)
			exec.runtimeMu.Unlock()
		}
	}
	cnJobFences.Unlock()
}

func removeCNTableJobFences(cnUUID string, accountID uint32, tableID uint64) {
	if cnUUID == "" {
		return
	}
	cnJobFences.Lock()
	for key := range cnJobFences.fences {
		if key.CNUUID == cnUUID &&
			key.AccountID == accountID &&
			key.TableID == tableID {
			delete(cnJobFences.fences, key)
		}
	}
	if value, ok := iscpExecutors.Load(cnUUID); ok {
		if exec, ok := value.(*ISCPTaskExecutor); ok && exec != nil {
			exec.runtimeMu.Lock()
			for key := range exec.fencedJobs {
				if key.AccountID == accountID && key.TableID == tableID {
					delete(exec.fencedJobs, key)
				}
			}
			exec.runtimeMu.Unlock()
		}
	}
	cnJobFences.Unlock()
}

func cleanupExpiredCNJobFencesLocked(now time.Time) {
	for key, fence := range cnJobFences.fences {
		if !fence.ExpireAt.IsZero() && now.After(fence.ExpireAt) {
			delete(cnJobFences.fences, key)
		}
	}
}

func (key JobRuntimeKey) consumerGroupKey() JobRuntimeKey {
	key.JobID = 0
	return key
}

func (exec *ISCPTaskExecutor) ensureRuntimeMapsLocked() {
	if exec.fencedJobs == nil {
		exec.fencedJobs = make(map[JobRuntimeKey]JobFence)
	}
	if exec.runningConsumers == nil {
		exec.runningConsumers = make(map[JobRuntimeKey]map[uint64]*RunningJobConsumer)
	}
}

func RollbackFenceTTL() time.Duration {
	ttl := DefaultRollbackFenceTTL
	if seconds, _, injected := fault.TriggerFault(objectio.FJ_ISCPCancelRollbackFenceTTL); injected && seconds > 0 {
		ttl = time.Duration(seconds) * time.Second
	}
	return ttl
}

func (exec *ISCPTaskExecutor) IsJobFenced(key JobRuntimeKey) bool {
	if exec == nil {
		return false
	}
	exec.runtimeMu.Lock()
	defer exec.runtimeMu.Unlock()
	return exec.isJobFencedLocked(key, time.Now())
}

func (exec *ISCPTaskExecutor) isJobFencedLocked(key JobRuntimeKey, now time.Time) bool {
	fence, ok := exec.fencedJobs[key]
	if !ok {
		return false
	}
	if !fence.ExpireAt.IsZero() && now.After(fence.ExpireAt) {
		delete(exec.fencedJobs, key)
		return false
	}
	return true
}

// InstallJobFence upserts both the executor-local and generation-independent CN fence.
func (exec *ISCPTaskExecutor) InstallJobFence(key JobRuntimeKey, ttl time.Duration) {
	if exec == nil || ttl <= 0 {
		return
	}
	now := time.Now()
	fence := JobFence{ExpireAt: now.Add(ttl)}
	if exec.cnUUID != "" {
		cnJobFences.Lock()
		defer cnJobFences.Unlock()
		cleanupExpiredCNJobFencesLocked(now)
		cnJobFences.fences[cnJobFenceKey{CNUUID: exec.cnUUID, JobRuntimeKey: key}] = fence
	}
	exec.runtimeMu.Lock()
	exec.ensureRuntimeMapsLocked()
	exec.fencedJobs[key] = fence
	exec.runtimeMu.Unlock()
}

func (exec *ISCPTaskExecutor) CancelAndDrainJobConsumer(
	ctx context.Context,
	accountID uint32,
	tableID uint64,
	jobName string,
	jobID uint64,
) error {
	if exec == nil {
		return nil
	}
	key := NewJobRuntimeKey(accountID, tableID, jobName, jobID)
	groupKey := key.consumerGroupKey()
	cancelErr := moerr.NewInternalErrorNoCtx("iscp job consumer canceled")

	ttl := RollbackFenceTTL()
	exec.InstallJobFence(key, ttl)
	exec.runtimeMu.Lock()
	exec.ensureRuntimeMapsLocked()
	handles := make([]*RunningJobConsumer, 0, 1)
	if byJobID := exec.runningConsumers[groupKey]; byJobID != nil {
		for runningJobID, h := range byJobID {
			if jobID != 0 && runningJobID != jobID {
				continue
			}
			handles = append(handles, h)
			if h.cancel != nil {
				h.cancel()
			}
			if h.cancelRetriever != nil {
				h.cancelRetriever(cancelErr)
			}
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

func (exec *ISCPTaskExecutor) RemoveJobFence(key JobRuntimeKey) {
	if exec == nil {
		return
	}
	RemoveCNJobFence(exec.cnUUID, key)
	exec.runtimeMu.Lock()
	delete(exec.fencedJobs, key)
	exec.runtimeMu.Unlock()
}

func (exec *ISCPTaskExecutor) RenewJobFence(key JobRuntimeKey, ttl time.Duration) bool {
	if exec == nil || ttl <= 0 {
		return false
	}
	now := time.Now()
	if exec.cnUUID != "" {
		return RenewCNJobFence(exec.cnUUID, key, ttl)
	}

	exec.runtimeMu.Lock()
	defer exec.runtimeMu.Unlock()
	exec.ensureRuntimeMapsLocked()
	if !exec.isJobFencedLocked(key, now) {
		return false
	}
	exec.fencedJobs[key] = JobFence{ExpireAt: now.Add(ttl)}
	return true
}

func (exec *ISCPTaskExecutor) RemoveTableJobFences(accountID uint32, tableID uint64) {
	if exec == nil {
		return
	}
	removeCNTableJobFences(exec.cnUUID, accountID, tableID)
	exec.runtimeMu.Lock()
	for key := range exec.fencedJobs {
		if key.AccountID == accountID && key.TableID == tableID {
			delete(exec.fencedJobs, key)
		}
	}
	exec.runtimeMu.Unlock()
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
	if exec.isJobFencedLocked(key, time.Now()) {
		return nil, false
	}
	groupKey := key.consumerGroupKey()
	h := &RunningJobConsumer{
		key:             key,
		jobID:           jobID,
		iterID:          iterID,
		cancel:          cancel,
		cancelRetriever: cancelRetriever,
		done:            make(chan struct{}),
	}
	byJobID := exec.runningConsumers[groupKey]
	if byJobID == nil {
		byJobID = make(map[uint64]*RunningJobConsumer)
		exec.runningConsumers[groupKey] = byJobID
	}
	byJobID[jobID] = h
	return h, true
}

func (exec *ISCPTaskExecutor) UnregisterRunningConsumer(h *RunningJobConsumer) {
	if exec == nil || h == nil {
		return
	}
	exec.runtimeMu.Lock()
	groupKey := h.key.consumerGroupKey()
	if byJobID := exec.runningConsumers[groupKey]; byJobID != nil {
		if byJobID[h.jobID] == h {
			delete(byJobID, h.jobID)
		}
		if len(byJobID) == 0 {
			delete(exec.runningConsumers, groupKey)
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
	keepStages := make([]int8, 0, len(iter.stages))
	for i, jobName := range iter.jobNames {
		key := NewJobRuntimeKey(iter.accountID, iter.tableID, jobName, iter.jobIDs[i])
		if exec.IsJobFenced(key) {
			continue
		}
		keepJobNames = append(keepJobNames, jobName)
		keepJobIDs = append(keepJobIDs, iter.jobIDs[i])
		keepLSN = append(keepLSN, iter.lsn[i])
		if len(iter.stages) == len(iter.jobNames) {
			keepStages = append(keepStages, iter.stages[i])
		}
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
		stages:    keepStages,
		fromTS:    iter.fromTS,
		toTS:      iter.toTS,
	}
}
