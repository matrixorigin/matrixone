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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
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
		return nil, false
	}
	exec, ok := v.(*ISCPTaskExecutor)
	return exec, ok
}

func NewJobRuntimeKey(accountID uint32, tableID uint64, jobName string, jobID uint64) JobRuntimeKey {
	return JobRuntimeKey{AccountID: accountID, TableID: tableID, JobName: jobName, JobID: jobID}
}

func (key JobRuntimeKey) consumerGroupKey() JobRuntimeKey {
	key.JobID = 0
	return key
}

func (exec *ISCPTaskExecutor) ensureRuntimeMapsLocked() {
	if exec.fencedJobs == nil {
		exec.fencedJobs = make(map[JobRuntimeKey]struct{})
	}
	if exec.runningConsumers == nil {
		exec.runningConsumers = make(map[JobRuntimeKey]map[uint64]*RunningJobConsumer)
	}
	if exec.cancelingJobs == nil {
		exec.cancelingJobs = make(map[JobRuntimeKey]struct{})
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
	jobID uint64,
) error {
	if exec == nil {
		return nil
	}
	key := NewJobRuntimeKey(accountID, tableID, jobName, jobID)
	groupKey := key.consumerGroupKey()
	cancelErr := moerr.NewInternalErrorNoCtx("iscp job consumer canceled")

	exec.runtimeMu.Lock()
	exec.ensureRuntimeMapsLocked()
	exec.fencedJobs[key] = struct{}{}
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

func (exec *ISCPTaskExecutor) enqueueCancelJob(key JobRuntimeKey) {
	if exec == nil {
		return
	}
	exec.runtimeMu.Lock()
	exec.ensureRuntimeMapsLocked()
	if _, ok := exec.cancelingJobs[key]; ok {
		exec.runtimeMu.Unlock()
		return
	}
	exec.cancelingJobs[key] = struct{}{}
	cancelJobs := exec.cancelJobs
	exec.runtimeMu.Unlock()

	if cancelJobs == nil {
		exec.clearCancelingJob(key)
		return
	}
	select {
	case cancelJobs <- key:
	default:
		go func() {
			ctx := exec.ctx
			if ctx == nil {
				exec.clearCancelingJob(key)
				return
			}
			select {
			case cancelJobs <- key:
			case <-ctx.Done():
				exec.clearCancelingJob(key)
			}
		}()
	}
}

func (exec *ISCPTaskExecutor) requeueCancelJob(ctx context.Context, key JobRuntimeKey) {
	if exec == nil {
		return
	}
	go func() {
		timer := time.NewTimer(DefaultRetryInterval)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			exec.clearCancelingJob(key)
			return
		}
		cancelJobs := exec.cancelJobs
		if cancelJobs == nil {
			exec.clearCancelingJob(key)
			return
		}
		select {
		case cancelJobs <- key:
		case <-ctx.Done():
			exec.clearCancelingJob(key)
		}
	}()
}

func (exec *ISCPTaskExecutor) clearCancelingJob(key JobRuntimeKey) {
	exec.runtimeMu.Lock()
	delete(exec.cancelingJobs, key)
	exec.runtimeMu.Unlock()
}

func (exec *ISCPTaskExecutor) finishCanceledJob(ctx context.Context, key JobRuntimeKey) error {
	if exec == nil {
		return nil
	}
	if err := exec.CancelAndDrainJobConsumer(ctx, key.AccountID, key.TableID, key.JobName, key.JobID); err != nil {
		return err
	}
	if err := exec.markJobCanceled(ctx, key); err != nil {
		return err
	}
	exec.clearCancelingJob(key)
	exec.RemoveJobFence(key)
	return nil
}

func (exec *ISCPTaskExecutor) markJobCanceled(ctx context.Context, key JobRuntimeKey) (err error) {
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithTimeoutCause(ctx, time.Minute*5, moerr.NewInternalErrorNoCtx("iscp cancel job timeout"))
	defer cancel()
	if _, _, injected := fault.TriggerFaultWithContext(ctx, objectio.FJ_ISCPCancelBeforeMarkCanceled); injected {
		logutil.Infof(
			"ISCP-Task cancel fault triggered %s: accountID=%d tableID=%d jobName=%s jobID=%d",
			objectio.FJ_ISCPCancelBeforeMarkCanceled,
			key.AccountID,
			key.TableID,
			key.JobName,
			key.JobID,
		)
	}
	if _, msg, injected := fault.TriggerFault(objectio.FJ_ISCPCancelMarkCanceledError); injected {
		if msg == "" {
			msg = objectio.FJ_ISCPCancelMarkCanceledError
		}
		return moerr.NewInternalErrorNoCtxf("injected ISCP cancel mark error: %s", msg)
	}
	txnOp, err := getTxn(ctx, exec.txnEngine, exec.cnTxnClient, "iscp cancel job")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, txnOp.Rollback(ctx))
			return
		}
		err = txnOp.Commit(ctx)
	}()
	sql := cdc.CDCSQLBuilder.ISCPLogUpdateStateSQL(
		key.AccountID,
		key.TableID,
		key.JobName,
		key.JobID,
		ISCPJobState_Canceled,
	)
	result, err := ExecWithResult(ctx, sql, exec.cnUUID, txnOp)
	if err != nil {
		return err
	}
	result.Close()
	logutil.Infof(
		"ISCP-Task marked job canceled: accountID=%d tableID=%d jobName=%s jobID=%d",
		key.AccountID,
		key.TableID,
		key.JobName,
		key.JobID,
	)
	return nil
}

func (exec *ISCPTaskExecutor) RemoveJobFence(key JobRuntimeKey) {
	if exec == nil {
		return
	}
	exec.runtimeMu.Lock()
	delete(exec.fencedJobs, key)
	exec.runtimeMu.Unlock()
}

func (exec *ISCPTaskExecutor) RemoveTableJobFences(accountID uint32, tableID uint64) {
	if exec == nil {
		return
	}
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
	if _, fenced := exec.fencedJobs[key]; fenced {
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
	for i, jobName := range iter.jobNames {
		key := NewJobRuntimeKey(iter.accountID, iter.tableID, jobName, iter.jobIDs[i])
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
