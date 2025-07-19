// Copyright 2021 Matrix Origin
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

package checkpoint

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

func (executor *checkpointExecutor) TriggerExecutingGCKP(ctx *gckpContext) (err error) {
	if !executor.active.Load() {
		err = ErrExecutorClosed
		return
	}
	_, err = executor.gckpQueue.Enqueue(ctx)
	return
}

func (executor *checkpointExecutor) RunGCKP(gckpCtx *gckpContext) (err error) {
	if !executor.active.Load() {
		err = ErrCheckpointDisabled
		return
	}
	if executor.runningGCKP.Load() != nil {
		err = ErrPendingCheckpoint
	}
	job := &checkpointJob{
		doneCh:      make(chan struct{}),
		executor:    executor,
		gckpCtx:     gckpCtx,
		runGCKPFunc: executor.runGCKPFunc,
	}
	if !executor.runningGCKP.CompareAndSwap(nil, job) {
		err = ErrPendingCheckpoint
		return
	}
	defer func() {
		job.Done(err)
		executor.runningGCKP.Store(nil)
	}()
	err = job.RunGCKP(executor.ctx)
	return
}

func (executor *checkpointExecutor) onGCKPEntries(items ...any) {
	var (
		err              error
		mergedCtx        *gckpContext
		fromCheckpointed types.TS
		toCheckpointed   types.TS
		now              = time.Now()
	)
	defer func() {
		var createdEntry string
		logger := logutil.Debug
		if err != nil {
			logger = logutil.Error
		} else {
			toEntry := executor.runner.store.MaxGlobalCheckpoint()
			if toEntry != nil {
				toCheckpointed = toEntry.GetEnd()
				createdEntry = toEntry.String()
			}
		}

		if err != nil || time.Since(now) > time.Second*10 || toCheckpointed.GT(&fromCheckpointed) {
			logger(
				"GCKP-Execute-End",
				zap.Duration("cost", time.Since(now)),
				zap.String("ctx", mergedCtx.String()),
				zap.String("created", createdEntry),
				zap.Error(err),
			)
		}
	}()

	for _, item := range items {
		oneCtx := item.(*gckpContext)
		if mergedCtx == nil {
			mergedCtx = oneCtx
		} else {
			mergedCtx.Merge(oneCtx)
		}
	}
	if mergedCtx == nil {
		return
	}

	if mergedCtx.histroyRetention == 0 {
		mergedCtx.histroyRetention = executor.cfg.GlobalHistoryDuration
	}

	fromEntry := executor.runner.store.MaxGlobalCheckpoint()
	if fromEntry != nil {
		fromCheckpointed = fromEntry.GetEnd()
	}

	if mergedCtx.end.LE(&fromCheckpointed) {
		logutil.Info(
			"GCKP-Execute-Skip",
			zap.String("have", fromCheckpointed.ToString()),
			zap.String("want", mergedCtx.end.ToString()),
		)
		return
	}

	// [force==false and ickpCount < count policy]
	if !mergedCtx.force {
		ickpCount := executor.runner.store.GetIncrementalCountAfterGlobal()
		if !executor.globalPolicy.Check(ickpCount) {
			logutil.Debug(
				"GCKP-Execute-Skip",
				zap.Int("pending-ickp", ickpCount),
				zap.String("want", mergedCtx.end.ToString()),
			)
			return
		}
	}

	err = executor.RunGCKP(mergedCtx)
}
