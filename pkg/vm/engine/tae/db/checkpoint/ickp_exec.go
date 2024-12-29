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

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

func (e *checkpointExecutor) TriggerExecutingICKP() (err error) {
	if !e.active.Load() {
		err = ErrExecutorClosed
		return
	}
	_, err = e.ickpQueue.Enqueue(struct{}{})
	return
}

func (e *checkpointExecutor) RunICKP() (err error) {
	if !e.active.Load() {
		err = ErrCheckpointDisabled
		return
	}
	if e.runningICKP.Load() != nil {
		err = ErrPendingCheckpoint
	}
	job := &checkpointJob{
		doneCh:      make(chan struct{}),
		runner:      e.runner,
		runICKPFunc: e.runICKPFunc,
	}
	if !e.runningICKP.CompareAndSwap(nil, job) {
		err = ErrPendingCheckpoint
		return
	}
	defer func() {
		job.Done(err)
		e.runningICKP.Store(nil)
	}()
	err = job.RunICKP(e.ctx)
	return
}

func (executor *checkpointExecutor) onICKPEntries(items ...any) {
	var (
		err error
		now = time.Now()
	)
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		// if err != nil || time.Since(now) > e.slowThreshold {
		if err != nil || time.Since(now) > time.Second {
			logger(
				"ICKP-Execute-End",
				zap.Duration("cost", time.Since(now)),
				zap.Error(err),
			)
		}
	}()

	err = executor.RunICKP()
}
