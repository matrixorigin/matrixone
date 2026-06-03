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

package casgc

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// DatalinkCASGCCronExpr fires the sweep at the top of every hour (6-field cron).
var DatalinkCASGCCronExpr = "0 0 * * * *"

// DatalinkCASGCTaskMetadata builds the cron-task metadata for the CAS GC sweep.
func DatalinkCASGCTaskMetadata(id task.TaskCode) task.TaskMetadata {
	return task.TaskMetadata{
		ID:       "DatalinkCASGCTask",
		Executor: id,
		Options:  task.TaskOptions{Concurrency: 1},
	}
}

// running guards against overlapping cron firings stacking up.
var running atomic.Bool

// sweeperOnce + sweeperSingleton make the Sweeper persist across cron firings.
//
// The Sweeper's two-pass grace window relies on in-memory `pending` state that
// records when each blob was first seen as an orphan. If a fresh Sweeper were
// built on every firing, that state would reset each hour and the grace window
// could never elapse, so nothing would ever be collected. We therefore build
// the Sweeper exactly once (lazily, on the first firing) and reuse it on every
// subsequent firing so `pending` survives between runs.
var (
	sweeperOnce sync.Once
	sweeper     *Sweeper
)

// DatalinkCASGCExecutorFactory returns the cron executor closure for the
// datalink CAS garbage-collection sweep. The shared file service holding the
// CAS is provided by the CN service at registration time.
func DatalinkCASGCExecutorFactory(
	cnUUID string,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	fs fileservice.FileService,
	_ *mpool.MPool,
) func(ctx context.Context, t task.Task) error {
	return func(ctx context.Context, _ task.Task) error {
		if !running.CompareAndSwap(false, true) {
			return nil
		}
		defer running.Store(false)

		// Build the persistent Sweeper exactly once so the grace-window state
		// survives across firings.
		sweeperOnce.Do(func() {
			sharedFS, err := fileservice.Get[fileservice.FileService](fs, defines.SharedFileServiceName)
			if err != nil {
				logutil.Errorf("casgc: failed to get SHARED file service: %v", err)
				return
			}
			env := &sqlEnv{uuid: cnUUID, engine: txnEngine, txnClient: cnTxnClient}
			sweeper = NewSweeper(sharedFS, Config{}, env)
		})

		if sweeper == nil {
			// SHARED fs acquisition failed; best-effort cron, skip this run.
			return nil
		}

		logutil.Infof("casgc: datalink CAS GC sweep START")
		deleted, err := sweeper.SweepAll(ctx)
		if err != nil {
			logutil.Errorf("casgc: datalink CAS GC sweep error: %v", err)
		}
		logutil.Infof("casgc: datalink CAS GC sweep END, deleted %d blob(s)", deleted)
		return nil
	}
}
