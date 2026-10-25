// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iscp

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// NewWatermarkBatchForTest supplies scheduler state to the external SQL test.
// The production transaction, SQL, reservation and rollback paths run unchanged.
func NewWatermarkBatchForTest(ctx context.Context, sid string, eng engine.Engine, txns client.TxnClient) (func() error, func() []types.TS) {
	exec := &ISCPTaskExecutor{ctx: ctx, cnUUID: sid, txnEngine: eng, cnTxnClient: txns, tables: newISCPTableTree()}
	jobs := []*JobEntry{newWatermarkFlushTestJob(exec, 3, "first"), newWatermarkFlushTestJob(exec, 4, "second")}
	return func() error { return exec.FlushWatermarkForAllTables(0) }, func() []types.TS {
		result := make([]types.TS, 0, len(jobs))
		for _, job := range jobs {
			job.tableInfo.mu.RLock()
			result = append(result, job.persistedWatermark)
			job.tableInfo.mu.RUnlock()
		}
		return result
	}
}
