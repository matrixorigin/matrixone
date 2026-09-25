// Copyright 2026 Matrix Origin
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

package frontend

import (
	"context"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// withCDCSourceGenerationGuard holds the source name-to-ID catalog locks until
// the synchronous target acknowledgement has committed.
func (exec *CDCTaskExecutor) withCDCSourceGenerationGuard(
	ctx context.Context, accountID uint32, databaseName, tableName string,
	expectedID uint64, acknowledge func() error,
) error {
	tenantCtx := defines.AttachAccountId(ctx, accountID)
	op, err := exec.cnTxnClient.New(tenantCtx, types.TS{}.ToTimestamp(),
		client.WithSkipPushClientReady(),
		client.WithTxnMode(txn.TxnMode_Pessimistic),
		client.WithTxnIsolation(txn.TxnIsolation_RC),
		client.WithTxnCreateBy(0, "", "cdc-source-generation-ack", 0))
	if err != nil {
		return err
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), exec.cnEngine.Hints().CommitOrRollbackTimeout)
		defer cancel()
		_ = op.Rollback(rollbackCtx)
	}()
	if err = exec.cnEngine.New(tenantCtx, op); err != nil {
		return err
	}
	pu := getPu(exec.cnUUID)
	proc := process.NewTopProcess(tenantCtx, exec.mp, exec.cnTxnClient, op,
		pu.FileService, pu.LockService, pu.QueryClient, pu.HAKeeperClient,
		pu.UdfService, getAicm(exec.cnUUID), pu.GetTaskService())
	defer proc.Free()
	proc.Base.SessionInfo.StorageEngine = exec.cnEngine
	catalogDB, err := exec.cnEngine.Database(tenantCtx, catalog.MO_CATALOG, op)
	if err != nil {
		return err
	}
	for _, target := range []struct {
		name  string
		parts []string
	}{
		{catalog.MO_DATABASE, []string{databaseName}},
		{catalog.MO_TABLES, []string{databaseName, tableName}},
	} {
		rel, relErr := catalogDB.Relation(tenantCtx, target.name, nil)
		if relErr != nil {
			return relErr
		}
		if err = lockCDCCatalogName(proc, tenantCtx, exec.cnEngine, rel, accountID, target.parts...); err != nil {
			return fmt.Errorf("CDC source guard lock %s: %w", target.name, err)
		}
	}
	now, _ := moruntime.ServiceRuntime(exec.cnUUID).Clock().Now()
	if err = op.GetWorkspace().AdvanceSnapshot(tenantCtx, now); err != nil {
		return fmt.Errorf("CDC source guard advance snapshot: %w", err)
	}
	db, err := exec.cnEngine.Database(tenantCtx, databaseName, op)
	if err != nil {
		return fmt.Errorf("CDC source guard resolve database: %w", err)
	}
	rel, err := db.Relation(tenantCtx, tableName, nil)
	if err != nil {
		return fmt.Errorf("CDC source guard resolve table: %w", err)
	}
	if actual := rel.GetTableID(tenantCtx); actual != expectedID {
		return moerr.NewInternalErrorf(ctx,
			"CDC source %s.%s changed before first target acknowledgement (%d -> %d); explicit recovery is required",
			databaseName, tableName, expectedID, actual)
	}
	cdcSourceGuardTestHook.RLock()
	hook := cdcSourceGuardTestHook.fn
	cdcSourceGuardTestHook.RUnlock()
	if hook != nil {
		hook()
	}
	return acknowledge()
}

var cdcSourceGuardTestHook struct {
	sync.RWMutex
	fn func()
}

// SetCDCSourceGuardHookForTest pauses first ACK after its source locks are held.
func SetCDCSourceGuardHookForTest(hook func()) (restore func()) {
	cdcSourceGuardTestHook.Lock()
	previous := cdcSourceGuardTestHook.fn
	cdcSourceGuardTestHook.fn = hook
	cdcSourceGuardTestHook.Unlock()
	return func() {
		cdcSourceGuardTestHook.Lock()
		cdcSourceGuardTestHook.fn = previous
		cdcSourceGuardTestHook.Unlock()
	}
}
