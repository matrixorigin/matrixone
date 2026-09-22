// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type accountLifecycleGate uint8

const (
	accountLifecycleSnapshotGate accountLifecycleGate = iota
	accountLifecycleViewGate
)

// acquireAccountLifecycleSharedGates is a variable only so frontend unit tests
// with a synthetic BackgroundExec can replace the physical lock acquisition.
// Production always uses acquireAccountLifecycleSharedGateRows.
var acquireAccountLifecycleSharedGates = acquireAccountLifecycleSharedGateRows

var accountLifecycleLockEventHook atomic.Pointer[func(string, bool)]

// SetAccountLifecycleLockEventHookForTest installs a process-local observer for
// direct gate attempts. The bool is false immediately before physical
// acquisition and true after it succeeds.
func SetAccountLifecycleLockEventHookForTest(hook func(string, bool)) func() {
	previous := accountLifecycleLockEventHook.Load()
	if hook == nil {
		accountLifecycleLockEventHook.Store(nil)
	} else {
		accountLifecycleLockEventHook.Store(&hook)
	}
	return func() { accountLifecycleLockEventHook.Store(previous) }
}

// acquireAccountLifecycleSharedGateRows performs the writer-fair admission on
// the initiating CN, before the verification SELECT can be sent through an old
// pipeline node. The SELECT then re-enters the lock under the same transaction.
// A remote old owner rejects the capability-bearing request before admission;
// lockservice retries it as Exclusive, preserving the legacy safe behavior.
func acquireAccountLifecycleSharedGateRows(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	gates ...accountLifecycleGate,
) error {
	systemCtx := defines.AttachLockWriterFair(
		defines.AttachAccountId(ctx, catalog.System_Account),
	)
	lockProc, err := newCloneDatabaseTargetLockProcess(systemCtx, ses, bh)
	if err != nil {
		return err
	}
	defer lockProc.Free()

	eng := lockProc.GetSessionInfo().StorageEngine
	txnOp := lockProc.GetTxnOperator()
	if err := validateAccountLifecycleTxn(ctx, txnOp); err != nil {
		return err
	}
	db, err := eng.Database(systemCtx, catalog.MO_CATALOG, txnOp)
	if err != nil {
		return err
	}

	for _, gate := range gates {
		var relationName string
		var gateName string
		var lockBat *batch.Batch
		switch gate {
		case accountLifecycleSnapshotGate:
			gateName = "snapshot"
			relationName = catalog.MO_FEATURE_REGISTRY
			lockBat = batch.NewWithSize(1)
			vec := vector.NewVec(types.T_varchar.ToType())
			if err := vector.AppendBytes(
				vec,
				[]byte(catalog.SnapshotLifecycleFeatureCode),
				false,
				lockProc.Mp(),
			); err != nil {
				vec.Free(lockProc.Mp())
				return err
			}
			lockBat.SetVector(0, vec)
		case accountLifecycleViewGate:
			gateName = "view"
			relationName = catalog.MO_TABLES
			lockBat, err = cloneCatalogLockBatch(
				lockProc,
				catalog.System_Account,
				catalog.MO_CATALOG,
				catalog.MO_VIEW_REFRESH,
			)
			if err != nil {
				return err
			}
		default:
			panic("unknown account lifecycle gate")
		}
		rel, err := db.Relation(systemCtx, relationName, nil)
		if err != nil {
			lockBat.Vecs[0].Free(lockProc.Mp())
			return err
		}
		if hook := accountLifecycleLockEventHook.Load(); hook != nil {
			(*hook)(gateName, false)
		}
		err = withCloneLockContext(lockProc, systemCtx, func() error {
			return lockop.LockRows(
				eng,
				lockProc,
				rel,
				rel.GetTableID(systemCtx),
				lockBat,
				0,
				*lockBat.Vecs[0].GetType(),
				lockpb.LockMode_Shared,
				lockpb.Sharding_None,
				catalog.System_Account,
			)
		})
		lockBat.Vecs[0].Free(lockProc.Mp())
		if err != nil {
			return err
		}
		if hook := accountLifecycleLockEventHook.Load(); hook != nil {
			(*hook)(gateName, true)
		}
	}
	return nil
}

func validateAccountLifecycleTxn(ctx context.Context, txnOp client.TxnOperator) error {
	if txnOp == nil || !txnOp.Txn().IsPessimistic() {
		return moerr.NewInternalError(ctx,
			"account lifecycle gates require a pessimistic transaction")
	}
	return nil
}
