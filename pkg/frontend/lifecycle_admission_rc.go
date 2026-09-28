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
	"math"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// admitLocalLifecycleRC keeps the catalog identity, lifecycle row, optional
// Snapshot quota and target database in one background transaction.
func admitLocalLifecycleRC(
	ctx context.Context, ses *Session, bh BackgroundExec,
	accountID uint32, databaseName, snapshotScope string, databaseMode lock.LockMode,
	publicationName, tableName string,
) error {
	if databaseName == "" {
		return moerr.NewInternalError(ctx, "missing snapshot database")
	}
	admission, err := beginLifecycleRCAdmission(ctx, ses, bh)
	if err != nil {
		return err
	}
	defer admission.proc.Free()
	lockProc := admission.proc
	systemCtx := context.WithValue(defines.AttachAccountId(ctx, catalog.System_Account), defines.LockWriterFairKey{}, false)
	lockKey := admission.lockKey
	databaseKey, err := cloneCatalogLockBatch(lockProc, accountID, databaseName)
	if err != nil {
		return err
	}
	if err = lockKey(catalog.MO_DATABASE, ctx, databaseKey, databaseMode, accountID); err != nil {
		return err
	}
	if tableName != "" {
		tableKey, err := cloneCatalogLockBatch(lockProc, accountID, databaseName, tableName)
		if err != nil {
			return err
		}
		if err = lockKey(catalog.MO_TABLES, ctx, tableKey, lock.LockMode_Shared, accountID); err != nil {
			return err
		}
	}
	if snapshotScope != "" {
		// Target waits precede the shared quota key. A blocked Snapshot on A
		// must not queue an otherwise independent Snapshot on B behind Q.
		// Querying first initializes a missing quota row.
		if _, err = queryQuota(ctx, ses, bh, accountID, featureCodeSnapshot, snapshotScope); err != nil {
			return err
		}
		if _, err = lockFeatureQuota(ctx, ses, bh, accountID, featureCodeSnapshot, snapshotScope); err != nil {
			return err
		}
	} else {
		// The system retention key serializes local PITR publication only
		// after the target is admitted, including the first absent row.
		name := vector.NewVec(types.T_varchar.ToType())
		creator := vector.NewVec(types.T_uint64.ToType())
		defer name.Free(lockProc.Mp())
		defer creator.Free(lockProc.Mp())
		if err = vector.AppendBytes(name, []byte(SYSMOCATALOGPITR), false, lockProc.Mp()); err != nil {
			return err
		}
		if err = vector.AppendFixed(creator, uint64(sysAccountID), false, lockProc.Mp()); err != nil {
			return err
		}
		encoded, err := function.RunFunctionDirectly(
			lockProc, function.SerialFunctionEncodeID, []*vector.Vector{name, creator}, 1)
		if err != nil {
			return err
		}
		key := batch.NewWithSize(1)
		key.SetVector(0, encoded)
		if err = lockKey(catalog.MO_PITR, systemCtx, key, lock.LockMode_Exclusive, catalog.System_Account); err != nil {
			return err
		}
	}
	if publicationName != "" {
		// Publication writers lock D before updating P. Follow that order
		// so a database snapshot cannot invert their two-row dependency.
		if accountID > math.MaxInt32 {
			return moerr.NewInternalError(ctx, "publication account id exceeds catalog key range")
		}
		publisher := vector.NewVec(types.T_int32.ToType())
		name := vector.NewVec(types.T_varchar.ToType())
		defer publisher.Free(lockProc.Mp())
		defer name.Free(lockProc.Mp())
		if err = vector.AppendFixed(publisher, int32(accountID), false, lockProc.Mp()); err != nil {
			return err
		}
		if err = vector.AppendBytes(name, []byte(publicationName), false, lockProc.Mp()); err != nil {
			return err
		}
		encoded, err := function.RunFunctionDirectly(
			lockProc, function.SerialFunctionEncodeID, []*vector.Vector{publisher, name}, 1)
		if err != nil {
			return err
		}
		key := batch.NewWithSize(1)
		key.SetVector(0, encoded)
		if err = lockKey(catalog.MO_PUBS, systemCtx, key, lock.LockMode_Shared, catalog.System_Account); err != nil {
			return err
		}
	}
	return admission.finish(ctx, ses, bh)
}

// lifecycleRCAdmission owns only a temporary Process; its locks belong to the
// existing background transaction. Callers keep their established D/T/Q/P order
// between begin and finish. Branch publishers finish before their first write.
type lifecycleRCAdmission struct {
	proc       *process.Process
	registryID uint64
}

func beginLifecycleRCAdmission(ctx context.Context, ses *Session, bh BackgroundExec) (_ *lifecycleRCAdmission, err error) {
	proc, err := newCloneDatabaseTargetLockProcess(ctx, ses, bh)
	if err != nil {
		return nil, err
	}
	ready := false
	defer func() {
		if !ready {
			proc.Free()
		}
	}()
	op := proc.GetTxnOperator()
	if op == nil || !op.Txn().IsPessimistic() || !op.Txn().IsRCIsolation() {
		return nil, moerr.NewInternalError(ctx, "local snapshot requires pessimistic RC")
	}
	a := &lifecycleRCAdmission{proc: proc}
	systemCtx := context.WithValue(defines.AttachAccountId(ctx, catalog.System_Account), defines.LockWriterFairKey{}, false)
	identity, err := cloneCatalogLockBatch(proc, catalog.System_Account,
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY)
	if err != nil {
		return nil, err
	}
	if err = a.lockKey(catalog.MO_TABLES, systemCtx, identity, lock.LockMode_Shared, catalog.System_Account); err != nil {
		return nil, err
	}
	db, err := proc.GetSessionInfo().StorageEngine.Database(systemCtx, catalog.MO_CATALOG, op)
	if err != nil {
		return nil, err
	}
	registry, err := db.Relation(systemCtx, catalog.MO_FEATURE_REGISTRY, nil)
	if err != nil {
		return nil, err
	}
	a.registryID = registry.GetTableID(systemCtx)
	key := batch.NewWithSize(1)
	key.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	if err = vector.AppendBytes(key.Vecs[0], []byte(catalog.SnapshotLifecycleFeatureCode), false, proc.Mp()); err != nil {
		key.Vecs[0].Free(proc.Mp())
		return nil, err
	}
	if err = a.lockKey(catalog.MO_FEATURE_REGISTRY, systemCtx, key, lock.LockMode_Shared, catalog.System_Account); err != nil {
		return nil, err
	}
	ready = true
	return a, nil
}

func (a *lifecycleRCAdmission) lockKey(relationName string, ctx context.Context, keys *batch.Batch, mode lock.LockMode, group uint32) error {
	defer keys.Vecs[0].Free(a.proc.Mp())
	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	eng := a.proc.GetSessionInfo().StorageEngine
	db, err := eng.Database(systemCtx, catalog.MO_CATALOG, a.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	relation, err := db.Relation(systemCtx, relationName, nil)
	if err != nil {
		return err
	}
	return withCloneLockContext(a.proc, ctx, func() error {
		_, err := lockop.LockRowsForAdmissionWithContext(ctx, eng, a.proc,
			relation.GetTableID(systemCtx), keys, 0, *keys.Vecs[0].GetType(), mode, group)
		return err
	})
}

func (a *lifecycleRCAdmission) finish(ctx context.Context, ses *Session, bh BackgroundExec) error {
	if err := advanceFeatureLimitSnapshot(ctx, ses, bh); err != nil {
		return err
	}
	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	eng := a.proc.GetSessionInfo().StorageEngine
	db, err := eng.Database(systemCtx, catalog.MO_CATALOG, a.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	registry, err := db.Relation(systemCtx, catalog.MO_FEATURE_REGISTRY, nil)
	if err != nil {
		return err
	}
	if registry.GetTableID(systemCtx) != a.registryID {
		return moerr.NewTxnNeedRetryWithDefChanged(ctx)
	}
	_, _, exists, err := queryFeatureRegistry(systemCtx, ses, bh, featureCodeSnapshot)
	if err != nil {
		return err
	}
	if !exists {
		return moerr.NewInternalError(ctx, "missing SNAPSHOT lifecycle registry row")
	}
	return nil
}
