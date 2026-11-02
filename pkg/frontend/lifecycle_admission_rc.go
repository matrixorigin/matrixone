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
	"math"
	"sync/atomic"

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
	return admitLifecycleRCSplit(ctx, ses, bh, 0, "", accountID, accountID,
		databaseName, snapshotScope, databaseMode, publicationName, tableName)
}

// lifecycleAdmissionPlan is the ordered lock-key plan of
// admitLifecycleRCSplit as a pure value (design §4.4 step 3): the alias form
// prepends the subscriber database key A = (S, N); D/T bind the physical
// owner under databaseMode (D) and Shared mode (T); Q is the physical
// owner's quota pair, or the system PITR retention key for database-less
// scopes; P is the mo_pubs key (P_pub, pubName) under the system account.
// Planning as data lets the ordering and the local/publication byte-identity
// be asserted without lock machinery.
type lifecycleAdmissionPlan struct {
	// A: present only for the subscription alias form (finding 12).
	aliasAccount uint32
	aliasDb      string
	// D and T: the physical owner carries the target catalog rows.
	physAccount  uint32
	databaseName string
	databaseMode lock.LockMode
	tableName    string
	// Q: the physical owner's Snapshot quota pair; the system PITR
	// retention key when the scope carries no database.
	quotaScope string
	pitr       bool
	// P: the publication row key, emitted only for publication forms.
	publisher       uint32
	publicationName string
}

// planLifecycleAdmission validates the admission arguments and computes the
// ordered key plan. A missing database fails before any step; a publisher id
// outside the catalog key range fails before the P step exists.
func planLifecycleAdmission(
	subscriberAccountID uint32, aliasDb string,
	physAccountID uint32, publisherAccountID uint32,
	databaseName, snapshotScope string, databaseMode lock.LockMode,
	publicationName, tableName string,
) (lifecycleAdmissionPlan, error) {
	if databaseName == "" {
		return lifecycleAdmissionPlan{}, moerr.NewInternalErrorNoCtx("missing snapshot database")
	}
	if publicationName != "" && publisherAccountID > math.MaxInt32 {
		return lifecycleAdmissionPlan{}, moerr.NewInternalErrorNoCtx("publication account id exceeds catalog key range")
	}
	return lifecycleAdmissionPlan{
		aliasAccount:    subscriberAccountID,
		aliasDb:         aliasDb,
		physAccount:     physAccountID,
		databaseName:    databaseName,
		tableName:       tableName,
		databaseMode:    databaseMode,
		quotaScope:      snapshotScope,
		pitr:            snapshotScope == "",
		publisher:       publisherAccountID,
		publicationName: publicationName,
	}, nil
}

// lifecycleAdmissionLockOps abstracts admitLifecycleRCSplit's physical lock
// steps so unit tests can drive deterministic interleavings and failure
// injection through the actual admission body (the
// resolveBranchSubscriptionMeta gostub precedent): the production binding is
// lifecycleRCAdmission; tests substitute recording fakes whose hooks fire at
// named key steps (e.g., a rebind while the alias key A is notionally held).
type lifecycleAdmissionLockOps interface {
	lockProcess() *process.Process
	lockKey(relationName string, ctx context.Context, keys *batch.Batch, mode lock.LockMode, group uint32) error
	finish(ctx context.Context, ses *Session, bh BackgroundExec) error
	free()
}

// lifecycleAdmissionLockOpsImpl is the production adapter: every method
// forwards to the real admission unchanged.
type lifecycleAdmissionLockOpsImpl struct {
	admission *lifecycleRCAdmission
}

func (o *lifecycleAdmissionLockOpsImpl) lockProcess() *process.Process {
	return o.admission.proc
}

func (o *lifecycleAdmissionLockOpsImpl) lockKey(
	relationName string, ctx context.Context, keys *batch.Batch, mode lock.LockMode, group uint32,
) error {
	return o.admission.lockKey(relationName, ctx, keys, mode, group)
}

func (o *lifecycleAdmissionLockOpsImpl) finish(ctx context.Context, ses *Session, bh BackgroundExec) error {
	return o.admission.finish(ctx, ses, bh)
}

func (o *lifecycleAdmissionLockOpsImpl) free() {
	o.admission.proc.Free()
}

// beginLifecycleAdmission is the production binding of the admission
// constructor; unit tests substitute it with a recording fake.
var beginLifecycleAdmission = func(
	ctx context.Context, ses *Session, bh BackgroundExec,
) (lifecycleAdmissionLockOps, error) {
	admission, err := beginLifecycleRCAdmission(ctx, ses, bh, lock.LockMode_Shared)
	if err != nil {
		return nil, err
	}
	return &lifecycleAdmissionLockOpsImpl{admission: admission}, nil
}

// admitLifecycleRCSplit is the shared admission body. The physical owner
// carries the D/T/Q keys and the publication account carries the mo_pubs
// key; the subscription alias form additionally prepends the subscriber
// database key (S, N) at the head of the lock order (design §4.4 step 3,
// finding 12). With aliasDb == "" and physAccountID == publisherAccountID
// the emitted lock-key sequence is byte-identical to the pre-split
// single-account flow.
func admitLifecycleRCSplit(
	ctx context.Context, ses *Session, bh BackgroundExec,
	subscriberAccountID uint32, aliasDb string,
	physAccountID uint32, publisherAccountID uint32,
	databaseName, snapshotScope string, databaseMode lock.LockMode,
	publicationName, tableName string,
) error {
	plan, err := planLifecycleAdmission(
		subscriberAccountID, aliasDb, physAccountID, publisherAccountID,
		databaseName, snapshotScope, databaseMode, publicationName, tableName)
	if err != nil {
		return err
	}
	admission, err := beginLifecycleAdmission(ctx, ses, bh)
	if err != nil {
		return err
	}
	defer admission.free()
	lockProc := admission.lockProcess()
	systemCtx := context.WithValue(defines.AttachAccountId(ctx, catalog.System_Account), defines.LockWriterFairKey{}, false)
	lockKey := admission.lockKey
	if plan.aliasDb != "" {
		// A (finding 12): the subscriber database key serializes the
		// admission against a drop-and-recreate of N, whose writers take
		// the same mo_database row inside their statement transaction.
		aliasKey, err := cloneCatalogLockBatch(lockProc, plan.aliasAccount, plan.aliasDb)
		if err != nil {
			return err
		}
		if err = lockKey(catalog.MO_DATABASE, ctx, aliasKey, lock.LockMode_Shared, plan.aliasAccount); err != nil {
			return err
		}
	}
	databaseKey, err := cloneCatalogLockBatch(lockProc, plan.physAccount, plan.databaseName)
	if err != nil {
		return err
	}
	if err = lockKey(catalog.MO_DATABASE, ctx, databaseKey, plan.databaseMode, plan.physAccount); err != nil {
		return err
	}
	if plan.tableName != "" {
		tableKey, err := cloneCatalogLockBatch(lockProc, plan.physAccount, plan.databaseName, plan.tableName)
		if err != nil {
			return err
		}
		if err = lockKey(catalog.MO_TABLES, ctx, tableKey, lock.LockMode_Shared, plan.physAccount); err != nil {
			return err
		}
	}
	if plan.quotaScope != "" {
		// Target waits precede the shared quota key. A blocked Snapshot on A
		// must not queue an otherwise independent Snapshot on B behind Q.
		// Querying first initializes a missing quota row.
		if _, err = queryQuota(ctx, ses, bh, plan.physAccount, featureCodeSnapshot, plan.quotaScope); err != nil {
			return err
		}
		if _, err = lockFeatureQuota(ctx, ses, bh, plan.physAccount, featureCodeSnapshot, plan.quotaScope); err != nil {
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
	if plan.publicationName != "" {
		// Publication writers lock D before updating P. Follow that order
		// so a database snapshot cannot invert their two-row dependency.
		publisher := vector.NewVec(types.T_int32.ToType())
		name := vector.NewVec(types.T_varchar.ToType())
		defer publisher.Free(lockProc.Mp())
		defer name.Free(lockProc.Mp())
		if err = vector.AppendFixed(publisher, int32(plan.publisher), false, lockProc.Mp()); err != nil {
			return err
		}
		if err = vector.AppendBytes(name, []byte(plan.publicationName), false, lockProc.Mp()); err != nil {
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

// lifecycleAdmissionLockEventHook mirrors accountLifecycleLockEventHook on the
// admission body: in-process integration tests install it to observe the real
// lockservice acquisitions and to pace them deterministically against other
// transactions. Every physical lockKey attempt fires twice: with false
// immediately before acquisition and with true after it succeeds. The label is
// "<relation>#<lock-owner group>#<mode>". Production never sets the hook.
var lifecycleAdmissionLockEventHook atomic.Pointer[func(string, bool)]

// SetLifecycleAdmissionLockEventHookForTest installs a process-local observer
// for admission lock attempts and returns a restore function.
func SetLifecycleAdmissionLockEventHookForTest(hook func(string, bool)) func() {
	previous := lifecycleAdmissionLockEventHook.Load()
	if hook == nil {
		lifecycleAdmissionLockEventHook.Store(nil)
	} else {
		lifecycleAdmissionLockEventHook.Store(&hook)
	}
	return func() { lifecycleAdmissionLockEventHook.Store(previous) }
}

// fireLifecycleAdmissionLockEvent reports one physical acquisition boundary to
// the installed hook. Holding an earlier grant while the hook runs is exactly
// the blocked-window the integration phases coordinate on.
func fireLifecycleAdmissionLockEvent(relationName string, group uint32, mode lock.LockMode, acquired bool) {
	if hook := lifecycleAdmissionLockEventHook.Load(); hook != nil {
		(*hook)(fmt.Sprintf("%s#%d#%s", relationName, group, mode.String()), acquired)
	}
}

func beginLifecycleRCAdmission(ctx context.Context, ses *Session, bh BackgroundExec, gateMode lock.LockMode) (_ *lifecycleRCAdmission, err error) {
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
	if err = a.lockKey(catalog.MO_FEATURE_REGISTRY, systemCtx, key, gateMode, catalog.System_Account); err != nil {
		return nil, err
	}
	ready = true
	return a, nil
}

func (a *lifecycleRCAdmission) lockKey(relationName string, ctx context.Context, keys *batch.Batch, mode lock.LockMode, group uint32) error {
	defer keys.Vecs[0].Free(a.proc.Mp())
	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	relationCtx := systemCtx
	if relationName == catalog.MO_SNAPSHOTS {
		// Each account owns a separate mo_snapshots relation. Resolve the same
		// relation that the subsequent named-snapshot SELECT FOR UPDATE reads.
		relationCtx = ctx
	}
	eng := a.proc.GetSessionInfo().StorageEngine
	db, err := eng.Database(relationCtx, catalog.MO_CATALOG, a.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	relation, err := db.Relation(relationCtx, relationName, nil)
	if err != nil {
		return err
	}
	fireLifecycleAdmissionLockEvent(relationName, group, mode, false)
	err = withCloneLockContext(a.proc, ctx, func() error {
		_, lockErr := lockop.LockRowsForAdmissionWithContext(ctx, eng, a.proc,
			relation.GetTableID(relationCtx), keys, 0, *keys.Vecs[0].GetType(), mode, group)
		return lockErr
	})
	if err == nil {
		fireLifecycleAdmissionLockEvent(relationName, group, mode, true)
	}
	return err
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
