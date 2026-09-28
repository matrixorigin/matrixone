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

package compile

import (
	"context"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
)

type lifecycleDatabaseName struct {
	accountID uint32
	name      string
	mode      lock.LockMode
}

// admitLifecycleRC takes the stable registry identity, the SNAPSHOT key, and
// complete database-name domain before the caller reads mutable catalog facts.
// All locks remain owned by the caller's transaction until its terminal path.
func (c *Compile) admitLifecycleRC(names []lifecycleDatabaseName) error {
	ctx := c.proc.Ctx
	txnOp := c.proc.GetTxnOperator()
	if txnOp == nil || !txnOp.Txn().IsPessimistic() || !txnOp.Txn().IsRCIsolation() {
		return moerr.NewInternalError(ctx, "lifecycle admission requires pessimistic RC")
	}
	for _, name := range names {
		if name.name == "" || (name.mode != lock.LockMode_Shared && name.mode != lock.LockMode_Exclusive) {
			return moerr.NewInternalError(ctx, "invalid lifecycle database lock")
		}
	}
	sort.Slice(names, func(i, j int) bool {
		if names[i].accountID != names[j].accountID {
			return names[i].accountID < names[j].accountID
		}
		if names[i].name != names[j].name {
			return names[i].name < names[j].name
		}
		return names[i].mode == lock.LockMode_Exclusive && names[j].mode != lock.LockMode_Exclusive
	})
	systemCtx := context.WithValue(
		defines.AttachAccountId(ctx, catalog.System_Account),
		defines.LockWriterFairKey{}, false,
	)
	if err := c.lockLifecycleIdentityRC(); err != nil {
		return err
	}
	systemDB, err := c.e.Database(systemCtx, catalog.MO_CATALOG, txnOp)
	if err != nil {
		return err
	}
	registry, err := systemDB.Relation(systemCtx, catalog.MO_FEATURE_REGISTRY, nil)
	if err != nil {
		return err
	}
	registryKeys := batch.NewWithSize(1)
	registryKeys.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	if err = vector.AppendBytes(registryKeys.Vecs[0],
		[]byte(catalog.SnapshotLifecycleFeatureCode), false, c.proc.Mp()); err != nil {
		registryKeys.Vecs[0].Free(c.proc.Mp())
		return err
	}
	_, err = lockop.LockRowsForAdmissionWithContext(systemCtx, c.e, c.proc,
		registry.GetTableID(systemCtx), registryKeys, 0, *registryKeys.Vecs[0].GetType(),
		lock.LockMode_Shared, catalog.System_Account)
	registryKeys.Vecs[0].Free(c.proc.Mp())
	if err != nil {
		return err
	}

	if len(names) > 0 {
		dbRel, err := systemDB.Relation(systemCtx, catalog.MO_DATABASE, nil)
		if err != nil {
			return err
		}
		for i, name := range names {
			if i > 0 && name.accountID == names[i-1].accountID && name.name == names[i-1].name {
				continue
			}
			keys, err := getLockBatch(c.proc, name.accountID, []string{name.name})
			if err != nil {
				return err
			}
			_, err = lockop.LockRowsForAdmissionWithContext(ctx, c.e, c.proc,
				dbRel.GetTableID(systemCtx), keys, 0, *keys.Vecs[0].GetType(),
				name.mode, name.accountID)
			keys.Vecs[0].Free(c.proc.Mp())
			if err != nil {
				return err
			}
		}
	}
	if err := c.advanceLifecycleAdmissionSnapshot(); err != nil {
		return err
	}
	// C excludes a concurrent replacement, but the transaction's earlier
	// snapshot can still resolve the retired registry after C was granted.
	currentDB, err := c.e.Database(systemCtx, catalog.MO_CATALOG, txnOp)
	if err != nil {
		return err
	}
	currentRegistry, err := currentDB.Relation(systemCtx, catalog.MO_FEATURE_REGISTRY, nil)
	if err != nil {
		return err
	}
	if currentRegistry.GetTableID(systemCtx) != registry.GetTableID(systemCtx) {
		return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
	}
	return c.verifyLifecycleRegistryRow()
}

// lockLifecycleIdentityRC pins the registry's physical relation before any
// caller resolves or locks its row. Broad owners also use this before G X.
func (c *Compile) lockLifecycleIdentityRC() error {
	ctx := context.WithValue(
		defines.AttachAccountId(c.proc.Ctx, catalog.System_Account),
		defines.LockWriterFairKey{}, false,
	)
	db, err := c.e.Database(ctx, catalog.MO_CATALOG, c.proc.GetTxnOperator())
	if err != nil {
		return err
	}
	relation, err := db.Relation(ctx, catalog.MO_TABLES, nil)
	if err != nil {
		return err
	}
	keys, err := getLockBatch(c.proc, catalog.System_Account,
		[]string{catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY})
	if err != nil {
		return err
	}
	defer keys.Vecs[0].Free(c.proc.Mp())
	_, err = lockop.LockRowsForAdmissionWithContext(ctx, c.e, c.proc,
		relation.GetTableID(ctx), keys, 0, *keys.Vecs[0].GetType(),
		lock.LockMode_Shared, catalog.System_Account)
	return err
}

func (c *Compile) advanceLifecycleAdmissionSnapshot() error {
	barrier, ok := getLogtailReadBarrier(c.e)
	if !ok {
		return moerr.NewInternalError(c.proc.Ctx, "lifecycle logtail barrier is unavailable")
	}
	frontier, err := barrier.AcquireLogtailReadBarrier(c.proc.Ctx)
	if err != nil {
		return err
	}
	workspace := c.proc.GetTxnOperator().GetWorkspace()
	if workspace == nil {
		return moerr.NewInternalError(c.proc.Ctx, "missing lifecycle transaction workspace")
	}
	return workspace.AdvanceSnapshot(c.proc.Ctx, frontier)
}

func (c *Compile) verifyLifecycleRegistryRow() error {
	res, err := c.runSqlWithResult(
		"select feature_code from mo_catalog.mo_feature_registry where feature_code = 'SNAPSHOT'",
		int32(catalog.System_Account),
	)
	if err != nil {
		return err
	}
	defer res.Close()
	count := 0
	res.ReadRows(func(rows int, _ []*vector.Vector) bool {
		count += rows
		return true
	})
	if count != 1 {
		return moerr.NewInternalError(c.proc.Ctx, "missing or duplicate SNAPSHOT lifecycle registry row")
	}
	return nil
}
