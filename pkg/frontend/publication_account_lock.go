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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Run the read on the same background transaction as its physical lock. Explicit
// grants must lock only the immutable IDs selected by the caller, even if the
// optimizer estimates that a catalog scan would justify locking the whole table.
func execWithExplicitPublicationAccountLocks(ctx context.Context, bh BackgroundExec, sql string, ids []int32) error {
	exec, ok := bh.(catalogLockBackgroundExec)
	if !ok {
		return moerr.NewInternalError(ctx, "publication account locking requires a native background executor")
	}
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
	return exec.execWithProcessHook(ctx, sql, func(ctx context.Context, proc *process.Process) error {
		if proc == nil || proc.GetSessionInfo() == nil || proc.GetSessionInfo().StorageEngine == nil {
			return moerr.NewInternalError(ctx, "publication account locking requires an engine process")
		}
		txn := proc.GetTxnOperator()
		if txn == nil || !txn.Txn().IsPessimistic() {
			return moerr.NewNotSupported(ctx, "publication account locking requires a pessimistic transaction")
		}
		eng := proc.GetSessionInfo().StorageEngine
		db, err := eng.Database(ctx, catalog.MO_CATALOG, txn)
		if err != nil {
			return err
		}
		rel, err := db.Relation(ctx, catalog.MOAccountTable, nil)
		if err != nil {
			return err
		}
		tableID := rel.GetTableID(ctx)
		pkType := types.T_int32.ToType()
		previous := proc.Ctx
		proc.Ctx = lockop.WithExactMutationRows(previous)
		defer func() { proc.Ctx = previous }()
		keys := batch.NewWithSize(1)
		keys.Vecs[0] = vector.NewVec(pkType)
		defer keys.Clean(proc.Mp())
		for chunk := range slices.Chunk(ids, int(proc.GetLockService().GetConfig().MaxLockRowCount)) {
			keys.Vecs[0].ResetWithSameType()
			if err := vector.AppendFixedList(keys.Vecs[0], chunk, nil, proc.Mp()); err != nil {
				return err
			}
			keys.SetRowCount(len(chunk))
			if err := lockop.LockRows(eng, proc, rel, tableID, keys, 0, pkType, lock.LockMode_Shared, lock.Sharding_None, catalog.System_Account); err != nil {
				return err
			}
		}
		return nil
	})
}
