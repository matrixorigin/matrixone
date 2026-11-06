// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// validateOptimisticWritesLocked bridges the conflict interval removed when RC
// advances SnapshotTS. Pessimistic RC protects this interval with row locks;
// optimistic RC must validate it before transfer can replace physical RowIDs.
// TN still validates the interval after SnapshotTS, closing the concurrent
// commit race. No dependency keys are retained outside the existing workspace.
func (txn *Transaction) validateOptimisticWritesLocked(ctx context.Context) error {
	meta := txn.op.Txn()
	if meta.Mode != pbtxn.TxnMode_Optimistic || !meta.IsRCIsolation() {
		return nil
	}
	end := txn.op.SnapshotTS()
	if txn.optimisticWriteSnapshot.IsEmpty() {
		txn.optimisticWriteSnapshot = end
	}
	if !txn.optimisticWriteSnapshot.Less(end) {
		return nil
	}
	from, to := types.TimestampToTS(txn.optimisticWriteSnapshot), types.TimestampToTS(end)
	// Resolve scalar identities first. Catalog lookup can reenter internal SQL,
	// so no workspace index or batch pointer may survive that unlocked call.
	tables := make(map[tombstoneTransferKey]*txnTable)
	names := make(map[tombstoneTransferKey][2]string)
	for _, e := range txn.writes {
		if (e.typ == INSERT || e.typ == DELETE) && e.bat != nil && e.bat.RowCount() > 0 {
			key := tombstoneTransferKey{e.accountId, e.databaseId, e.tableId}
			names[key] = [2]string{e.databaseName, physicalCatalogTableName(e.databaseId, e.tableId, e.tableName)}
		}
	}
	for key, name := range names {
		rel, err := func() (engine.Relation, error) {
			txn.Unlock()
			defer txn.Lock()
			return txn.getTable(ctx, key.accountId, name[0], name[1])
		}()
		if err != nil {
			return err
		}
		if delegate, ok := rel.(*txnTableDelegate); ok {
			tables[key] = delegate.origin
		} else {
			tables[key] = rel.(*txnTable)
		}
	}
	for _, e := range txn.writes {
		if (e.typ != INSERT && e.typ != DELETE) || e.bat == nil || e.bat.RowCount() == 0 {
			continue
		}
		tbl := tables[tombstoneTransferKey{e.accountId, e.databaseId, e.tableId}]
		if tbl == nil {
			return moerr.NewInternalErrorNoCtx("optimistic RC workspace changed during dependency lookup")
		}
		if err := validateOptimisticWriteEntry(ctx, tbl, e, from, to); err != nil {
			return err
		}

	}
	// On error keep the older fence: retry/COMMIT cannot erase the dependency.
	txn.optimisticWriteSnapshot = end
	return nil
}

func validateOptimisticWriteEntry(ctx context.Context, tbl *txnTable, e Entry, from, to types.TS) error {
	created, err := tbl.isCreatedInTxn(ctx)
	if err != nil {
		return err
	}
	if created {
		return nil
	}

	check := func(keys *vector.Vector) error {
		changed, err := tbl.primaryKeysMayBeChangedWithHistory(ctx, from, to, keys, true, true, true)
		if err != nil {
			return err
		}
		if changed {
			return moerr.NewTxnWWConflictNoCtx(e.tableId, "optimistic RC snapshot advance")
		}
		return nil
	}
	if e.fileName == "" {
		pos := -1
		if e.typ == DELETE {
			pos = 1
		} else {
			pk := tbl.GetTableDef(ctx).Pkey
			if pk != nil {
				for n, attr := range e.bat.Attrs {
					if attr == pk.PkeyColName {
						pos = n
						break
					}
				}
			}
		}
		if pos < 0 || pos >= len(e.bat.Vecs) {
			return moerr.NewInternalErrorNoCtx("missing optimistic RC write dependency")
		}
		if err := check(e.bat.Vecs[pos]); err != nil {
			return err
		}
		return nil
	}
	// Spilled writes carry ObjectStats rather than keys. Read only one PK
	// column/block at a time, including persisted tombstones' logical PK column.
	statsPos := 1
	seqnum := uint16(tbl.GetTableDef(ctx).Cols[tbl.primaryIdx].Seqnum)
	if e.typ == DELETE {
		statsPos, seqnum = 0, 1
	}
	for n := 0; n < e.bat.Vecs[statsPos].Length(); n++ {
		stats := objectio.ObjectStats(e.bat.Vecs[statsPos].GetBytesAt(n))
		var checkErr error
		objectio.ForeachBlkInObjStatsList(true, nil, func(blk objectio.BlockInfo, _ objectio.BlockObject) bool {
			cols, release, err := ioutil.LoadColumns2(ctx, []uint16{seqnum}, nil, tbl.eng.(*Engine).fs, blk.MetaLocation(), 0, false, nil)
			if err != nil {
				checkErr = err
				return false
			}
			defer release()
			checkErr = check(cols[0].GetDownstreamVector())
			return checkErr == nil
		}, stats)
		if checkErr != nil {
			return checkErr
		}
	}
	return nil
}
