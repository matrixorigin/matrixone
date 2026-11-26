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
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type dataBranchForkKeyRef struct {
	tableID uint64
	account uint32
}

// A branch created from a transaction-local table has no committed snapshot
// of its fork image. Record its initial key membership in the clone
// transaction, before later statements can compact inherited rows away.
func captureDataBranchForkKeys(
	ctx context.Context, ses *Session, bh BackgroundExec, receipt *cloneReceipt,
	txn TxnOperator, destination *plan.TableDef,
) error {
	if receipt.snapshot != nil || destination.Pkey == nil || destination.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return nil
	}
	sourceCtx := defines.AttachAccountId(ctx, receipt.srcAccount)
	_, _, source, err := ses.proc.GetSessionInfo().StorageEngine.GetRelationById(sourceCtx, txn, receipt.srcTableID)
	if err != nil {
		return err
	}
	creation, ok := source.(engine.RelationCreatedInCurrentTxn)
	if !ok {
		return nil
	}
	uncommitted, err := creation.CreatedInCurrentTxn(sourceCtx)
	if err != nil || !uncommitted {
		return err
	}

	ownerCtx := context.WithValue(defines.AttachAccountId(ctx, receipt.toAccount), defines.BranchForkKeyAccess{}, true)
	pk := sqlquote.Ident(destination.Pkey.PkeyColName)
	if destination.Pkey.CompPkeyCol == nil {
		pk = "serial(" + pk + ")"
	}
	ledger := catalog.MO_CATALOG + "." + catalog.MO_BRANCH_FORK_KEYS
	bh.ClearExecResultSet()
	if err := bh.Exec(ownerCtx, fmt.Sprintf("insert into %s (table_id,key_bytes) select %d,%s from %s.%s",
		ledger, receipt.dstTableID, pk, sqlquote.Ident(receipt.dstDb), sqlquote.Ident(receipt.dstTbl))); err != nil {
		return err
	}
	return bh.Exec(ownerCtx, fmt.Sprintf("insert into %s (table_id,key_bytes) values (%d,null)", ledger, receipt.dstTableID))
}

// lookupDataBranchForkKeys returns indexes in the input key vector. The
// marker distinguishes an empty captured fork from a legacy fork.
func lookupDataBranchForkKeys(
	ctx context.Context, ses *Session, bh BackgroundExec, point dataBranchForkKeyRef,
	keys *vector.Vector, composite bool,
) (found []int64, recorded bool, err error) {
	if !composite {
		keys, err = function.RunFunctionDirectly(ses.proc, function.SerialFunctionEncodeID, []*vector.Vector{keys}, keys.Length())
		if err != nil {
			return nil, false, err
		}
		defer keys.Free(ses.proc.Mp())
	}
	ctx = context.WithValue(defines.AttachAccountId(ctx, point.account), defines.BranchForkKeyAccess{}, true)
	ledger := catalog.MO_CATALOG + "." + catalog.MO_BRANCH_FORK_KEYS
	prefix := fmt.Sprintf("select key_bytes from %s where table_id=%d and (key_bytes is null or key_bytes in (", ledger, point.tableID)
	for lo := 0; lo < keys.Length(); {
		hi := lo
		var query strings.Builder
		query.WriteString(prefix)
		for hi < keys.Length() {
			key := keys.GetBytesAt(hi)
			if hi > lo && (hi-lo >= dataBranchMetadataIDBatchSize || query.Len()+2*len(key)+4 > maxSqlBatchSize) {
				break
			}
			if hi != lo {
				query.WriteByte(',')
			}
			query.WriteString("x'")
			query.WriteString(hex.EncodeToString(key))
			query.WriteByte(39)
			hi++
		}
		query.WriteString("))")
		result, err := runSql(ctx, ses, bh, query.String(), nil, nil)
		if err != nil {
			return nil, false, err
		}
		present := make(map[string]struct{}, hi-lo)
		result.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				if cols[0].IsNull(uint64(i)) {
					recorded = true
				} else {
					present[string(cols[0].GetBytesAt(i))] = struct{}{}
				}
			}
			return true
		})
		result.Close()
		for i := lo; i < hi; i++ {
			if _, ok := present[string(keys.GetBytesAt(i))]; ok {
				found = append(found, int64(i))
			}
		}
		lo = hi
	}
	return found, recorded, nil
}
