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

package disttae

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"go.uber.org/zap"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type UT_ForceTransCheck struct{}

const (
	transferTxnLastThreshold = time.Second * 5
)

func skipTransfer(
	ctx context.Context,
	txn *Transaction) bool {

	if ctx.Value(UT_ForceTransCheck{}) != nil {
		return false
	}

	if time.Since(txn.start) < transferTxnLastThreshold {
		return true
	}

	if !txn.op.Txn().IsRCIsolation() {
		return true
	}

	return false
}

func transferTombstones(
	ctx context.Context,
	txn *Transaction,
	start, end types.TS,
) (err error) {

	if skipTransfer(ctx, txn) {
		return nil
	}

	if err = txn.op.UpdateSnapshot(
		ctx, timestamp.Timestamp{}); err != nil {
		return err
	}
	end = types.TimestampToTS(txn.op.SnapshotTS())

	if err = transferInmemTombstones(ctx, txn, start, end); err != nil {
		return err
	}

	return transferTombstoneObjects(ctx, txn, start, end)
}

func transferInmemTombstones(
	ctx context.Context,
	txn *Transaction,
	start, end types.TS,
) (err error) {

	return txn.forEachTableHasDeletesLocked(
		false,
		func(tbl *txnTable) error {
			//ctx = tbl.proc.Load().Ctx
			state, err := tbl.getPartitionState(ctx)
			if err != nil {
				return err
			}
			//var endTs timestamp.Timestamp
			//if commit {
			//	endTs = latestTs
			//} else {
			//	endTs = tbl.db.op.SnapshotTS()
			//}
			deleteObjs, createObjs := state.GetChangedObjsBetween(start, end)

			trace.GetService(txn.proc.GetService()).ApplyFlush(
				tbl.db.op.Txn().ID,
				tbl.tableId,
				start.ToTimestamp(),
				tbl.db.op.SnapshotTS(),
				len(deleteObjs))

			if len(deleteObjs) > 0 {
				if err := TransferTombstones(
					ctx,
					tbl,
					state,
					deleteObjs,
					createObjs,
					txn.proc.Mp(),
					txn.engine.fs,
				); err != nil {
					return err
				}
			}
			return nil
		})
}

func transferTombstoneObjects(
	ctx context.Context,
	txn *Transaction,
	start, end types.TS,
) (err error) {
	//
	//var start types.TS
	//if txn.statementID == 1 {
	//	start = types.TimestampToTS(txn.timestamps[0])
	//} else {
	//	//statementID > 1
	//	start = types.TimestampToTS(txn.timestamps[txn.statementID-2])
	//}
	//
	//end := types.TimestampToTS(txn.op.SnapshotTS())

	var flow *TransferFlow
	return txn.forEachTableHasDeletesLocked(
		true,
		func(tbl *txnTable) error {
			now := time.Now()
			if flow, err = ConstructCNTombstoneObjectsTransferFlow(
				ctx, start, end,
				tbl, txn, txn.proc.Mp(), txn.proc.GetFileService()); err != nil {
				return err
			} else if flow == nil {
				return nil
			}

			defer func() {
				err = flow.Close()
			}()

			if err = flow.Process(ctx); err != nil {
				return err
			}

			statsList, tail := flow.GetResult()
			if len(tail) > 0 {
				logutil.Fatal("tombstone sinker tail size is not zero",
					zap.Int("tail", len(tail)))
			}

			obj := make([]string, 0, len(statsList))
			for i := range statsList {
				fileName := statsList[i].ObjectLocation().String()
				obj = append(obj, statsList[i].String())
				bat := batch.New([]string{catalog.ObjectMeta_ObjectStats})
				bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
				if err = vector.AppendBytes(
					bat.GetVector(0), statsList[i].Marshal(), false, tbl.proc.Load().GetMPool()); err != nil {
					return err
				}

				bat.SetRowCount(bat.Vecs[0].Length())

				if err = txn.WriteFile(
					DELETE,
					tbl.accountId, tbl.db.databaseId, tbl.tableId,
					tbl.db.databaseName, tbl.tableName, fileName,
					bat, txn.tnStores[0],
				); err != nil {
					return err
				}
			}

			fmt.Println("transfer obj", len(obj))

			logutil.Info("CN-TRANSFER-TOMBSTONE-OBJ",
				zap.String("txn-id", txn.op.Txn().DebugString()),
				zap.String("table",
					fmt.Sprintf("%s(%d)-%s(%d)",
						tbl.db.databaseName, tbl.db.databaseId, tbl.tableName, tbl.tableId)),
				zap.Duration("time-spent", time.Since(now)),
				zap.Int("transferred-row-cnt", flow.transferred),
				zap.String("new-files", strings.Join(obj, "; ")))

			return nil
		})
}
