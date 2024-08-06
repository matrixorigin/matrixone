package disttae

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"go.uber.org/zap"
)

func ConstructPrintablePK(buf []byte, tableDef *plan.TableDef) string {
	if tableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		tuple, _, _, _ := types.DecodeTuple(buf)
		return tuple.ErrString(nil)
	} else {
		return hex.EncodeToString(buf)
	}
}

func TransferTombstones(
	ctx context.Context,
	table *txnTable,
	state *logtailreplay.PartitionState,
	deletedObjects, createdObjects map[objectio.ObjectNameShort]struct{},
	fs *fileservice.FileService,
) (err error) {
	if len(deletedObjects) == 0 {
		return
	}
	var blockList []objectio.BlockInfo
	serviceName := table.proc.Load().GetService()

	relData := NewEmptyBlockListRelationData()
	relData.AppendBlockInfo(objectio.EmptyBlockInfo)

	var datasource engine.DataSource
	if datasource, err = table.buildLocalDataSource(
		ctx,
		0,
		relData,
		Policy_CheckCommittedS3Only,
	); err != nil {
		return
	}

	for name := range createdObjects {
		if object, ok := state.GetObject(name); ok {
			objectStats := object.ObjectStats
			ForeachBlkInObjStatsList(
				false,
				nil,
				func(blk objectio.BlockInfo, _ objectio.BlockObject) bool {
					// if object.HasDeltaLoc {
					// 	_, commitTs, ok := state.GetBockDeltaLoc(blk.BlockID)
					// 	if ok {
					// 		blk.CommitTs = commitTs
					// 	}
					// }
					blockList = append(blockList, blk)
					return true
				},
				objectStats,
			)
		}
	}

	txnWrites := table.getTxn().writes

	// loop the transaction workspace to transfer all tombstones
	for _, entry := range txnWrites {
		// skip all entries not table-realted
		// skip all non-delete entries
		if entry.tableId != table.tableId ||
			entry.typ != DELETE ||
			entry.fileName != "" {
			continue
		}

		var (
			transferedCnt, transferIntent int
		)

		// column 0 is rowid, column 1 is pk
		// fetch rowid and pk column data
		rowids := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
		pkColumn := entry.bat.GetVector(1)

		// transfer all old rowids to new rowids
		for i, rowid := range rowids {
			blockId := rowid.BorrowBlockID()
			// if the block of the rowid is not in the deleted objects, skip transfer
			if _, deleted := deletedObjects[*objectio.ShortName(blockId)]; !deleted {
				continue
			}
			transferIntent++
			// TODO: refactor readNewRowid later
			newId, ok, err := table.readNewRowid(pkColumn, i, blockList, datasource, nil)
			if err != nil {
				return err
			}
			if !ok {
				logutil.Error("TRANSFER-DELETE-ROWID-ERR",
					zap.String("rowid", rowids[i].ShortStringEx()),
					zap.String("pk", ConstructPrintablePK(pkColumn.GetBytesAt(i), table.GetTableDef(ctx))),
					zap.String("state", "not-found"),
				)
			}

			newBlockID := newId.BorrowBlockID()
			trace.GetService(serviceName).ApplyTransferRowID(
				table.db.op.Txn().ID,
				table.tableId,
				rowids[i][:],
				newId[:],
				blockId[:],
				newBlockID[:],
				pkColumn,
				i)
			rowids[i] = newId
			transferedCnt++
		}

		if transferIntent != transferedCnt {
			var idx int
			detail := stringifySlice(
				rowids,
				func(a any) string {
					rid := a.(types.Rowid)
					pk := ConstructPrintablePK(pkColumn.GetBytesAt(idx), table.GetTableDef(ctx))
					idx++
					return fmt.Sprintf("%s:%s", pk, rid.ShortStringEx())
				},
			)
			logutil.Error(
				"TRANSFER-DELETE-ERR",
				zap.String("note", entry.note),
				zap.Uint64("table-id", table.tableId),
				zap.String("table-name", table.tableName),
				zap.String(
					"block-list",
					stringifySlice(blockList, func(a any) string {
						info := a.(objectio.BlockInfo)
						return info.String()
					}),
				),
				zap.String("detail", detail),
			)
			return moerr.NewInternalErrorNoCtx(
				"%v-%v transfer deletes failed %v/%v in %v blks",
				table.tableId,
				table.tableName,
				transferedCnt,
				transferIntent,
				len(blockList),
			)
		}
	}
	return nil
}

func GetRowidByPKWithDeltaLoc(
	ctx context.Context,
	table *txnTable,
	pkColumn *vector.Vector,
	pkRowIdx int,
	blockList []objectio.BlockInfo,
	dataSource engine.DataSource,
) (newRowId types.Rowid, found bool, err error) {
	return
}
