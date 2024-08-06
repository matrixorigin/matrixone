package disttae

import (
	"context"
	"encoding/hex"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
)

func ConstructPrintablePK(buf []byte, tableDef *plan.TableDef) string {
	if tableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		tuple, _, _, _ := types.DecodeTuple(buf)
		return tuple.ErrString(nil)
	} else {
		return hex.EncodeToString(buf)
	}
}

func ConstructInExpr(vec *vector.Vector) *plan.Expr {
	data, _ := vec.MarshalBinary()
	return &plan.Expr{
		Typ: plan2.MakePlan2Type(vec.GetType()),
		Expr: &plan.Expr_Vec{
			Vec: &plan.LiteralVec{
				Len:  int32(vec.Length()),
				Data: data,
			},
		},
	}
}

func TransferTombstones(
	ctx context.Context,
	table *txnTable,
	state *logtailreplay.PartitionState,
	deletedObjects, createdObjects map[objectio.ObjectNameShort]struct{},
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	if len(deletedObjects) == 0 || len(createdObjects) == 0 {
		return
	}
	// TODO:
	var objectList []objectio.ObjectStats
	for name := range createdObjects {
		if obj, ok := state.GetObject(name); ok {
			objectList = append(objectList, obj.ObjectStats)
		}
	}

	txnWrites := table.getTxn().writes

	var (
		transferIntents *vector.Vector
		targetRowids    *vector.Vector
		searchPKColumn  *vector.Vector
		searchEntryPos  *vector.Vector
		searchBatPos    *vector.Vector
		readPKColumn    *vector.Vector
	)

	defer func() {
		if transferIntents != nil {
			transferIntents.Free(mp)
		}
		if targetRowids != nil {
			targetRowids.Free(mp)
		}
		if searchPKColumn != nil {
			searchPKColumn.Free(mp)
		}
		if searchEntryPos != nil {
			searchEntryPos.Free(mp)
		}
		if searchBatPos != nil {
			searchBatPos.Free(mp)
		}
		if readPKColumn != nil {
			readPKColumn.Free(mp)
		}
	}()

	// loop the transaction workspace to transfer all tombstones
	for i, entry := range txnWrites {
		// skip all entries not table-realted
		// skip all non-delete entries
		if entry.tableId != table.tableId ||
			entry.typ != DELETE ||
			entry.fileName != "" {
			continue
		}

		// column 0 is rowid, column 1 is pk
		// fetch rowid and pk column data
		rowids := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
		pkColumn := entry.bat.GetVector(1)

		for j, rowid := range rowids {
			blockId := rowid.BorrowBlockID()
			// if the block of the rowid is not in the deleted objects, skip transfer
			if _, deleted := deletedObjects[*objectio.ShortName(blockId)]; !deleted {
				continue
			}
			if transferIntents == nil {
				transferIntents = vector.NewVec(types.T_Rowid.ToType())
				targetRowids = vector.NewVec(types.T_Rowid.ToType())
				searchPKColumn = vector.NewVec(types.T_blob.ToType())
				searchEntryPos = vector.NewVec(types.T_int32.ToType())
				searchBatPos = vector.NewVec(types.T_int32.ToType())
				readPKColumn = vector.NewVec(*pkColumn.GetType())
			}
			if err = vector.AppendFixed[types.Rowid](transferIntents, rowid, false, mp); err != nil {
				return
			}
			if err = vector.AppendFixed[int32](searchEntryPos, int32(i), false, mp); err != nil {
				return
			}
			if err = vector.AppendFixed[int32](searchBatPos, int32(j), false, mp); err != nil {
				return
			}
			if err = searchPKColumn.UnionOne(pkColumn, int64(j), mp); err != nil {
				return
			}

			if transferIntents.Length() >= 8192 {
				if err = batchTransferToTombstones(
					ctx,
					table,
					txnWrites,
					objectList,
					transferIntents,
					targetRowids,
					searchPKColumn,
					searchEntryPos,
					searchBatPos,
					readPKColumn,
					mp,
					fs,
				); err != nil {
					return
				}
			}
		}
	}

	if transferIntents != nil && transferIntents.Length() > 0 {
		if err = batchTransferToTombstones(
			ctx,
			table,
			txnWrites,
			objectList,
			transferIntents,
			targetRowids,
			searchPKColumn,
			searchEntryPos,
			searchBatPos,
			readPKColumn,
			mp,
			fs,
		); err != nil {
			return
		}
	}
	return nil
}

func batchTransferToTombstones(
	ctx context.Context,
	table *txnTable,
	txnWrites []Entry,
	objectList []objectio.ObjectStats,
	transferIntents *vector.Vector,
	targetRowids *vector.Vector,
	searchPKColumn *vector.Vector,
	searchEntryPos *vector.Vector,
	searchBatPos *vector.Vector,
	readPKColumn *vector.Vector,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	if err = targetRowids.PreExtend(transferIntents.Length(), mp); err != nil {
		return
	}
	if err = doTransferRowids(
		ctx,
		table,
		objectList,
		transferIntents,
		targetRowids,
		readPKColumn,
		mp,
		fs,
	); err != nil {
		return
	}

	if err = mergesort.SortColumnsByIndex(
		[]*vector.Vector{searchPKColumn, searchEntryPos, searchBatPos},
		0,
		mp,
	); err != nil {
		return
	}

	if err = mergesort.SortColumnsByIndex(
		[]*vector.Vector{readPKColumn, targetRowids},
		0,
		mp,
	); err != nil {
		return
	}

	entryPositions := vector.MustFixedCol[int32](searchEntryPos)
	batPositions := vector.MustFixedCol[int32](searchBatPos)
	rowids := vector.MustFixedCol[types.Rowid](targetRowids)
	for pos, endPos := 0, searchPKColumn.Length(); pos < endPos; pos++ {
		entry := txnWrites[entryPositions[pos]]
		if err = vector.SetFixedAt[types.Rowid](
			entry.bat.GetVector(0),
			int(batPositions[pos]),
			rowids[pos],
		); err != nil {
			return
		}
	}

	searchPKColumn.Reset(*searchPKColumn.GetType())
	searchEntryPos.Reset(*searchEntryPos.GetType())
	searchBatPos.Reset(*searchBatPos.GetType())
	targetRowids.Reset(*targetRowids.GetType())
	transferIntents.Reset(*transferIntents.GetType())
	readPKColumn.Reset(*readPKColumn.GetType())
	return
}

// doTransferRowids transfers rowids from transferIntents to targetRowids
func doTransferRowids(
	ctx context.Context,
	table *txnTable,
	objectList []objectio.ObjectStats,
	transferIntents, targetRowids, readPKColumn *vector.Vector,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	transferIntents.InplaceSort()
	expr := ConstructInExpr(transferIntents)

	var blockList objectio.BlockInfoSlice
	if _, err = TryFastFilterBlocks(
		ctx,
		table,
		table.db.op.SnapshotTS(),
		table.GetTableDef(ctx),
		[]*plan.Expr{expr},
		nil,
		objectList,
		nil,
		&blockList,
		fs,
		table.proc.Load(),
	); err != nil {
		return
	}
	relData := NewEmptyBlockListRelationData()
	for i, end := 0, blockList.Len(); i < end; i++ {
		relData.AppendBlockInfo(*blockList.Get(i))
	}

	readers, err := table.BuildReaders(
		ctx, table.proc.Load(), expr, relData, 1, 0, false,
	)
	if err != nil {
		return
	}
	defer func() {
		readers[0].Close()
	}()

	attrs := []string{
		table.GetTableDef(ctx).Pkey.PkeyColName,
		catalog.Row_ID,
	}
	for {
		var bat *batch.Batch
		if bat, err = readers[0].Read(
			ctx,
			attrs,
			expr,
			mp,
			nil,
		); err != nil {
			return
		}
		if bat == nil {
			break
		}
		defer bat.Clean(mp)
		if err = vector.GetUnionAllFunction(
			*readPKColumn.GetType(), mp,
		)(
			readPKColumn, bat.GetVector(0),
		); err != nil {
			return
		}

		if err = vector.GetUnionAllFunction(
			*targetRowids.GetType(), mp,
		)(
			targetRowids, bat.GetVector(1),
		); err != nil {
			return
		}
	}

	if targetRowids.Length() != transferIntents.Length() {
		err = moerr.NewInternalErrorNoCtx(
			"transfer rowids failed, length mismatch, expect %d, got %d",
			transferIntents.Length(),
			targetRowids.Length(),
		)
	}

	return
}
