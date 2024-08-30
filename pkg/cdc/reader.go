package cdc

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/tools"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ Reader = new(tableReader)

type tableReader struct {
	cnTxnClient  client.TxnClient
	cnEngine     engine.Engine
	mp           *mpool.MPool
	packerPool   *fileservice.Pool[*types.Packer]
	info         *DbTableInfo
	interCh      chan tools.Pair[*TableCtx, *DecoderOutput]
	wMarkUpdater *WatermarkUpdater
	tick         *time.Ticker
}

func NewTableReader(
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
	mp *mpool.MPool,
	packerPool *fileservice.Pool[*types.Packer],
	info *DbTableInfo,
	interCh chan tools.Pair[*TableCtx, *DecoderOutput],
	wMarkUpdater *WatermarkUpdater,
) Reader {
	ret := &tableReader{
		cnTxnClient:  cnTxnClient,
		cnEngine:     cnEngine,
		mp:           mp,
		packerPool:   packerPool,
		info:         info,
		interCh:      interCh,
		wMarkUpdater: wMarkUpdater,
		tick:         time.NewTicker(time.Second * 1), //test interval
	}
	return ret
}

func (reader *tableReader) tableCtx() *TableCtx {
	return &TableCtx{
		db:      reader.info.DbName,
		dbId:    reader.info.DbId,
		table:   reader.info.TblName,
		tableId: reader.info.TblId,
	}
}

func (reader *tableReader) Close() {

}

func (reader *tableReader) Run(
	ctx context.Context,
	ar *ActiveRoutine) {
	for {
		select {
		case <-ar.Pause:
			return
		case <-ar.Cancel:
			return
		case <-reader.tick.C:
		}

		err := reader.readTable(
			ctx,
			ar,
		)
		if err != nil {
			logutil.Errorf("reader %v failed err:%v", reader.info, err)
			break
		}
	}
}

func (reader *tableReader) readTable(
	ctx context.Context,
	ar *ActiveRoutine) (err error) {

	var txnOp client.TxnOperator
	//step1 : create an txnop
	nowTs := reader.cnEngine.LatestLogtailAppliedTime()
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		"readMultipleTables",
		0)

	txnOp, err = reader.cnTxnClient.New(
		ctx,
		nowTs,
		createByOpt)
	if err != nil {
		return err
	}
	defer func() {
		//same timeout value as it in frontend
		ctx2, cancel := context.WithTimeout(ctx, reader.cnEngine.Hints().CommitOrRollbackTimeout)
		defer cancel()
		if err != nil {
			_ = txnOp.Rollback(ctx2)
		} else {
			_ = txnOp.Commit(ctx2)
		}
	}()
	err = reader.cnEngine.New(ctx, txnOp)
	if err != nil {
		return err
	}

	var packer *types.Packer
	put := reader.packerPool.Get(&packer)
	defer put.Put()

	//step2 : read table
	err = reader.readTableWithTxn(
		ctx,
		txnOp,
		packer,
		ar)
	if err != nil {
		return
	}

	return
}

func (reader *tableReader) readTableWithTxn(
	ctx context.Context,
	txnOp client.TxnOperator,
	packer *types.Packer,
	ar *ActiveRoutine) (err error) {
	var rel engine.Relation
	var changes engine.ChangesHandle
	//step1 : get relation
	_, _, rel, err = reader.cnEngine.GetRelationById(ctx, txnOp, reader.info.TblId)
	if err != nil {
		return
	}

	tableDef := rel.CopyTableDef(ctx)
	tableCtx := reader.tableCtx()
	tableCtx.tblDef = tableDef

	//step2 : define time range
	//	from = last wmark
	//  to = txn operator snapshot ts
	wMark := reader.wMarkUpdater.GetTableWatermark(reader.info.TblId)
	fromTs := types.TimestampToTS(wMark)
	toTs := types.TimestampToTS(txnOp.SnapshotTS())
	fmt.Fprintln(os.Stderr, reader.info, "from", fromTs.ToString(), "to", toTs.ToString())
	changes, err = rel.CollectChanges(fromTs, toTs)
	if err != nil {
		return
	}
	defer changes.Close()

	//step3: pull data
	var insertData, deleteData *batch.Batch
	var insertAtmBatch, deleteAtmBatch *AtomicBatch

	allocateAtomicBatchIfNeed := func(atmBatch *AtomicBatch) *AtomicBatch {
		if atmBatch == nil {
			atmBatch = NewAtomicBatch(
				reader.mp,
				fromTs, toTs,
			)
		}
		return atmBatch
	}

	var curHint engine.Hint
	for {
		select {
		case <-ar.Pause:
			return
		case <-ar.Cancel:
			return
		default:
		}

		insertData, deleteData, curHint, err = changes.Next()
		if err != nil {
			return
		}
		//FIXME: define the rule with changes handle
		tsColIdx, compositedPkColIdx := 0, 3
		//both nil denote no more data
		if insertData == nil && deleteData == nil {
			//only has checkpoint
			reader.interCh <- tools.NewPair(
				tableCtx,
				&DecoderOutput{
					noMoreData: true,
					toTs:       toTs,
				})

			break
		}

		switch curHint {
		case engine.Invalid:
		case engine.Checkpoint:
			// transform into insert instantly
			reader.interCh <- tools.NewPair(
				tableCtx,
				&DecoderOutput{
					outputTyp:     OutputTypeCheckpoint,
					checkpointBat: insertData,
				})
		case engine.Tail_wip:
			insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
			deleteAtmBatch = allocateAtomicBatchIfNeed(deleteAtmBatch)
			insertAtmBatch.Append(packer, insertData, tsColIdx, compositedPkColIdx)
			deleteAtmBatch.Append(packer, deleteData, tsColIdx, compositedPkColIdx)
		case engine.Tail_done:
			insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
			deleteAtmBatch = allocateAtomicBatchIfNeed(deleteAtmBatch)
			insertAtmBatch.Append(packer, insertData, tsColIdx, compositedPkColIdx)
			deleteAtmBatch.Append(packer, deleteData, tsColIdx, compositedPkColIdx)
			reader.interCh <- tools.NewPair(
				tableCtx,
				&DecoderOutput{
					outputTyp:      OutputTypeTailDone,
					insertAtmBatch: insertAtmBatch,
					deleteAtmBatch: deleteAtmBatch,
				},
			)
			insertAtmBatch = nil
			deleteAtmBatch = nil
		}
	}

	//FIXME: it is engine's bug
	//Tail_wip does not finished with Tail_done
	if insertAtmBatch != nil || deleteAtmBatch != nil {

		reader.interCh <- tools.NewPair(
			tableCtx,
			&DecoderOutput{
				outputTyp:      OutputTypeUnfinishedTailWIP,
				insertAtmBatch: insertAtmBatch,
				deleteAtmBatch: deleteAtmBatch,
			},
		)
	}

	return
}
