package cdc

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/tools"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

func NewPartitioner(
	q disttae.Queue[tools.Pair[*disttae.TableCtx, *disttae.DecoderInput]],
	outputChs map[uint64]chan tools.Pair[*disttae.TableCtx, *disttae.DecoderInput],
) Partitioner {
	return &tableIdPartitioner{
		q:         q,
		outputChs: outputChs,
	}
}

var _ Partitioner = new(tableIdPartitioner)

type tableIdPartitioner struct {
	q         disttae.Queue[tools.Pair[*disttae.TableCtx, *disttae.DecoderInput]]
	outputChs map[uint64]chan tools.Pair[*disttae.TableCtx, *disttae.DecoderInput]
}

func (p tableIdPartitioner) Partition(entry tools.Pair[*disttae.TableCtx, *disttae.DecoderInput]) {
	tableCtx := entry.Key

	if ch, ok := p.outputChs[tableCtx.TableId()]; !ok {
		_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Partitioner: no inputCh found for table{%v}\n", tableCtx.TableId())
	} else {
		ch <- entry
	}
}

func (p tableIdPartitioner) Run(_ context.Context, ar *ActiveRoutine) {
	_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Partitioner: start\n")
	defer fmt.Fprintf(os.Stderr, "^^^^^ Partitioner: end\n")

	for {
		select {
		case <-ar.Pause:
			return

		case <-ar.Cancel:
			return

		default:
			if !p.q.Empty() {
				entry := p.q.Front()
				tableCtx := entry.Key
				decoderInput := entry.Value
				p.q.Pop()

				//TODO:process heartbeat.to decoder? to sinker?
				if decoderInput.IsHeartbeat() {
					//_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Partitioner:{%s} heartbeat\n", decoderInput.TS().DebugString())
					continue
				} else if decoderInput.IsDDL() {
					//TODO:action on ddl
					continue
				}

				_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Partitioner: {%s} [%v(%v)].[%v(%v)]\n",
					decoderInput.TS().DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())

				p.Partition(entry)

				_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Partitioner: {%s} [%v(%v)].[%v(%v)], entry pushed\n",
					decoderInput.TS().DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func PollTablesFunc(
	ctx context.Context,
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
	infos []*DbTableInfo,
	interChs map[uint64]chan tools.Pair[*disttae.TableCtx, *DecoderOutput],
	wMarkUpdater *WatermarkUpdater,
	ar *ActiveRoutine) {
	for {
		select {
		case <-ar.Pause:
			return
		case <-ar.Cancel:
			return
		default:
		}

		fun := func() (err error) {
			var txnOp client.TxnOperator
			//step1 : create an txnop
			ts := cnEngine.LatestLogtailAppliedTime()
			//typTs := types.TimestampToTS(ts)
			createByOpt := client.WithTxnCreateBy(
				0,
				"",
				"PollTablesFunc",
				0)

			txnOp, err = cnTxnClient.New(
				ctx,
				ts,
				createByOpt)
			if err != nil {
				return err
			}
			defer func() {
				//same timeout value as it in frontend
				ctx2, cancel := context.WithTimeout(ctx, cnEngine.Hints().CommitOrRollbackTimeout)
				defer cancel()
				if err != nil {
					_ = txnOp.Rollback(ctx2)
				} else {
					_ = txnOp.Commit(ctx2)
				}
			}()
			err = cnEngine.New(ctx, txnOp)
			if err != nil {
				return err
			}

			//step2 : polling on every table
			for _, info := range infos {
				pollTable(ctx, txnOp, cnEngine, info, interChs[info.TblId], wMarkUpdater, ar)
			}
			return
		}
		err := fun()
		if err != nil {
			//TODO:
		}
	}
}

func pollTable(
	ctx context.Context,
	txnOp client.TxnOperator,
	cnEngine engine.Engine,
	tblInfo *DbTableInfo,
	interCh chan tools.Pair[*disttae.TableCtx, *DecoderOutput],
	wMarkUpdater *WatermarkUpdater,
	ar *ActiveRoutine) (err error) {
	var rel engine.Relation
	var changes engine.ChangesHandle
	//step1 : get relation
	_, _, rel, err = cnEngine.GetRelationById(ctx, txnOp, tblInfo.TblId)
	if err != nil {
		return
	}

	//step2 : define time range
	//	from = last wmark
	//  to = txn operator snapshot ts
	wMark := wMarkUpdater.GetTableWatermark(tblInfo.TblId)
	typTs := types.TimestampToTS(wMark)
	toTs := types.TimestampToTS(txnOp.SnapshotTS())
	changes, err = rel.CollectChanges(typTs, toTs)
	if err != nil {
		return
	}
	defer changes.Close()

	//step3: pull data
	var insertData, deleteData *batch.Batch
	var hint engine.Hint
	for {
		select {
		case <-ar.Pause:
			return
		case <-ar.Cancel:
			return
		default:
		}

		insertData, deleteData, hint, err = changes.Next()
		if err != nil {
			return
		}
		//both nil denote no more data
		if insertData == nil && deleteData == nil {
			break
		}

		switch hint {
		case engine.Checkpoint:
			//TODO: transform into insert instantly
		case engine.Tail_wip:
			//TODO: cache this batch pair
		case engine.Tail_done:
			//TODO: cache this batch pair and transforming batches until now
		}
	}

	//step4: transforming data
	return
}
