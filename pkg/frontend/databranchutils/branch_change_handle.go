package databranchutils

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var _ engine.ChangesHandle = new(BranchChangeHandle)

type BranchChangeHandle struct {
	handle     engine.ChangesHandle
	filterData func(bat *batch.Batch) error
}

func (b *BranchChangeHandle) Next(
	ctx context.Context,
	mp *mpool.MPool,
) (
	data *batch.Batch,
	tombstone *batch.Batch,
	hint engine.ChangesHandle_Hint,
	err error,
) {
	if data, tombstone, hint, err = b.handle.Next(ctx, mp); err != nil {
		return
	}

	if data != nil && b.filterData != nil {
		if err = b.filterData(data); err != nil {
			return
		}
	}

	return data, tombstone, hint, nil
}

func (b *BranchChangeHandle) Close() error {
	return b.handle.Close()
}

var CollectChanges = func(
	ctx context.Context,
	eng engine.Engine,
	accountId uint32,
	snapshot timestamp.Timestamp,
	txnOp client.TxnOperator,
	rel engine.Relation,
	fromTs, toTs types.TS,
	mp *mpool.MPool,
	isClonedTable bool,
) (engine.ChangesHandle, error) {

	var (
		err error
	)

	handle := new(BranchChangeHandle)

	minTS := fromTs
	if isClonedTable {
		getCommitTSForClonedTable(ctx, eng, mp, txnOp, rel)
	}

	if handle.handle, err = rel.CollectChanges(
		ctx, minTS, toTs, true, mp,
	); err != nil {
		return nil, err
	}

	//handle.filterData = func(bat *batch.Batch) error {
	//	var (
	//		sels []int64
	//	)
	//
	//	tsCol := vector.MustFixedColWithTypeCheck[types.TS](bat.Vecs[bat.VectorCount()-1])
	//	for i, ts := range tsCol {
	//		fmt.Println(rel.GetTableName(), ts.ToString(), fromTs.ToString(), minTS.ToString())
	//		if ts.LT(&minTS) {
	//			sels = append(sels, int64(i))
	//		}
	//	}
	//
	//	if len(sels) > 0 {
	//		bat.Shrink(sels, true)
	//	}
	//
	//	return nil
	//}

	return handle, nil
}

func getCommitTSForClonedTable(
	ctx context.Context,
	eng engine.Engine,
	mp *mpool.MPool,
	txnOp client.TxnOperator,
	clonedTable engine.Relation,
) (commitTS types.TS, err error) {

	var (
		bat        *batch.Batch
		readers    []engine.Reader
		relData    engine.RelData
		moTableRel engine.Relation
		wantedCols = []string{catalog.SystemRelAttr_ID, objectio.DefaultCommitTS_Attr}
	)

	defer func() {
		if len(readers) == 1 {
			readers[0].Close()
		}
		if bat != nil {
			bat.Clean(mp)
		}
	}()

	if _, _, moTableRel, err = eng.GetRelationById(
		ctx, txnOp, catalog.MO_TABLES_ID,
	); err != nil {
		return
	}

	relData, err = moTableRel.Ranges(ctx, engine.DefaultRangesParam)

	if readers, err = moTableRel.BuildReaders(
		ctx, clonedTable.GetProcess(), nil, relData, 1, 0,
		false, engine.Policy_CheckCommittedOnly, engine.FilterHint{},
	); err != nil {
		return
	}

	bat = batch.NewWithSize(len(wantedCols))
	bat.Attrs = append(bat.Attrs, wantedCols...)
	bat.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_TS.ToType())

	stop := false
	for !stop {
		if stop, err = readers[0].Read(ctx, wantedCols, nil, mp, bat); err != nil {
			return
		}

		if bat.RowCount() != 0 {
			fmt.Println(bat.Attrs, common.MoBatchToString(bat, bat.RowCount()))
			fmt.Println()
		}
	}

	return commitTS, nil
}

/////////////////

//////////////
