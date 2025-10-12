package databranchutils

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
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
	txnSnapshot timestamp.Timestamp,
	rel engine.Relation,
	fromTs, toTs types.TS,
	mp *mpool.MPool,
	isClonedTable bool,
) (engine.ChangesHandle, error) {
	//return rel.CollectChanges(ctx, fromTs, toTs, true, mp)
	var (
		err error
	)

	handle := new(BranchChangeHandle)

	minTS := fromTs
	if isClonedTable {
		item := eng.(*disttae.Engine).GetLatestCatalogCache().GetTableByIdAndTime(
			accountId,
			rel.GetDBID(ctx), rel.GetTableID(ctx),
			txnSnapshot,
		)

		t := types.TimestampToTS(item.Ts)
		if t.Next(); t.GT(&minTS) {
			minTS = t
		}
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
