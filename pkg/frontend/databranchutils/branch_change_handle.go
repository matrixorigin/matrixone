package databranchutils

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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

	if b.handle == nil {
		// collect nothing
		return
	}

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
	if b.handle != nil {
		return b.handle.Close()
	}
	return nil
}

var CollectChanges = func(
	ctx context.Context,
	rel engine.Relation,
	from types.TS,
	end types.TS,
	mp *mpool.MPool,
) (engine.ChangesHandle, error) {

	var (
		err error
	)

	handle := new(BranchChangeHandle)

	if end.GE(&from) {
		if handle.handle, err = rel.CollectChanges(
			ctx, from, end, false, mp,
		); err != nil {
			return nil, err
		}
	}

	return handle, nil
}

/////////////////

//////////////
