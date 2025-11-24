// Copyright 2025 Matrix Origin
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

	if end.GE(&from) {
		handle := new(BranchChangeHandle)
		if handle.handle, err = rel.CollectChanges(
			ctx, from, end, mp,
		); err != nil {
			return nil, err
		}

		return handle, nil
	}

	return nil, nil
}
