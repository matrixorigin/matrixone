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
	handle engine.ChangesHandle
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

	return b.handle.Next(ctx, mp)
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

	if end.GE(&from) {
		handle := new(BranchChangeHandle)
		ctx = engine.WithSnapshotReadPolicy(ctx, engine.SnapshotReadPolicyVisibleState)
		var err error
		if handle.handle, err = rel.CollectChanges(
			ctx, from, end, false, mp,
		); err != nil {
			return nil, err
		}
		return handle, nil
	}

	return nil, nil
}

// CollectChangesWithPKFilter is the same as CollectChanges but additionally
// attaches a PK filter to the context so that CollectChanges can prune
// objects and blocks via ZoneMap that do not match the requested PK values.
// Only DATA BRANCH PICK uses this; other callers use the plain CollectChanges.
// Row-level PK filtering is handled downstream by the data branch hashmap.
var CollectChangesWithPKFilter = func(
	ctx context.Context,
	rel engine.Relation,
	from types.TS,
	end types.TS,
	mp *mpool.MPool,
	pkFilter *engine.PKFilter,
) (engine.ChangesHandle, error) {

	if end.GE(&from) {
		handle := new(BranchChangeHandle)
		ctx = engine.WithSnapshotReadPolicy(ctx, engine.SnapshotReadPolicyVisibleState)
		if pkFilter != nil && len(pkFilter.Segments) > 0 {
			ctx = engine.WithPKFilter(ctx, pkFilter)
		}
		var err error
		if handle.handle, err = rel.CollectChanges(
			ctx, from, end, false, mp,
		); err != nil {
			return nil, err
		}
		return handle, nil
	}

	return nil, nil
}
