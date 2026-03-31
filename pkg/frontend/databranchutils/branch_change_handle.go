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
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ engine.ChangesHandle = new(BranchChangeHandle)

type BranchChangeHandle struct {
	handle          engine.ChangesHandle
	filterData      func(bat *batch.Batch) error
	filterTombstone func(bat *batch.Batch) error
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
	if tombstone != nil && b.filterTombstone != nil {
		if err = b.filterTombstone(tombstone); err != nil {
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
// attaches a PK filter to the context so that the engine layer can prune
// objects, blocks and rows that do not match the requested PK values.
// Only DATA BRANCH PICK uses this; other callers use the plain CollectChanges.
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
		if pkFilter != nil && pkFilter.Vec != nil {
			ctx = engine.WithPKFilter(ctx, pkFilter)
			// Data batches: PK is at index 0 (sort key column).
			handle.filterData = buildPKFilterFunc(pkFilter, 0)
			// Tombstone batches: index 0 = Rowid, index 1 = PK.
			handle.filterTombstone = buildPKFilterFunc(pkFilter, 1)
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

// buildPKFilterFunc creates a row-level filter callback that removes rows
// whose PK value does not appear in the filter vector.  This is the safety
// net that catches any rows not pruned at the object/block level.
// pkColIdx specifies which column holds the PK (0 for data batches, 1 for tombstone).
func buildPKFilterFunc(pkFilter *engine.PKFilter, pkColIdx int) func(bat *batch.Batch) error {
	return func(bat *batch.Batch) error {
		if bat == nil || bat.RowCount() == 0 || pkFilter == nil || pkFilter.Vec == nil {
			return nil
		}
		if pkColIdx >= len(bat.Vecs) {
			return nil
		}
		pkVec := bat.Vecs[pkColIdx]
		filterVec := pkFilter.Vec

		sels := make([]int64, 0, bat.RowCount())
		for i := range bat.RowCount() {
			if pkVec.GetNulls().Contains(uint64(i)) {
				continue
			}
			raw := pkVec.GetRawBytesAt(i)
			if filterVec.GetType().IsVarlen() {
				// For varlen types, do linear scan on filter vec
				found := false
				for j := range filterVec.Length() {
					if bytes.Equal(raw, filterVec.GetRawBytesAt(j)) {
						found = true
						break
					}
				}
				if found {
					sels = append(sels, int64(i))
				}
			} else {
				// For fixed-length types, use ContainsKey on a temp ZM per-value
				// or direct binary search.  Since AnyIn already did bulk check,
				// here we just do a simple linear scan as the safety net.
				found := false
				for j := range filterVec.Length() {
					if bytes.Equal(raw, filterVec.GetRawBytesAt(j)) {
						found = true
						break
					}
				}
				if found {
					sels = append(sels, int64(i))
				}
			}
		}

		if len(sels) == bat.RowCount() {
			return nil // all rows match
		}

		bat.Shrink(sels, false)
		return nil
	}
}
