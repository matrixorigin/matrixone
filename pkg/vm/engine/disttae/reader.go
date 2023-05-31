// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

func (r *emptyReader) Close() error {
	return nil
}

func (r *emptyReader) Read(_ context.Context, _ []string,
	_ *plan.Expr, _ *mpool.MPool, _ engine.VectorPool) (*batch.Batch, error) {
	return nil, nil
}

func (r *blockReader) Close() error {
	return nil
}

func (r *blockReader) Read(ctx context.Context, cols []string,
	_ *plan.Expr, mp *mpool.MPool, vp engine.VectorPool) (*batch.Batch, error) {
	if len(r.blks) == 0 {
		return nil, nil
	}
	defer func() {
		r.blks = r.blks[1:]
		r.currentStep++
	}()

	blockInfo := r.blks[0]

	if len(cols) != len(r.seqnums) {
		if len(r.seqnums) == 0 {
			r.prefetchColIdxs = make([]uint16, 0)
			r.seqnums = make([]uint16, len(cols))
			r.colTypes = make([]types.Type, len(cols))
			r.colNulls = make([]bool, len(cols))
			r.pkidxInColIdxs = -1
			r.indexOfFirstSortedColumn = -1
			for i, column := range cols {
				// sometimes Name2ColIndex have no row_id， sometimes have one
				if column == catalog.Row_ID {
					// actually rowid's seqnum does not matter because it is generated in memory
					r.seqnums[i] = objectio.SEQNUM_ROWID
					r.colTypes[i] = types.T_Rowid.ToType()
				} else {
					if plan2.GetSortOrder(r.tableDef, column) == 0 {
						r.indexOfFirstSortedColumn = i
					}
					logicalIdx := r.tableDef.Name2ColIndex[column]
					colDef := r.tableDef.Cols[logicalIdx]
					r.seqnums[i] = uint16(colDef.Seqnum)
					r.prefetchColIdxs = append(r.prefetchColIdxs, r.seqnums[i])
					if int(r.seqnums[i]) == r.primarySeqnum {
						r.pkidxInColIdxs = i
						r.pkName = column
					}
					r.colTypes[i] = types.T(colDef.Typ.Id).ToType()
					if colDef.Default != nil {
						r.colNulls[i] = colDef.Default.NullAbility
					}
				}
			}
		} else {
			panic(moerr.NewInternalError(ctx, "blockReader reads different number of columns"))
		}
	}

	//prefetch some objects
	for len(r.steps) > 0 && r.steps[0] == r.currentStep {
		blockio.BlockPrefetch(r.prefetchColIdxs, r.fs, [][]*catalog.BlockInfo{r.infos[0]})
		r.infos = r.infos[1:]
		r.steps = r.steps[1:]
	}

	logutil.Debugf("read %v with %v", cols, r.seqnums)
	bat, err := blockio.BlockRead(r.ctx, blockInfo, nil, r.seqnums, r.colTypes, r.ts, r.fs, mp, vp)
	if err != nil {
		return nil, err
	}
	bat.SetAttributes(cols)

	if blockInfo.Sorted && r.indexOfFirstSortedColumn != -1 {
		bat.GetVector(int32(r.indexOfFirstSortedColumn)).SetSorted(true)
	}

	// if it's not sorted or no filter expr, just return
	if !blockInfo.Sorted || r.pkidxInColIdxs == -1 || r.expr == nil {
		return bat, nil
	}

	// if expr like : pkCol = xx，  we will try to find(binary search) the row in batch
	vec := bat.GetVector(int32(r.pkidxInColIdxs))
	if !r.init {
		r.init = true
		r.canCompute, r.searchFunc = getBinarySearchFuncByExpr(r.expr, r.pkName, vec.GetType().Oid)
	}
	if r.canCompute && r.searchFunc != nil {
		row := r.searchFunc(vec)
		if row >= vec.Length() {
			// can not find row.
			bat.Shrink([]int64{})
		} else if row > -1 {
			// maybe find row.
			bat.Shrink([]int64{int64(row)})
		}
	}

	logutil.Debug(testutil.OperatorCatchBatch("block reader", bat))
	return bat, nil
}

func (r *blockMergeReader) Close() error {
	return nil
}

func (r *blockMergeReader) Read(ctx context.Context, cols []string,
	expr *plan.Expr, mp *mpool.MPool, vp engine.VectorPool) (*batch.Batch, error) {
	if len(r.blks) == 0 {
		r.sels = nil
		return nil, nil
	}
	defer func() { r.blks = r.blks[1:] }()
	info := &r.blks[0].meta

	if len(cols) != len(r.seqnums) {
		if len(r.seqnums) == 0 {
			r.seqnums = make([]uint16, len(cols))
			r.colTypes = make([]types.Type, len(cols))
			r.colNulls = make([]bool, len(cols))
			for i, column := range cols {
				// sometimes Name2ColIndex have no row_id， sometimes have one
				// sometimes Name2ColIndex have no row_id， sometimes have one
				if column == catalog.Row_ID {
					// actually rowid's seqnum does not matter because it is generated in memory
					r.seqnums[i] = objectio.SEQNUM_ROWID
					r.colTypes[i] = types.T_Rowid.ToType()
				} else {
					logicalIdx := r.tableDef.Name2ColIndex[column]
					colDef := r.tableDef.Cols[logicalIdx]
					r.seqnums[i] = uint16(colDef.Seqnum)
					r.colTypes[i] = types.T(colDef.Typ.Id).ToType()
					if colDef.Default != nil {
						r.colNulls[i] = colDef.Default.NullAbility
					}
				}
			}
		} else {
			panic(moerr.NewInternalError(ctx, "blockReader reads different number of columns"))
		}
	}

	logutil.Debugf("read %v with %v", cols, r.seqnums)

	//TODO::there is a bug to fix.
	//var deletes []int64
	//if len(r.blks[0].deletes) > 0 {
	//	deletes := make([]int64, len(r.blks[0].deletes))
	//	copy(deletes, r.blks[0].deletes)
	//	//FIXME::why sort deletes?  batch.Shrink need to shrink by the ordered sels.
	//	sort.Slice(deletes, func(i, j int) bool {
	//		return deletes[i] < deletes[j]
	//	})
	//}
	bat, err := blockio.BlockRead(r.ctx, info, nil, r.seqnums, r.colTypes, r.ts, r.fs, mp, vp)
	if err != nil {
		return nil, err
	}
	bat.SetAttributes(cols)

	//start to load deletes, which maybe
	//in txn.blockId_dn_delete_metaLoc_batch or in partitionState.
	if _, ok := r.table.db.txn.blockId_dn_delete_metaLoc_batch[r.blks[0].meta.BlockID]; ok {
		deletes, err := r.table.LoadDeletesForBlock(r.blks[0].meta.BlockID)
		if err != nil {
			return nil, err
		}
		//TODO:: need to optimize .
		r.blks[0].deletes = append(r.blks[0].deletes, deletes...)

	}

	{
		state, err := r.table.getPartitionState(ctx)
		if err != nil {
			return nil, err
		}
		ts := types.TimestampToTS(r.ts)
		iter := state.NewRowsIter(ts, &r.blks[0].meta.BlockID, true)
		for iter.Next() {
			entry := iter.Entry()
			_, offset := entry.RowID.Decode()
			r.blks[0].deletes = append(r.blks[0].deletes, int64(offset))
		}
		iter.Close()
	}
	//TODO::there is a bug to fix
	r.sels = r.sels[:0]
	deletes := make([]int64, len(r.blks[0].deletes))
	copy(deletes, r.blks[0].deletes)
	//sort.Ints(deletes)
	sort.Slice(deletes, func(i, j int) bool {
		return deletes[i] < deletes[j]
	})
	for i := 0; i < bat.Length(); i++ {
		if len(deletes) > 0 && i == int(deletes[0]) {
			deletes = deletes[1:]
			continue
		}
		r.sels = append(r.sels, int64(i))
	}
	bat.Shrink(r.sels)

	logutil.Debug(testutil.OperatorCatchBatch("block merge reader", bat))
	return bat, nil
}

func NewMergeReader(readers []engine.Reader) *mergeReader {
	return &mergeReader{
		rds: readers,
	}
}

func (r *mergeReader) Close() error {
	return nil
}

func (r *mergeReader) Read(ctx context.Context, cols []string,
	expr *plan.Expr, mp *mpool.MPool, vp engine.VectorPool) (*batch.Batch, error) {
	if len(r.rds) == 0 {
		return nil, nil
	}
	for len(r.rds) > 0 {
		bat, err := r.rds[0].Read(ctx, cols, expr, mp, vp)
		if err != nil {
			for _, rd := range r.rds {
				rd.Close()
			}
			return nil, err
		}
		if bat == nil {
			r.rds = r.rds[1:]
		}
		if bat != nil {
			logutil.Debug(testutil.OperatorCatchBatch("merge reader", bat))
			return bat, nil
		}
	}
	return nil, nil
}
