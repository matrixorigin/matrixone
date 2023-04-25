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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
	defer func() { r.blks = r.blks[1:] }()

	info := &r.blks[0]

	if len(cols) != len(r.colIdxs) {
		if len(r.colIdxs) == 0 {
			r.colIdxs = make([]uint16, len(cols))
			r.colTypes = make([]types.Type, len(cols))
			r.colNulls = make([]bool, len(cols))
			r.pkidxInColIdxs = -1
			for i, column := range cols {
				// sometimes Name2ColIndex have no row_id， sometimes have one
				if column == catalog.Row_ID {
					if colIdx, ok := r.tableDef.Name2ColIndex[column]; ok {
						r.colIdxs[i] = uint16(colIdx)
					} else {
						r.colIdxs[i] = uint16(len(r.tableDef.Name2ColIndex))
					}
					r.colTypes[i] = types.T_Rowid.ToType()
				} else {
					r.colIdxs[i] = uint16(r.tableDef.Name2ColIndex[column])
					if r.colIdxs[i] == uint16(r.primaryIdx) {
						r.pkidxInColIdxs = i
						r.pkName = column
					}
					colDef := r.tableDef.Cols[r.colIdxs[i]]
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
	if len(r.ufs) == 0 {
		r.ufs = make([]func(*vector.Vector, *vector.Vector, int64) error, len(r.colTypes))
		for i, typ := range r.colTypes {
			r.ufs[i] = vector.GetUnionOneFunction(typ, mp)
		}
		if r.pkidxInColIdxs != -1 && r.expr != nil {
			var canCompute bool

			canCompute, r.searchFunc = getBinarySearchFuncByExpr(r.expr, r.pkName,
				r.colTypes[r.pkidxInColIdxs].Oid)
			if !canCompute {
				r.searchFunc = nil
			}
		}
	}
	if !r.blks[0].Sorted || r.pkidxInColIdxs == -1 || r.expr == nil {
		return blockio.BlockRead(r.ctx, info, r.colIdxs, r.colTypes, r.ts,
			r.fs, mp, vp, -1, nil, nil, nil)
	}
	return blockio.BlockRead(r.ctx, info, r.colIdxs, r.colTypes, r.ts,
		r.fs, mp, vp, r.pkidxInColIdxs, nil, r.ufs, r.searchFunc)
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
	info := &r.blks[0].meta.Info

	if len(cols) != len(r.colIdxs) {
		if len(r.colIdxs) == 0 {
			r.colIdxs = make([]uint16, len(cols))
			r.colTypes = make([]types.Type, len(cols))
			r.colNulls = make([]bool, len(cols))
			for i, column := range cols {
				// sometimes Name2ColIndex have no row_id， sometimes have one
				if column == catalog.Row_ID {
					if colIdx, ok := r.tableDef.Name2ColIndex[column]; ok {
						r.colIdxs[i] = uint16(colIdx)
					} else {
						r.colIdxs[i] = uint16(len(r.tableDef.Name2ColIndex))
					}
					r.colTypes[i] = types.T_Rowid.ToType()
				} else {
					r.colIdxs[i] = uint16(r.tableDef.Name2ColIndex[column])
					colDef := r.tableDef.Cols[r.colIdxs[i]]
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
	deleteRows := make([]int64, len(r.blks[0].deletes))
	for _, row := range r.blks[0].deletes {
		deleteRows = append(deleteRows, int64(row))
	}
	sort.Slice(deleteRows, func(i, j int) bool { return deleteRows[i] < deleteRows[j] })
	return blockio.BlockRead(r.ctx, info, r.colIdxs, r.colTypes, r.ts,
		r.fs, mp, vp, -1, deleteRows, nil, nil)
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
