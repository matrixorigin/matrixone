// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type RuntimeFilterEvaluator interface {
	Evaluate(objectio.ZoneMap) bool
}

type RuntimeInFilter struct {
	InList *vector.Vector
}

type RuntimeZonemapFilter struct {
	Zm objectio.ZoneMap
}

func (f *RuntimeInFilter) Evaluate(zm objectio.ZoneMap) bool {
	return zm.AnyIn(f.InList)
}

func (f *RuntimeZonemapFilter) Evaluate(zm objectio.ZoneMap) bool {
	return f.Zm.FastIntersect(zm)
}

func ApplyRuntimeFilters(
	ctx context.Context,
	proc *process.Process,
	tableDef *plan.TableDef,
	blockInfos objectio.BlockInfoSlice,
	exprs []*plan.Expr,
	runtimeFilters []process.RuntimeFilterMessage,
) ([]byte, error) {
	var err error
	evaluators := make([]RuntimeFilterEvaluator, len(runtimeFilters))

	for i, filter := range runtimeFilters {
		switch filter.Typ {
		case process.RuntimeFilter_IN:
			vec := vector.NewVec(types.T_any.ToType())
			err = vec.UnmarshalBinary(filter.Data)
			if err != nil {
				return nil, err
			}
			evaluators[i] = &RuntimeInFilter{
				InList: vec,
			}

		case process.RuntimeFilter_MIN_MAX:
			evaluators[i] = &RuntimeZonemapFilter{
				Zm: filter.Data,
			}
		}
	}

	var (
		objDataMeta objectio.ObjectDataMeta
		objMeta     objectio.ObjectMeta
		skipObj     bool
		auxIdCnt    int32
	)

	for _, expr := range exprs {
		auxIdCnt = plan2.AssignAuxIdForExpr(expr, auxIdCnt)
	}

	columnMap := make(map[int]int)
	zms := make([]objectio.ZoneMap, auxIdCnt)
	vecs := make([]*vector.Vector, auxIdCnt)
	plan2.GetColumnMapByExprs(exprs, tableDef, columnMap)

	defer func() {
		for i := range vecs {
			if vecs[i] != nil {
				vecs[i].Free(proc.Mp())
			}
		}
	}()

	errCtx := errutil.ContextWithNoReport(ctx, true)
	fs, err := fileservice.Get[fileservice.FileService](proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}
	curr := 1 // Skip the first block which is always the memtable
	for i := 1; i < blockInfos.Len(); i++ {
		blk := blockInfos.Get(i)
		location := blk.MetaLocation()

		if !objectio.IsSameObjectLocVsMeta(location, objDataMeta) {
			if objMeta, err = objectio.FastLoadObjectMeta(errCtx, &location, false, fs); err != nil {
				return nil, err
			}
			objDataMeta = objMeta.MustDataMeta()

			skipObj = false
			// here we only eval expr on the object meta if it has more than 2 blocks
			if objDataMeta.BlockCount() > 2 {
				for i, expr := range exprs {
					zm := colexec.GetExprZoneMap(errCtx, proc, expr, objDataMeta, columnMap, zms, vecs)
					if zm.IsInited() && !evaluators[i].Evaluate(zm) {
						skipObj = true
						break
					}
				}
			}
		}

		if skipObj {
			continue
		}

		var skipBlk bool

		// eval filter expr on the block
		blkMeta := objDataMeta.GetBlockMeta(uint32(location.ID()))
		for i, expr := range exprs {
			zm := colexec.GetExprZoneMap(errCtx, proc, expr, blkMeta, columnMap, zms, vecs)
			if zm.IsInited() && !evaluators[i].Evaluate(zm) {
				skipBlk = true
				break
			}
		}

		if skipBlk {
			continue
		}

		// store the block in ranges
		blockInfos.Set(curr, blk)
		curr++
	}

	return blockInfos.Slice(0, curr), nil
}
