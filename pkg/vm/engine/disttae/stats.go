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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func groupBlocksToObjectsForStats(blocks [][]catalog.BlockInfo) []*catalog.BlockInfo {
	var objs []*catalog.BlockInfo
	objMap := make(map[string]int, 0)
	for i := range blocks {
		for j := range blocks[i] {
			block := blocks[i][j]
			objName := block.MetaLocation().Name().String()
			if _, ok := objMap[objName]; !ok {
				objMap[objName] = 1
				objs = append(objs, &block)
			}
		}
	}
	return objs
}

// get ndv, minval , maxval, datatype from zonemap
func getInfoFromZoneMap(ctx context.Context, columns []int, blocks [][]catalog.BlockInfo, blockNumTotal int, tableDef *plan.TableDef, proc *process.Process) (*plan2.InfoFromZoneMap, error) {

	lenCols := len(columns)
	info := plan2.NewInfoFromZoneMap(lenCols, blockNumTotal)

	var err error
	var objectMeta objectio.ObjectMeta
	//group blocks to objects
	objs := groupBlocksToObjectsForStats(blocks)

	var init bool
	for i := range objs {
		location := objs[i].MetaLocation()
		if objectMeta, err = objectio.FastLoadObjectMeta(ctx, &location, proc.FileService); err != nil {
			return nil, err
		}
		if !init {
			init = true
			for idx, colIdx := range columns {
				objColMeta := objectMeta.ObjectColumnMeta(uint16(colIdx))
				info.ColumnZMs[idx] = objColMeta.ZoneMap().Clone()
				info.DataTypes[idx] = types.T(tableDef.Cols[columns[idx]].Typ.Id).ToType()
				info.ColumnNDVs[idx] = float64(objColMeta.Ndv())
			}
		} else {
			for idx, colIdx := range columns {
				objColMeta := objectMeta.ObjectColumnMeta(uint16(colIdx))
				zm := objColMeta.ZoneMap().Clone()
				if !zm.IsInited() {
					continue
				}
				//update zm
				index.UpdateZM(&info.ColumnZMs[idx], zm.GetMaxBuf())
				index.UpdateZM(&info.ColumnZMs[idx], zm.GetMinBuf())
				//update ndv
				ndv := float64(objColMeta.Ndv())
				if ndv > info.ColumnNDVs[idx] {
					info.ColumnNDVs[idx] = ndv
				}
				rate := ndv / 20000
				if rate > 1 {
					rate = 1
				}
				info.ColumnNDVs[idx] += float64(objColMeta.Ndv()) * rate
			}
		}
	}
	return info, nil
}

// calculate the stats for scan node.
// we need to get the zonemap from cn, and eval the filters with zonemap
func CalcStats(
	ctx context.Context,
	blocks [][]catalog.BlockInfo,
	expr *plan.Expr,
	tableDef *plan.TableDef,
	proc *process.Process,
	sortKeyName string,
	s *plan2.StatsInfoMap,
) (stats *plan.Stats, err error) {
	var (
		blockNumNeed, blockNumTotal int
		tableCnt, cost              int64
		columnMap                   map[int]int
		defCols, exprCols           []int
		maxCol                      int
		isMonoExpr                  bool
		meta                        objectio.ObjectMeta
	)
	if isMonoExpr = plan2.CheckExprIsMonotonic(ctx, expr); isMonoExpr {
		columnMap, defCols, exprCols, maxCol = plan2.GetColumnsByExpr(expr, tableDef)
	}
	for i := range blocks {
		blockNumTotal += len(blocks[i])
		for _, blk := range blocks[i] {
			location := blk.MetaLocation()
			tableCnt += int64(location.Rows())
			needed := true
			if isMonoExpr {
				if !objectio.IsSameObjectLocVsMeta(location, meta) {
					if meta, err = objectio.FastLoadObjectMeta(ctx, &location, proc.FileService); err != nil {
						return
					}
				}
				needed = needRead(ctx, expr, meta, blk, tableDef, columnMap, defCols, exprCols, maxCol, proc)
			}
			if needed {
				cost += int64(location.Rows())
				blockNumNeed++
			}
		}
	}

	stats = new(plan.Stats)
	stats.BlockNum = int32(blockNumNeed)
	stats.TableCnt = float64(tableCnt)
	stats.Cost = float64(cost)

	columns := plan2.MakeAllColumns(tableDef)
	if s.NeedUpdate(blockNumTotal) {
		info, err := getInfoFromZoneMap(ctx, columns, blocks, blockNumTotal, tableDef, proc)
		if err != nil {
			return plan2.DefaultStats(), nil
		}
		plan2.UpdateStatsInfoMap(info, columns, blockNumTotal, stats.TableCnt, tableDef, s)
	}

	if expr != nil {
		stats.Outcnt = plan2.EstimateOutCnt(expr, sortKeyName, stats.TableCnt, stats.Cost, s)
	} else {
		stats.Outcnt = stats.TableCnt
	}
	stats.Selectivity = stats.Outcnt / stats.TableCnt
	return stats, nil
}
