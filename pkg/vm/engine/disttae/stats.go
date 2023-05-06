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
	"math"
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
func getInfoFromZoneMap(ctx context.Context, columns []int, blocks [][]catalog.BlockInfo, tableCnt float64, tableDef *plan.TableDef, proc *process.Process) (*plan2.InfoFromZoneMap, error) {

	lenCols := len(columns)
	info := plan2.NewInfoFromZoneMap(lenCols)

	var err error
	var objectMeta objectio.ObjectMeta
	//group blocks to objects
	objs := groupBlocksToObjectsForStats(blocks)

	var init bool
	for i := range objs {
		if objectMeta, err = loadObjectMeta(ctx, objs[i].MetaLocation(), proc.FileService, proc.Mp()); err != nil {
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
				index.UpdateZM(&info.ColumnZMs[idx], zm.GetMaxBuf())
				index.UpdateZM(&info.ColumnZMs[idx], zm.GetMinBuf())
				info.ColumnNDVs[idx] += float64(objColMeta.Ndv())
			}
		}
	}

	//adjust ndv
	lenobjs := float64(len(objs))
	for idx := range columns {
		rate := info.ColumnNDVs[idx] / tableCnt
		if rate > 1 {
			rate = 1
		}
		info.ColumnNDVs[idx] /= math.Pow(lenobjs, (1 - rate))
	}
	return info, nil
}

// calculate the stats for scan node.
// we need to get the zonemap from cn, and eval the filters with zonemap
func CalcStats(ctx context.Context, blocks [][]catalog.BlockInfo, expr *plan.Expr, tableDef *plan.TableDef, proc *process.Process, sortKeyName string, s *plan2.StatsInfoMap) (stats *plan.Stats, err error) {
	var blockNumNeed, blockNumTotal int
	var tableCnt, cost int64
	exprMono := plan2.CheckExprIsMonotonic(ctx, expr)
	columnMap, columns, maxCol := plan2.GetColumnsByExpr(expr, tableDef)
	var meta objectio.ObjectMeta
	for i := range blocks {
		for _, blk := range blocks[i] {
			location := blk.MetaLocation()
			blockNumTotal++
			tableCnt += int64(location.Rows())
			ok := true
			if exprMono {
				if !objectio.IsSameObjectLocVsMeta(location, meta) {
					if meta, err = loadObjectMeta(ctx, location, proc.FileService, proc.Mp()); err != nil {
						return
					}
				}
				ok = needRead(ctx, expr, meta, blk, tableDef, columnMap, columns, maxCol, proc)
			}
			if ok {
				cost += int64(location.Rows())
				blockNumNeed++
			}
		}
	}

	stats = new(plan.Stats)
	stats.BlockNum = int32(blockNumNeed)
	stats.TableCnt = float64(tableCnt)
	stats.Cost = float64(cost)

	columns = plan2.MakeAllColumns(tableDef)
	if s.NeedUpdate(blockNumTotal) {
		info, err := getInfoFromZoneMap(ctx, columns, blocks, float64(tableCnt), tableDef, proc)
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
