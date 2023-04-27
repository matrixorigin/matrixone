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

func maybeUnique(zm objectio.ZoneMap, rows uint32) bool {
	switch zm.GetType() {
	case types.T_int8:
		return uint32(types.DecodeInt8(zm.GetMaxBuf()))-uint32(types.DecodeInt8(zm.GetMinBuf())+1) >= rows
	case types.T_int16:
		return uint32(types.DecodeInt16(zm.GetMaxBuf()))-uint32(types.DecodeInt16(zm.GetMinBuf())+1) >= rows
	case types.T_int32:
		return uint32(types.DecodeInt32(zm.GetMaxBuf()))-uint32(types.DecodeInt32(zm.GetMinBuf())+1) >= rows
	case types.T_int64:
		return uint32(types.DecodeInt64(zm.GetMaxBuf()))-uint32(types.DecodeInt64(zm.GetMinBuf())+1) >= rows
	case types.T_uint8:
		return uint32(types.DecodeUint8(zm.GetMaxBuf()))-uint32(types.DecodeUint8(zm.GetMinBuf())+1) >= rows
	case types.T_uint16:
		return uint32(types.DecodeUint16(zm.GetMaxBuf()))-uint32(types.DecodeUint16(zm.GetMinBuf())+1) >= rows
	case types.T_uint32:
		return uint32(types.DecodeUint32(zm.GetMaxBuf()))-uint32(types.DecodeUint32(zm.GetMinBuf())+1) >= rows
	case types.T_uint64:
		return uint32(types.DecodeUint64(zm.GetMaxBuf()))-uint32(types.DecodeUint64(zm.GetMinBuf())+1) >= rows
	}
	return true
}

// get minval , maxval, datatype from zonemap
func getInfoFromZoneMap(ctx context.Context, columns []int, blocks [][]catalog.BlockInfo, blockNumTotal int, tableDef *plan.TableDef, proc *process.Process) (*plan2.InfoFromZoneMap, error) {

	lenCols := len(columns)
	info := plan2.NewInfoFromZoneMap(lenCols, blockNumTotal)

	var err error
	var objectMeta objectio.ObjectMeta
	//first, get info needed from zonemap
	var init bool
	for i := range blocks {
		for _, blk := range blocks[i] {
			location := blk.MetaLocation()
			if !objectio.IsSameObjectLocVsMeta(location, objectMeta) {
				if objectMeta, err = loadObjectMeta(ctx, location, proc.FileService, proc.Mp()); err != nil {
					return nil, err
				}
			}
			num := location.ID()
			if !init {
				init = true
				for idx, colIdx := range columns {
					info.ColumnZMs[idx] = objectMeta.GetColumnMeta(uint32(num), uint16(colIdx)).ZoneMap().Clone()
					info.DataTypes[idx] = types.T(tableDef.Cols[columns[idx]].Typ.Id).ToType()
					info.MaybeUniqueMap[idx] = true
				}
			}

			rows := location.Rows()

			for idx, colIdx := range columns {
				colMeta := objectMeta.GetColumnMeta(uint32(num), uint16(colIdx))
				// if colMeta.Ndv() != rows {
				// 	info.MaybeUniqueMap[idx] = false
				// }
				zm := colMeta.ZoneMap()

				if !maybeUnique(zm, rows) {
					info.MaybeUniqueMap[idx] = false
				}

				info.ValMap[idx][string(zm.GetMinBuf())] = 1

				index.UpdateZM(&info.ColumnZMs[idx], zm.GetMaxBuf())
				index.UpdateZM(&info.ColumnZMs[idx], zm.GetMinBuf())
			}
		}
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
