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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func maybeUnique(v1, v2 any, rows int64) bool {
	switch value1 := v1.(type) {
	case int8:
		value2 := v2.(int8)
		return int64(value2-value1+1) >= rows
	case int16:
		value2 := v2.(int16)
		return int64(value2-value1+1) >= rows
	case int32:
		value2 := v2.(int32)
		return int64(value2-value1+1) >= rows
	case int64:
		value2 := v2.(int64)
		return value2-value1+1 >= rows
	case uint8:
		value2 := v2.(uint8)
		return int64(value2-value1+1) >= rows
	case uint16:
		value2 := v2.(uint16)
		return int64(value2-value1+1) >= rows
	case uint32:
		value2 := v2.(uint32)
		return int64(value2-value1+1) >= rows
	case uint64:
		value2 := v2.(uint64)
		return int64(value2-value1+1) >= rows
	}
	return true
}

// get minval , maxval, datatype from zonemap
func getInfoFromZoneMap(columns []int, ctx context.Context, blocks *[][]BlockMeta, blockNumTotal int, tableDef *plan.TableDef) (*plan2.InfoFromZoneMap, error) {

	lenCols := len(columns)
	info := plan2.NewInfoFromZoneMap(lenCols, blockNumTotal)

	//first, get info needed from zonemap
	var init bool
	for i := range *blocks {
		for j := range (*blocks)[i] {
			zonemapVal, blkTypes, err := getZonemapDataFromMeta(columns, (*blocks)[i][j], tableDef)
			if err != nil {
				return nil, err
			}
			if !init {
				init = true
				for i := range zonemapVal {
					info.MinVal[i] = zonemapVal[i][0]
					info.MaxVal[i] = zonemapVal[i][1]
					info.DataTypes[i] = types.T(blkTypes[i]).ToType()
					info.MaybeUniqueMap[i] = true
				}
			}

			for colIdx := range zonemapVal {
				currentBlockMin := zonemapVal[colIdx][0]
				currentBlockMax := zonemapVal[colIdx][1]

				if !maybeUnique(currentBlockMin, currentBlockMax, (*blocks)[i][j].Rows) {
					info.MaybeUniqueMap[colIdx] = false
				}

				if s, ok := currentBlockMin.([]uint8); ok {
					info.ValMap[colIdx][string(s)] = 1
				} else {
					info.ValMap[colIdx][currentBlockMin] = 1
				}

				if compute.CompareGeneric(currentBlockMin, info.MinVal[colIdx], info.DataTypes[colIdx].Oid) < 0 {
					info.MinVal[colIdx] = currentBlockMin
				}
				if compute.CompareGeneric(currentBlockMax, info.MaxVal[colIdx], info.DataTypes[colIdx].Oid) > 0 {
					info.MaxVal[colIdx] = currentBlockMax
				}
			}
		}
	}

	return info, nil
}

// calculate the stats for scan node.
// we need to get the zonemap from cn, and eval the filters with zonemap
func CalcStats(ctx context.Context, blocks *[][]BlockMeta, expr *plan.Expr, tableDef *plan.TableDef, proc *process.Process, sortKeyName string, s *plan2.StatsInfoMap) (*plan.Stats, error) {
	var blockNumNeed, blockNumTotal int
	var tableCnt, cost int64
	exprMono := plan2.CheckExprIsMonotonic(ctx, expr)
	columnMap, columns, maxCol := plan2.GetColumnsByExpr(expr, tableDef)
	for i := range *blocks {
		for j := range (*blocks)[i] {
			blockNumTotal++
			tableCnt += (*blocks)[i][j].Rows
			if !exprMono || needRead(ctx, expr, (*blocks)[i][j], tableDef, columnMap, columns, maxCol, proc) {
				cost += (*blocks)[i][j].Rows
				blockNumNeed++
			}
		}
	}

	stats := new(plan.Stats)
	stats.BlockNum = int32(blockNumNeed)
	stats.TableCnt = float64(tableCnt)
	stats.Cost = float64(cost)

	columns = plan2.MakeAllColumns(tableDef)
	if s.NeedUpdate(blockNumTotal) {
		info, err := getInfoFromZoneMap(columns, ctx, blocks, blockNumTotal, tableDef)
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
