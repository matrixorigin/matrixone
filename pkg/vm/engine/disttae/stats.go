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

func makeColumns(tableDef *plan.TableDef) []int {
	lenCols := len(tableDef.Cols)
	cols := make([]int, lenCols)
	for i := 0; i < lenCols; i++ {
		cols[i] = i
	}
	return cols
}

// get minval , maxval, datatype from zonemap, and calculate ndv
// then put these in a statsInfoMap and return
func updateStatsInfoMap(ctx context.Context, blocks *[][]BlockMeta, blockNumTotal int, tableCnt float64, tableDef *plan.TableDef, s *plan2.StatsInfoMap) error {

	columns := makeColumns(tableDef)
	lenCols := len(columns)
	dataTypes := make([]types.Type, lenCols)
	maxVal := make([]any, lenCols)         //maxvalue of all blocks for column
	minVal := make([]any, lenCols)         //minvalue of all blocks for column
	valMap := make([]map[any]int, lenCols) // all distinct value in blocks zonemap
	for i := range columns {
		valMap[i] = make(map[any]int, blockNumTotal)
	}

	//first, get info needed from zonemap
	var init bool
	for i := range *blocks {
		for j := range (*blocks)[i] {
			zonemapVal, blkTypes, err := getZonemapDataFromMeta(ctx, columns, (*blocks)[i][j], tableDef)
			if err != nil {
				return err
			}
			if !init {
				init = true
				for i := range zonemapVal {
					minVal[i] = zonemapVal[i][0]
					maxVal[i] = zonemapVal[i][1]
					dataTypes[i] = types.T(blkTypes[i]).ToType()
				}
			}

			for colIdx := range zonemapVal {
				currentBlockMin := zonemapVal[colIdx][0]
				currentBlockMax := zonemapVal[colIdx][1]
				if s, ok := currentBlockMin.([]uint8); ok {
					valMap[colIdx][string(s)] = 1
				} else {
					valMap[colIdx][currentBlockMin] = 1
				}

				if compute.CompareGeneric(currentBlockMin, minVal[colIdx], dataTypes[colIdx]) < 0 {
					minVal[i] = zonemapVal[i][0]
				}
				if compute.CompareGeneric(currentBlockMax, maxVal[colIdx], dataTypes[colIdx]) > 0 {
					maxVal[i] = zonemapVal[i][1]
				}
			}
		}
	}

	//calc ndv with min,max,distinct value in zonemap, blocknumer and column type
	//set info in statsInfoMap
	for i := range columns {
		colName := tableDef.Cols[columns[i]].Name
		s.NdvMap[colName] = plan2.CalcNdv(minVal[i], maxVal[i], float64(len(valMap[i])), float64(blockNumTotal), tableCnt, dataTypes[i])
		s.DataTypeMap[colName] = dataTypes[i].Oid
		switch dataTypes[i].Oid {
		case types.T_int8:
			s.MinValMap[colName] = float64(minVal[i].(int8))
			s.MaxValMap[colName] = float64(maxVal[i].(int8))
		case types.T_int16:
			s.MinValMap[colName] = float64(minVal[i].(int16))
			s.MaxValMap[colName] = float64(maxVal[i].(int16))
		case types.T_int32:
			s.MinValMap[colName] = float64(minVal[i].(int32))
			s.MaxValMap[colName] = float64(maxVal[i].(int32))
		case types.T_int64:
			s.MinValMap[colName] = float64(minVal[i].(int64))
			s.MaxValMap[colName] = float64(maxVal[i].(int64))
		case types.T_uint8:
			s.MinValMap[colName] = float64(minVal[i].(uint8))
			s.MaxValMap[colName] = float64(maxVal[i].(uint8))
		case types.T_uint16:
			s.MinValMap[colName] = float64(minVal[i].(uint16))
			s.MaxValMap[colName] = float64(maxVal[i].(uint16))
		case types.T_uint32:
			s.MinValMap[colName] = float64(minVal[i].(uint32))
			s.MaxValMap[colName] = float64(maxVal[i].(uint32))
		case types.T_uint64:
			s.MinValMap[colName] = float64(minVal[i].(uint64))
			s.MaxValMap[colName] = float64(maxVal[i].(uint64))
		case types.T_date:
			s.MinValMap[colName] = float64(minVal[i].(types.Date))
			s.MaxValMap[colName] = float64(maxVal[i].(types.Date))
		}
	}
	return nil
}

// calculate the stats for scan node.
// we need to get the zonemap from cn, and eval the filters with zonemap
func CalcStats(ctx context.Context, blocks *[][]BlockMeta, expr *plan.Expr, tableDef *plan.TableDef, proc *process.Process, sortKeyName string) (*plan.Stats, error) {
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

	var statsCache *plan2.StatsCache
	s := plan2.GetStatsInfoMapFromCache(statsCache, tableDef.TblId)
	if s.NeedUpdate(blockNumTotal) {
		err := updateStatsInfoMap(ctx, blocks, blockNumTotal, stats.TableCnt, tableDef, s)
		if err != nil {
			return plan2.DefaultStats(), nil
		}
	}

	if expr != nil {
		stats.Outcnt = plan2.EstimateOutCnt(expr, sortKeyName, stats.TableCnt, stats.Cost, s)
	} else {
		stats.Outcnt = stats.TableCnt
	}
	stats.Selectivity = stats.Outcnt / stats.TableCnt
	return stats, nil
}
