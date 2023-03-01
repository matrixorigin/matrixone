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
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
)

type statsInfoMap struct {
	ndvMap      map[string]float64
	minValMap   map[string]float64
	maxValMap   map[string]float64
	dataTypeMap map[string]types.T
}

func estimateOutCntBySortOrder(tableCnt, cost float64, sortOrder int) float64 {
	if sortOrder == -1 {
		return cost
	}
	// coefficient is 0.5 when tableCnt equals cost, and 1 when tableCnt >> cost
	coefficient1 := math.Pow(0.6, cost/tableCnt)
	// coefficient is 0.25 when tableCnt is small, and 1 when very large table.
	coefficient2 := math.Pow(0.3, (1 / math.Log10(tableCnt)))

	outCnt := cost * coefficient1 * coefficient2
	if sortOrder == 0 {
		return outCnt * 0.95
	} else if sortOrder == 1 {
		return outCnt * 0.7
	} else if sortOrder == 2 {
		return outCnt * 0.25
	} else {
		return outCnt * 0.1
	}

}

func estimateOutCntForEquality(expr *plan.Expr, sortKeyName string, tableCnt, cost float64, ndvMap map[string]float64) float64 {
	// only filter like func(col)>1 , or (col=1) or (col=2) can estimate outcnt
	// and only 1 colRef is allowd in the filter. otherwise, no good method to calculate
	ret, col := plan2.CheckFilter(expr)
	if !ret {
		return cost / 100
	}
	sortOrder := util.GetClusterByColumnOrder(sortKeyName, col.Name)
	//if col is clusterby, we assume most of the rows in blocks we read is needed
	//otherwise, deduce selectivity according to ndv
	if sortOrder != -1 {
		return estimateOutCntBySortOrder(tableCnt, cost, sortOrder)
	} else {
		//check strict filter, otherwise can not estimate outcnt by ndv
		ret, col, _ := plan2.CheckStrictFilter(expr)
		if ret {
			if ndv, ok := ndvMap[col.Name]; ok {
				return tableCnt / ndv
			}
		}
	}
	return cost / 100
}

func calcOutCntByMinMax(funcName string, tableCnt, min, max, val float64) float64 {
	switch funcName {
	case ">", ">=":
		return (max - val) / (max - min) * tableCnt
	case "<", "<=":
		return (val - min) / (max - min) * tableCnt
	}
	return -1 // never reach here
}

func estimateOutCntForNonEquality(expr *plan.Expr, funcName, sortKeyName string, tableCnt, cost float64, s statsInfoMap) float64 {
	// only filter like func(col)>1 , or (col=1) or (col=2) can estimate outcnt
	// and only 1 colRef is allowd in the filter. otherwise, no good method to calculate
	ret, col := plan2.CheckFilter(expr)
	if !ret {
		return cost / 10
	}
	sortOrder := util.GetClusterByColumnOrder(sortKeyName, col.Name)
	//if col is clusterby, we assume most of the rows in blocks we read is needed
	//otherwise, deduce selectivity according to ndv
	if sortOrder != -1 {
		return estimateOutCntBySortOrder(tableCnt, cost, sortOrder)
	} else {
		//check strict filter, otherwise can not estimate outcnt by min/max val
		ret, col, constExpr := plan2.CheckStrictFilter(expr)
		if ret {
			switch s.dataTypeMap[col.Name] {
			case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
				if val, valOk := constExpr.Value.(*plan.Const_I64Val); valOk {
					return calcOutCntByMinMax(funcName, tableCnt, s.minValMap[col.Name], s.maxValMap[col.Name], float64(val.I64Val))
				}
			case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
				if val, valOk := constExpr.Value.(*plan.Const_U64Val); valOk {
					return calcOutCntByMinMax(funcName, tableCnt, s.minValMap[col.Name], s.maxValMap[col.Name], float64(val.U64Val))
				}
			case types.T_date:
				if val, valOk := constExpr.Value.(*plan.Const_Dateval); valOk {
					return calcOutCntByMinMax(funcName, tableCnt, s.minValMap[col.Name], s.maxValMap[col.Name], float64(val.Dateval))
				}
			}
		}
	}
	return cost / 10
}

// estimate output lines for a filter
func estimateOutCnt(expr *plan.Expr, sortKeyName string, tableCnt, cost float64, s statsInfoMap) float64 {
	if expr == nil {
		return cost
	}
	var outcnt float64
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funcName := exprImpl.F.Func.ObjName
		switch funcName {
		case "=":
			outcnt = estimateOutCntForEquality(expr, sortKeyName, tableCnt, cost, s.ndvMap)
		case ">", "<", ">=", "<=":
			//for filters like a>1, no good way to estimate, return 3 * equality
			outcnt = estimateOutCntForNonEquality(expr, funcName, sortKeyName, tableCnt, cost, s)
		case "and":
			//get the smaller one of two children, and tune it down a little bit
			out1 := estimateOutCnt(exprImpl.F.Args[0], sortKeyName, tableCnt, cost, s)
			out2 := estimateOutCnt(exprImpl.F.Args[1], sortKeyName, tableCnt, cost, s)
			outcnt = math.Min(out1, out2) * 0.8
		case "or":
			//get the bigger one of two children, and tune it up a little bit
			out1 := estimateOutCnt(exprImpl.F.Args[0], sortKeyName, tableCnt, cost, s)
			out2 := estimateOutCnt(exprImpl.F.Args[1], sortKeyName, tableCnt, cost, s)
			outcnt = math.Max(out1, out2) * 1.5
		default:
			//no good way to estimate, just 0.1*cost
			outcnt = cost * 0.1
		}
	}
	if outcnt > cost {
		//outcnt must be smaller than cost
		return cost
	}
	return outcnt
}

func calcNdv(minVal, maxVal any, distinctValNum, blockNumTotal, tableCnt float64, t types.Type) float64 {
	ndv1 := calcNdvUsingMinMax(minVal, maxVal, t)
	ndv2 := calcNdvUsingDistinctValNum(distinctValNum, blockNumTotal, tableCnt)
	if ndv1 <= 0 {
		return ndv2
	}
	return math.Min(ndv1, ndv2)
}

// treat distinct val in zonemap like a sample , then estimate the ndv
// more blocks, more accurate
func calcNdvUsingDistinctValNum(distinctValNum, blockNumTotal, tableCnt float64) float64 {
	// coefficient is 0.1 when 1 block, and 1 when many blocks.
	coefficient := math.Pow(0.1, (1 / math.Log10(blockNumTotal*10)))
	// very little distinctValNum, assume ndv is very low
	if distinctValNum <= 1 {
		return 1 // only one value
	} else if distinctValNum == 2 {
		return 2 / coefficient
	} else if distinctValNum <= 10 && distinctValNum/blockNumTotal < 0.2 {
		return distinctValNum / coefficient
	}
	// assume ndv is high
	// ndvRate is from 0 to 1. 1 means unique key, and 0 means ndv is only 1
	ndvRate := (distinctValNum / blockNumTotal) / 2
	ndv := tableCnt * ndvRate * coefficient
	if ndv < 1 {
		ndv = 1
	}
	return ndv
}

func calcNdvUsingMinMax(minVal, maxVal any, t types.Type) float64 {
	switch t.Oid {
	case types.T_bool:
		return 2
	case types.T_int8:
		return float64(maxVal.(int8)-minVal.(int8)) + 1
	case types.T_int16:
		return float64(maxVal.(int16)-minVal.(int16)) + 1
	case types.T_int32:
		return float64(maxVal.(int32)-minVal.(int32)) + 1
	case types.T_int64:
		return float64(maxVal.(int64)-minVal.(int64)) + 1
	case types.T_uint8:
		return float64(maxVal.(uint8)-minVal.(uint8)) + 1
	case types.T_uint16:
		return float64(maxVal.(uint16)-minVal.(uint16)) + 1
	case types.T_uint32:
		return float64(maxVal.(uint32)-minVal.(uint32)) + 1
	case types.T_uint64:
		return float64(maxVal.(uint64)-minVal.(uint64)) + 1
	case types.T_decimal64:
		return maxVal.(types.Decimal64).Sub(minVal.(types.Decimal64)).ToFloat64() + 1
	case types.T_decimal128:
		return maxVal.(types.Decimal128).Sub(minVal.(types.Decimal128)).ToFloat64() + 1
	case types.T_float32:
		return float64(maxVal.(float32)-minVal.(float32)) + 1
	case types.T_float64:
		return maxVal.(float64) - minVal.(float64) + 1
	case types.T_timestamp:
		return float64(maxVal.(types.Timestamp)-minVal.(types.Timestamp)) + 1
	case types.T_date:
		return float64(maxVal.(types.Date)-minVal.(types.Date)) + 1
	case types.T_time:
		return float64(maxVal.(types.Time)-minVal.(types.Time)) + 1
	case types.T_datetime:
		return float64(maxVal.(types.Datetime)-minVal.(types.Datetime)) + 1
	case types.T_uuid, types.T_char, types.T_varchar, types.T_blob, types.T_json, types.T_text:
		return -1
	default:
		return -1
	}
}

// get minval , maxval, datatype from zonemap, and calculate ndv
// then put these in a statsInfoMap and return
func getStatsInfoMap(ctx context.Context, columns []int, blocks *[][]BlockMeta, blockNumTotal int, tableCnt float64, tableDef *plan.TableDef) (statsInfoMap, error) {
	lenCols := len(columns)
	dataTypes := make([]types.Type, lenCols)
	maxVal := make([]any, lenCols)         //maxvalue of all blocks for column
	minVal := make([]any, lenCols)         //minvalue of all blocks for column
	valMap := make([]map[any]int, lenCols) // all distinct value in blocks zonemap
	for i := range columns {
		valMap[i] = make(map[any]int, blockNumTotal)
	}

	var s statsInfoMap
	//first, get info needed from zonemap
	var init bool
	for i := range *blocks {
		for j := range (*blocks)[i] {
			zonemapVal, blkTypes, err := getZonemapDataFromMeta(ctx, columns, (*blocks)[i][j], tableDef)
			if err != nil {
				return s, err
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
				if s, ok := currentBlockMax.([]uint8); ok {
					valMap[colIdx][string(s)] = 1
				} else {
					valMap[colIdx][currentBlockMax] = 1
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
	s.ndvMap = make(map[string]float64, lenCols)
	s.minValMap = make(map[string]float64, lenCols)
	s.maxValMap = make(map[string]float64, lenCols)
	s.dataTypeMap = make(map[string]types.T, lenCols)
	for i := range columns {
		colName := tableDef.Cols[columns[i]].Name
		s.ndvMap[colName] = calcNdv(minVal[i], maxVal[i], float64(len(valMap[i])), float64(blockNumTotal), tableCnt, dataTypes[i])
		s.dataTypeMap[colName] = dataTypes[i].Oid
		switch dataTypes[i].Oid {
		case types.T_int8:
			s.minValMap[colName] = float64(minVal[i].(int8))
			s.maxValMap[colName] = float64(maxVal[i].(int8))
		case types.T_int16:
			s.minValMap[colName] = float64(minVal[i].(int16))
			s.maxValMap[colName] = float64(maxVal[i].(int16))
		case types.T_int32:
			s.minValMap[colName] = float64(minVal[i].(int32))
			s.maxValMap[colName] = float64(maxVal[i].(int32))
		case types.T_int64:
			s.minValMap[colName] = float64(minVal[i].(int64))
			s.maxValMap[colName] = float64(maxVal[i].(int64))
		case types.T_uint8:
			s.minValMap[colName] = float64(minVal[i].(uint8))
			s.maxValMap[colName] = float64(maxVal[i].(uint8))
		case types.T_uint16:
			s.minValMap[colName] = float64(minVal[i].(uint16))
			s.maxValMap[colName] = float64(maxVal[i].(uint16))
		case types.T_uint32:
			s.minValMap[colName] = float64(minVal[i].(uint32))
			s.maxValMap[colName] = float64(maxVal[i].(uint32))
		case types.T_uint64:
			s.minValMap[colName] = float64(minVal[i].(uint64))
			s.maxValMap[colName] = float64(maxVal[i].(uint64))
		case types.T_date:
			s.minValMap[colName] = float64(minVal[i].(types.Date))
			s.maxValMap[colName] = float64(maxVal[i].(types.Date))
		}
	}

	return s, nil
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
	if expr != nil {
		s, err := getStatsInfoMap(ctx, columns, blocks, blockNumTotal, stats.TableCnt, tableDef)
		if err != nil {
			return plan2.DefaultStats(), nil
		}
		stats.Outcnt = estimateOutCnt(expr, sortKeyName, stats.TableCnt, stats.Cost, s)
	} else {
		stats.Outcnt = stats.TableCnt
	}
	stats.Selectivity = stats.Outcnt / stats.TableCnt
	return stats, nil
}
