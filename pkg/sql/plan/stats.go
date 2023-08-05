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

package plan

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const BlockNumForceOneCN = 200
const blockNDVThreshHold = 100
const blockSelectivityThreshHold = 0.95
const highNDVcolumnThreshHold = 0.95

// stats cache is small, no need to use LRU for now
type StatsCache struct {
	cachePool map[uint64]*StatsInfoMap
}

func NewStatsCache() *StatsCache {
	return &StatsCache{
		cachePool: make(map[uint64]*StatsInfoMap, 100),
	}
}

type StatsInfoMap struct {
	NdvMap      map[string]float64
	MinValMap   map[string]float64
	MaxValMap   map[string]float64
	DataTypeMap map[string]types.T
	BlockNumber int //detect if block number changes , update stats info map
	TableCnt    float64
	tableName   string
}

func NewStatsInfoMap() *StatsInfoMap {
	return &StatsInfoMap{
		NdvMap:      make(map[string]float64),
		MinValMap:   make(map[string]float64),
		MaxValMap:   make(map[string]float64),
		DataTypeMap: make(map[string]types.T),
		BlockNumber: 0,
		TableCnt:    0,
	}
}

func (sc *StatsInfoMap) NeedUpdate(currentBlockNum int) bool {
	if sc.BlockNumber == 0 || sc.BlockNumber != currentBlockNum {
		return true
	}
	return false
}

func (sc *StatsCache) GetStatsInfoMap(tableID uint64) *StatsInfoMap {
	if sc == nil {
		return NewStatsInfoMap()
	}
	switch tableID {
	case catalog.MO_DATABASE_ID, catalog.MO_TABLES_ID, catalog.MO_COLUMNS_ID:
		return NewStatsInfoMap()
	}
	if s, ok := (sc.cachePool)[tableID]; ok {
		return s
	} else {
		s = NewStatsInfoMap()
		(sc.cachePool)[tableID] = s
		return s
	}
}

type InfoFromZoneMap struct {
	ColumnZMs  []objectio.ZoneMap
	DataTypes  []types.Type
	ColumnNDVs []float64
	TableCnt   float64
}

func NewInfoFromZoneMap(lenCols int) *InfoFromZoneMap {
	info := &InfoFromZoneMap{
		ColumnZMs:  make([]objectio.ZoneMap, lenCols),
		DataTypes:  make([]types.Type, lenCols),
		ColumnNDVs: make([]float64, lenCols),
	}
	return info
}

func UpdateStatsInfoMap(info *InfoFromZoneMap, blockNumTotal int, tableDef *plan.TableDef, s *StatsInfoMap) {
	logutil.Debugf("need to update statsCache for table %v", tableDef.Name)
	s.BlockNumber = blockNumTotal
	s.TableCnt = info.TableCnt
	s.tableName = tableDef.Name
	//calc ndv with min,max,distinct value in zonemap, blocknumer and column type
	//set info in statsInfoMap
	for i, coldef := range tableDef.Cols[:len(tableDef.Cols)-1] {
		colName := coldef.Name
		s.NdvMap[colName] = info.ColumnNDVs[i]
		s.DataTypeMap[colName] = info.DataTypes[i].Oid
		if !info.ColumnZMs[i].IsInited() {
			s.MinValMap[colName] = 0
			s.MaxValMap[colName] = 0
			continue
		}
		switch info.DataTypes[i].Oid {
		case types.T_int8:
			s.MinValMap[colName] = float64(types.DecodeInt8(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeInt8(info.ColumnZMs[i].GetMaxBuf()))
		case types.T_int16:
			s.MinValMap[colName] = float64(types.DecodeInt16(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeInt16(info.ColumnZMs[i].GetMaxBuf()))
		case types.T_int32:
			s.MinValMap[colName] = float64(types.DecodeInt32(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeInt32(info.ColumnZMs[i].GetMaxBuf()))
		case types.T_int64:
			s.MinValMap[colName] = float64(types.DecodeInt64(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeInt64(info.ColumnZMs[i].GetMaxBuf()))
		case types.T_uint8:
			s.MinValMap[colName] = float64(types.DecodeUint8(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeUint8(info.ColumnZMs[i].GetMaxBuf()))
		case types.T_uint16:
			s.MinValMap[colName] = float64(types.DecodeUint16(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeUint16(info.ColumnZMs[i].GetMaxBuf()))
		case types.T_uint32:
			s.MinValMap[colName] = float64(types.DecodeUint32(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeUint32(info.ColumnZMs[i].GetMaxBuf()))
		case types.T_uint64:
			s.MinValMap[colName] = float64(types.DecodeUint64(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeUint64(info.ColumnZMs[i].GetMaxBuf()))
		case types.T_date:
			s.MinValMap[colName] = float64(types.DecodeDate(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeDate(info.ColumnZMs[i].GetMaxBuf()))
		}
	}
}

// cols in one table, return if ndv of  multi column is high enough
func isHighNdvCols(cols []int32, tableDef *TableDef, builder *QueryBuilder) bool {
	if tableDef == nil {
		return false
	}
	// first to check if it is primary key.
	if tableDef.Pkey != nil {
		pkNames := tableDef.Pkey.Names
		pks := make([]int32, len(pkNames))
		for i := range pkNames {
			pks[i] = tableDef.Name2ColIndex[pkNames[i]]
		}
		if containsAllPKs(cols, pks) {
			return true
		}
	}

	sc := builder.compCtx.GetStatsCache()
	if sc == nil {
		return false
	}
	s := sc.GetStatsInfoMap(tableDef.TblId)
	var totalNDV float64 = 1
	for i := range cols {
		totalNDV *= s.NdvMap[tableDef.Cols[cols[i]].Name]
	}
	return totalNDV > s.TableCnt*highNDVcolumnThreshHold
}

func getColStatsInfo(col *plan.ColRef, builder *QueryBuilder) *StatsInfoMap {
	if builder == nil {
		return nil
	}
	sc := builder.compCtx.GetStatsCache()
	if sc == nil {
		return nil
	}
	tableDef, ok := builder.tag2Table[col.RelPos]
	if !ok {
		return nil
	}
	//fix column name
	if len(col.Name) == 0 {
		col.Name = tableDef.Cols[col.ColPos].Name
	}

	return sc.GetStatsInfoMap(tableDef.TblId)
}

func getColNdv(col *plan.ColRef, builder *QueryBuilder) float64 {
	s := getColStatsInfo(col, builder)
	if s == nil {
		return -1
	}
	return s.NdvMap[col.Name]
}

// this function is used to calculate the ndv of expressions,
// like year(l_orderdate), substring(phone_number), and assume col is the first argument
// if only the ndv of column is needed, please call getColNDV
// if this function fail, it will return -1
func getExprNdv(expr *plan.Expr, builder *QueryBuilder) float64 {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funcName := exprImpl.F.Func.ObjName
		switch funcName {
		case "year":
			return getExprNdv(exprImpl.F.Args[0], builder) / 365
		case "substring":
			// no good way to calc ndv for substring
			return math.Min(getExprNdv(exprImpl.F.Args[0], builder), 25)
		default:
			return getExprNdv(exprImpl.F.Args[0], builder)
		}
	case *plan.Expr_Col:
		return getColNdv(exprImpl.Col, builder)
	}
	return -1
}

func estimateEqualitySelectivity(expr *plan.Expr, builder *QueryBuilder) float64 {
	// only filter like func(col)=1 or col=? can estimate outcnt
	// and only 1 colRef is allowd in the filter. otherwise, no good method to calculate
	ret, _ := CheckFilter(expr)
	if !ret {
		return 0.01
	}
	ndv := getExprNdv(expr, builder)
	if ndv > 0 {
		return 1 / ndv
	}
	return 0.01
}

func calcSelectivityByMinMax(funcName string, min, max, val float64) float64 {
	switch funcName {
	case ">", ">=":
		return (max - val) / (max - min)
	case "<", "<=":
		return (val - min) / (max - min)
	}
	return -1 // never reach here
}

func estimateNonEqualitySelectivity(expr *plan.Expr, funcName string, builder *QueryBuilder) float64 {
	// only filter like func(col)>1 , or (col=1) or (col=2) can estimate outcnt
	// and only 1 colRef is allowd in the filter. otherwise, no good method to calculate
	ret, col := CheckFilter(expr)
	if !ret {
		return 0.1
	}
	s := getColStatsInfo(col, builder)
	if s == nil {
		return 0.1
	}
	//check strict filter, otherwise can not estimate outcnt by min/max val
	ret, col, constExpr, _ := CheckStrictFilter(expr)
	if ret {
		switch s.DataTypeMap[col.Name] {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			if val, valOk := constExpr.Value.(*plan.Const_I64Val); valOk {
				return calcSelectivityByMinMax(funcName, s.MinValMap[col.Name], s.MaxValMap[col.Name], float64(val.I64Val))
			}
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			if val, valOk := constExpr.Value.(*plan.Const_U64Val); valOk {
				return calcSelectivityByMinMax(funcName, s.MinValMap[col.Name], s.MaxValMap[col.Name], float64(val.U64Val))
			}
		case types.T_date:
			if val, valOk := constExpr.Value.(*plan.Const_Dateval); valOk {
				return calcSelectivityByMinMax(funcName, s.MinValMap[col.Name], s.MaxValMap[col.Name], float64(val.Dateval))
			}
		}
	}

	//check strict filter, otherwise can not estimate outcnt by min/max val
	ret, col, constExpr, leftFuncName := CheckFunctionFilter(expr)
	if ret {
		switch leftFuncName {
		case "year":
			if val, valOk := constExpr.Value.(*plan.Const_I64Val); valOk {
				minVal := types.Date(s.MinValMap[col.Name])
				maxVal := types.Date(s.MaxValMap[col.Name])
				return calcSelectivityByMinMax(funcName, float64(minVal.Year()), float64(maxVal.Year()), float64(val.I64Val))
			}
		}
	}

	return 0.1
}

func estimateExprSelectivity(expr *plan.Expr, builder *QueryBuilder) float64 {
	if expr == nil {
		return 1
	}

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funcName := exprImpl.F.Func.ObjName
		switch funcName {
		case "=":
			return estimateEqualitySelectivity(expr, builder)
		case "!=", "<>":
			return 0.9
		case ">", "<", ">=", "<=":
			return estimateNonEqualitySelectivity(expr, funcName, builder)
		case "and":
			sel1 := estimateExprSelectivity(exprImpl.F.Args[0], builder)
			sel2 := estimateExprSelectivity(exprImpl.F.Args[1], builder)
			if canMergeToBetweenAnd(exprImpl.F.Args[0], exprImpl.F.Args[1]) && (sel1+sel2) > 1 {
				return sel1 + sel2 - 1
			} else {
				return andSelectivity(sel1, sel2)
			}
		case "or":
			sel1 := estimateExprSelectivity(exprImpl.F.Args[0], builder)
			sel2 := estimateExprSelectivity(exprImpl.F.Args[1], builder)
			return orSelectivity(sel1, sel2)
		case "not":
			return 1 - estimateExprSelectivity(exprImpl.F.Args[0], builder)
		case "like":
			return 0.2
		case "in":
			// use ndv map,do not need nodeID
			ndv := getExprNdv(expr, builder)
			if ndv > 10 {
				return 10 / ndv
			}
			return 0.5
		default:
			return 0.15
		}
	case *plan.Expr_C:
		return 1
	}
	return 1
}

func estimateFilterWeight(expr *plan.Expr, w float64) float64 {
	switch expr.Typ.Id {
	case int32(types.T_decimal64):
		w += 64
	case int32(types.T_decimal128):
		w += 128
	case int32(types.T_float32), int32(types.T_float64):
		w += 8
	case int32(types.T_char), int32(types.T_varchar), int32(types.T_text), int32(types.T_json):
		w += 4
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funcImpl := exprImpl.F
		switch funcImpl.Func.GetObjName() {
		case "like":
			w += 10
		case "cast":
			w += 3
		case "in":
			w += 2
		case "<>", "!=":
			w += 1.2
		case "<", "<=":
			w += 1.1
		default:
			w += 1
		}
		for _, child := range exprImpl.F.Args {
			w += estimateFilterWeight(child, 0)
		}
	}
	return w
}

// harsh estimate of block selectivity, will improve it in the future
func estimateFilterBlockSelectivity(ctx context.Context, expr *plan.Expr, tableDef *plan.TableDef, builder *QueryBuilder) float64 {
	if !CheckExprIsMonotonic(ctx, expr) {
		return 1
	}
	ret, col := CheckFilter(expr)
	if ret && col != nil {
		switch GetSortOrder(tableDef, col.Name) {
		case 0:
			return math.Min(expr.Selectivity, 0.5)
		case 1:
			return math.Min(expr.Selectivity*3, 0.5)
		case 2:
			return math.Min(expr.Selectivity*10, 0.5)
		}
	}
	if getExprNdv(expr, builder) < blockNDVThreshHold {
		return 1
	}
	// do not know selectivity for this expr, default 0.5
	return 0.5
}

func rewriteFilterListByStats(ctx context.Context, nodeID int32, builder *QueryBuilder) {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			rewriteFilterListByStats(ctx, child, builder)
		}
	}
	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		if node.ObjRef != nil && len(node.FilterList) >= 1 {
			sort.Slice(node.FilterList, func(i, j int) bool {
				cost1 := estimateFilterWeight(node.FilterList[i], 0) * estimateExprSelectivity(node.FilterList[i], builder)
				cost2 := estimateFilterWeight(node.FilterList[j], 0) * estimateExprSelectivity(node.FilterList[j], builder)
				return cost1 <= cost2
			})
			sort.Slice(node.BlockFilterList, func(i, j int) bool {
				blockSel1 := node.BlockFilterList[i].Selectivity
				blockSel2 := node.BlockFilterList[j].Selectivity
				return blockSel1 <= blockSel2
			})
		}
	}
}

func ReCalcNodeStats(nodeID int32, builder *QueryBuilder, recursive bool, leafNode bool) {
	node := builder.qry.Nodes[nodeID]
	if recursive {
		if len(node.Children) > 0 {
			for _, child := range node.Children {
				ReCalcNodeStats(child, builder, recursive, leafNode)
			}
		}
	}

	var leftStats, rightStats, childStats *Stats
	if len(node.Children) == 1 {
		childStats = builder.qry.Nodes[node.Children[0]].Stats
	} else if len(node.Children) == 2 {
		leftStats = builder.qry.Nodes[node.Children[0]].Stats
		rightStats = builder.qry.Nodes[node.Children[1]].Stats
	}

	switch node.NodeType {
	case plan.Node_JOIN:
		ndv := math.Min(leftStats.Outcnt, rightStats.Outcnt)
		if ndv < 1 {
			ndv = 1
		}
		//assume all join is not cross join
		//will fix this in the future
		//isCrossJoin := (len(node.OnList) == 0)
		isCrossJoin := false
		selectivity := math.Pow(rightStats.Selectivity, math.Pow(leftStats.Selectivity, 0.5))
		selectivity_out := math.Min(math.Pow(leftStats.Selectivity, math.Pow(rightStats.Selectivity, 0.5)), selectivity)

		for _, pred := range node.OnList {
			if pred.Ndv <= 0 {
				pred.Ndv = getExprNdv(pred, builder)
			}
		}
		hashmapStats := &plan.HashMapStats{
			HashmapSize: rightStats.Outcnt,
		}

		switch node.JoinType {
		case plan.Node_INNER:
			outcnt := leftStats.Outcnt * rightStats.Outcnt / ndv
			if !isCrossJoin {
				outcnt *= selectivity
			}
			if outcnt < rightStats.Outcnt && leftStats.Selectivity > 0.95 {
				outcnt = rightStats.Outcnt
			}
			node.Stats = &plan.Stats{
				Outcnt:       outcnt,
				Cost:         leftStats.Cost + rightStats.Cost,
				HashmapStats: hashmapStats,
				Selectivity:  selectivity_out,
			}

		case plan.Node_LEFT:
			outcnt := leftStats.Outcnt
			node.Stats = &plan.Stats{
				Outcnt:       outcnt,
				Cost:         leftStats.Cost + rightStats.Cost,
				HashmapStats: hashmapStats,
				Selectivity:  selectivity_out,
			}

		case plan.Node_RIGHT:
			outcnt := rightStats.Outcnt
			node.Stats = &plan.Stats{
				Outcnt:       outcnt,
				Cost:         leftStats.Cost + rightStats.Cost,
				HashmapStats: hashmapStats,
				Selectivity:  selectivity_out,
			}

		case plan.Node_OUTER:
			outcnt := leftStats.Outcnt + rightStats.Outcnt
			node.Stats = &plan.Stats{
				Outcnt:       outcnt,
				Cost:         leftStats.Cost + rightStats.Cost,
				HashmapStats: hashmapStats,
				Selectivity:  selectivity_out,
			}

		case plan.Node_SEMI:
			node.Stats = &plan.Stats{
				Outcnt:       leftStats.Outcnt * selectivity,
				Cost:         leftStats.Cost + rightStats.Cost,
				HashmapStats: hashmapStats,
				Selectivity:  selectivity_out,
			}
		case plan.Node_ANTI:
			node.Stats = &plan.Stats{
				Outcnt:       leftStats.Outcnt * (1 - rightStats.Selectivity) * 0.5,
				Cost:         leftStats.Cost + rightStats.Cost,
				HashmapStats: hashmapStats,
				Selectivity:  selectivity_out * 0.5,
			}

		case plan.Node_SINGLE, plan.Node_MARK:
			node.Stats = &plan.Stats{
				Outcnt:       leftStats.Outcnt,
				Cost:         leftStats.Cost + rightStats.Cost,
				HashmapStats: hashmapStats,
				Selectivity:  selectivity_out,
			}
		}

	case plan.Node_AGG:
		if len(node.GroupBy) > 0 {
			incnt := childStats.Outcnt
			outcnt := 1.0
			for _, groupby := range node.GroupBy {
				ndv := getExprNdv(groupby, builder)
				if ndv > 1 {
					groupby.Ndv = ndv
					outcnt *= ndv
				}
			}
			if outcnt > incnt {
				outcnt = math.Min(incnt, outcnt*math.Pow(childStats.Selectivity, 0.8))
			}
			hashmapStats := &plan.HashMapStats{
				HashmapSize: outcnt,
			}
			node.Stats = &plan.Stats{
				Outcnt:       outcnt,
				Cost:         incnt + outcnt,
				HashmapStats: hashmapStats,
				Selectivity:  1,
			}
			if len(node.FilterList) > 0 {
				node.Stats.Outcnt *= 0.05
				node.Stats.Selectivity *= 0.05
			}
		} else {
			hashmapStats := &plan.HashMapStats{
				HashmapSize: 1,
			}
			node.Stats = &plan.Stats{
				Outcnt:       1,
				Cost:         childStats.Cost,
				Selectivity:  1,
				HashmapStats: hashmapStats,
			}
		}

	case plan.Node_UNION:
		hashmapStats := &plan.HashMapStats{
			HashmapSize: rightStats.Outcnt,
		}
		node.Stats = &plan.Stats{
			Outcnt:       (leftStats.Outcnt + rightStats.Outcnt) * 0.7,
			Cost:         leftStats.Outcnt + rightStats.Outcnt,
			HashmapStats: hashmapStats,
			Selectivity:  1,
		}
	case plan.Node_UNION_ALL:
		node.Stats = &plan.Stats{
			Outcnt:      leftStats.Outcnt + rightStats.Outcnt,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			Selectivity: 1,
		}
	case plan.Node_INTERSECT:
		hashmapStats := &plan.HashMapStats{
			HashmapSize: rightStats.Outcnt,
		}
		node.Stats = &plan.Stats{
			Outcnt:       math.Min(leftStats.Outcnt, rightStats.Outcnt) * 0.5,
			Cost:         leftStats.Outcnt + rightStats.Outcnt,
			HashmapStats: hashmapStats,
			Selectivity:  1,
		}
	case plan.Node_INTERSECT_ALL:
		hashmapStats := &plan.HashMapStats{
			HashmapSize: rightStats.Outcnt,
		}
		node.Stats = &plan.Stats{
			Outcnt:       math.Min(leftStats.Outcnt, rightStats.Outcnt) * 0.7,
			Cost:         leftStats.Outcnt + rightStats.Outcnt,
			HashmapStats: hashmapStats,
			Selectivity:  1,
		}
	case plan.Node_MINUS:
		minus := math.Max(leftStats.Outcnt, rightStats.Outcnt) - math.Min(leftStats.Outcnt, rightStats.Outcnt)
		hashmapStats := &plan.HashMapStats{
			HashmapSize: rightStats.Outcnt,
		}
		node.Stats = &plan.Stats{
			Outcnt:       minus * 0.5,
			Cost:         leftStats.Outcnt + rightStats.Outcnt,
			HashmapStats: hashmapStats,
			Selectivity:  1,
		}
	case plan.Node_MINUS_ALL:
		minus := math.Max(leftStats.Outcnt, rightStats.Outcnt) - math.Min(leftStats.Outcnt, rightStats.Outcnt)
		hashmapStats := &plan.HashMapStats{
			HashmapSize: rightStats.Outcnt,
		}
		node.Stats = &plan.Stats{
			Outcnt:       minus * 0.7,
			Cost:         leftStats.Outcnt + rightStats.Outcnt,
			HashmapStats: hashmapStats,
			Selectivity:  1,
		}

	case plan.Node_VALUE_SCAN:
		node.Stats = DefaultStats()
		/*
			if node.RowsetData == nil {
				node.Stats = DefaultStats()
			} else {
				colsData := node.RowsetData.Cols
				rowCount := float64(len(colsData[0].Data))
				blockNumber := rowCount/float64(options.DefaultBlockMaxRows) + 1
				node.Stats = &plan.Stats{
					TableCnt:    (rowCount),
					BlockNum:    int32(blockNumber),
					Outcnt:      rowCount,
					Cost:        rowCount,
					Selectivity: 1,
				}
			}
		*/
	case plan.Node_SINK_SCAN:
		node.Stats = builder.qry.Nodes[node.GetSourceStep()[0]].Stats

	case plan.Node_RECURSIVE_SCAN:
		node.Stats = builder.qry.Nodes[node.GetSourceStep()[0]].Stats

	case plan.Node_EXTERNAL_SCAN:
		//calc for external scan is heavy, avoid recalc of this
		if node.Stats == nil || node.Stats.TableCnt == 0 {
			node.Stats = getExternalStats(node, builder)
		}

	case plan.Node_TABLE_SCAN:
		//calc for scan is heavy. use leafNode to judge if scan need to recalculate
		if node.ObjRef != nil && leafNode {
			if len(node.BindingTags) > 0 {
				builder.tag2Table[node.BindingTags[0]] = node.TableDef
			}
			node.Stats = calcScanStats(node, builder)
		}

	case plan.Node_FILTER:
		//filters which can not push down to scan nodes. hard to estimate selectivity
		node.Stats = &plan.Stats{
			Outcnt:      childStats.Outcnt * 0.05,
			Cost:        childStats.Cost,
			Selectivity: 0.05,
		}

	default:
		if len(node.Children) > 0 && childStats != nil {
			node.Stats = &plan.Stats{
				Outcnt:      childStats.Outcnt,
				Cost:        childStats.Outcnt,
				Selectivity: childStats.Selectivity,
			}
		} else if node.Stats == nil {
			node.Stats = DefaultStats()
		}
	}
}

func foldTableScanFilters(proc *process.Process, qry *Query, nodeId int32) error {
	node := qry.Nodes[nodeId]
	if node.NodeType == plan.Node_TABLE_SCAN && len(node.FilterList) > 0 {
		for i, e := range node.FilterList {
			foldedExpr, err := ConstantFold(batch.EmptyForConstFoldBatch, e, proc, false)
			if err != nil {
				return err
			}
			node.FilterList[i] = foldedExpr
		}
	}
	for _, childId := range node.Children {
		err := foldTableScanFilters(proc, qry, childId)
		if err != nil {
			return err
		}
	}
	return nil
}

func calcScanStats(node *plan.Node, builder *QueryBuilder) *plan.Stats {
	if !needStats(node.TableDef) {
		return DefaultStats()
	}
	if !builder.compCtx.Stats(node.ObjRef) {
		return DefaultStats()
	}
	//get statsInfoMap from statscache
	sc := builder.compCtx.GetStatsCache()
	if sc == nil {
		return DefaultStats()
	}
	s := sc.GetStatsInfoMap(node.TableDef.TblId)

	stats := new(plan.Stats)
	stats.TableCnt = s.TableCnt
	var blockSel float64 = 1

	var blockExprList []*plan.Expr
	for i := range node.FilterList {
		fixColumnName(node.TableDef, node.FilterList[i])
		node.FilterList[i].Selectivity = estimateExprSelectivity(node.FilterList[i], builder)
		currentBlockSel := estimateFilterBlockSelectivity(builder.GetContext(), node.FilterList[i], node.TableDef, builder)
		if currentBlockSel < blockSelectivityThreshHold {
			copyOfExpr := DeepCopyExpr(node.FilterList[i])
			copyOfExpr.Selectivity = currentBlockSel
			blockExprList = append(blockExprList, copyOfExpr)
		}
		blockSel = andSelectivity(blockSel, currentBlockSel)
	}
	node.BlockFilterList = blockExprList
	expr := rewriteFiltersForStats(node.FilterList, builder.compCtx.GetProcess())
	stats.Selectivity = estimateExprSelectivity(expr, builder)
	stats.Outcnt = stats.Selectivity * stats.TableCnt
	stats.Cost = stats.TableCnt * blockSel
	stats.BlockNum = int32(float64(s.BlockNumber)*blockSel) + 1
	return stats
}

func needStats(tableDef *TableDef) bool {
	switch tableDef.TblId {
	case catalog.MO_DATABASE_ID, catalog.MO_TABLES_ID, catalog.MO_COLUMNS_ID:
		return false
	}
	switch tableDef.Name {
	case "sys_async_task", "sys_cron_task":
		return false
	}
	if strings.HasPrefix(tableDef.Name, "mo_") || strings.HasPrefix(tableDef.Name, "__mo_") {
		return false
	}
	return true
}

func DefaultHugeStats() *plan.Stats {
	stats := new(Stats)
	stats.TableCnt = 10000000
	stats.Cost = 10000000
	stats.Outcnt = 10000000
	stats.Selectivity = 1
	stats.BlockNum = 1000
	return stats
}

func DefaultStats() *plan.Stats {
	stats := new(Stats)
	stats.TableCnt = 1000
	stats.Cost = 1000
	stats.Outcnt = 1000
	stats.Selectivity = 1
	stats.BlockNum = 1
	return stats
}

func (builder *QueryBuilder) applySwapRuleByStats(nodeID int32, recursive bool) {
	node := builder.qry.Nodes[nodeID]
	if recursive && len(node.Children) > 0 {
		for _, child := range node.Children {
			builder.applySwapRuleByStats(child, recursive)
		}
	}
	if node.NodeType != plan.Node_JOIN {
		return
	}

	leftChild := builder.qry.Nodes[node.Children[0]]
	rightChild := builder.qry.Nodes[node.Children[1]]
	if rightChild.NodeType == plan.Node_FUNCTION_SCAN {
		return
	}

	switch node.JoinType {
	case plan.Node_INNER, plan.Node_OUTER:
		if leftChild.Stats.Outcnt < rightChild.Stats.Outcnt {
			node.Children[0], node.Children[1] = node.Children[1], node.Children[0]

		}

	case plan.Node_LEFT, plan.Node_SEMI, plan.Node_ANTI:
		//right joins does not support non equal join for now
		if builder.IsEquiJoin(node) && leftChild.Stats.Outcnt < rightChild.Stats.Outcnt && !builder.haveOnDuplicateKey {
			node.BuildOnLeft = true
		}
	}

	if builder.hasRecursiveScan(builder.qry.Nodes[node.Children[1]]) {
		node.Children[0], node.Children[1] = node.Children[1], node.Children[0]
	}
}

func (builder *QueryBuilder) hasRecursiveScan(node *plan.Node) bool {
	if node.NodeType == plan.Node_RECURSIVE_SCAN {
		return true
	}
	for _, nodeID := range node.Children {
		if builder.hasRecursiveScan(builder.qry.Nodes[nodeID]) {
			return true
		}
	}
	return false
}

func compareStats(stats1, stats2 *Stats) bool {
	// selectivity is first considered to reduce data
	// when selectivity very close, we first join smaller table
	if math.Abs(stats1.Selectivity-stats2.Selectivity) > 0.01 {
		return stats1.Selectivity < stats2.Selectivity
	} else {
		// todo we need to calculate ndv of outcnt here
		return stats1.Outcnt < stats2.Outcnt
	}
}

func andSelectivity(s1, s2 float64) float64 {
	if s1 > 0.15 || s2 > 0.15 || s1*s2 > 0.1 {
		return s1 * s2
	}
	return math.Min(s1, s2) * math.Max(math.Pow(s1, s2), math.Pow(s2, s1))
}

func orSelectivity(s1, s2 float64) float64 {
	var s float64
	if math.Abs(s1-s2) < 0.001 && s1 < 0.2 {
		s = s1 + s2
	} else {
		s = math.Max(s1, s2) * 1.5
	}
	if s > 1 {
		return 1
	} else {
		return s
	}
}

const blockThresholdForTpQuery = 16

func IsTpQuery(qry *plan.Query) bool {
	for _, node := range qry.GetNodes() {
		stats := node.Stats
		if stats == nil || stats.BlockNum > blockThresholdForTpQuery {
			return false
		}
	}
	return true
}

func ReCalcQueryStats(builder *QueryBuilder, query *plan.Query) {
	for _, rootID := range builder.qry.Steps {
		ReCalcNodeStats(rootID, builder, true, false)
	}
}

func PrintStats(qry *plan.Query) string {
	buf := bytes.NewBuffer(make([]byte, 0, 1024*64))
	buf.WriteString("Print Stats: \n")
	for _, node := range qry.GetNodes() {
		stats := node.Stats
		buf.WriteString(fmt.Sprintf("Node ID: %v, Node Type %v, ", node.NodeId, node.NodeType))
		if stats == nil {
			buf.WriteString("Stats: nil\n")
		} else {
			buf.WriteString(fmt.Sprintf("blocknum %v, outcnt %v \n", node.Stats.BlockNum, node.Stats.Outcnt))
		}
	}
	return buf.String()
}

func DeepCopyStats(stats *plan.Stats) *plan.Stats {
	if stats == nil {
		return nil
	}
	var hashmapStats *plan.HashMapStats
	if stats.HashmapStats != nil {
		hashmapStats = &plan.HashMapStats{
			HashmapSize:   stats.HashmapStats.HashmapSize,
			HashOnPK:      stats.HashmapStats.HashOnPK,
			Shuffle:       stats.HashmapStats.Shuffle,
			ShuffleColIdx: stats.HashmapStats.ShuffleColIdx,
			ShuffleType:   stats.HashmapStats.ShuffleType,
			ShuffleColMin: stats.HashmapStats.ShuffleColMin,
			ShuffleColMax: stats.HashmapStats.ShuffleColMax,
			ShuffleMethod: stats.HashmapStats.ShuffleMethod,
		}
	}
	return &plan.Stats{
		BlockNum:     stats.BlockNum,
		Rowsize:      stats.Rowsize,
		Cost:         stats.Cost,
		Outcnt:       stats.Outcnt,
		TableCnt:     stats.TableCnt,
		Selectivity:  stats.Selectivity,
		HashmapStats: hashmapStats,
	}
}
