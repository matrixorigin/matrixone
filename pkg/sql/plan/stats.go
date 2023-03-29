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
	"context"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"math"
	"sort"
	"strings"
)

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
	if s, ok := (sc.cachePool)[tableID]; ok {
		return s
	} else {
		s = NewStatsInfoMap()
		(sc.cachePool)[tableID] = s
		return s
	}
}

type InfoFromZoneMap struct {
	ValMap         []map[any]int // all distinct value in blocks zonemap
	MinVal         []any         //minvalue of all blocks for column
	MaxVal         []any         //maxvalue of all blocks for column
	DataTypes      []types.Type
	MaybeUniqueMap []bool
}

func NewInfoFromZoneMap(lenCols, blockNumTotal int) *InfoFromZoneMap {
	info := &InfoFromZoneMap{
		ValMap:         make([]map[any]int, lenCols),
		MinVal:         make([]any, lenCols),
		MaxVal:         make([]any, lenCols),
		DataTypes:      make([]types.Type, lenCols),
		MaybeUniqueMap: make([]bool, lenCols),
	}
	for i := 0; i < lenCols; i++ {
		info.ValMap[i] = make(map[any]int, blockNumTotal)
	}
	return info
}

func GetHighNDVColumns(s *StatsInfoMap, b *Binding) []int32 {
	cols := make([]int32, 0)
	if s.TableCnt != 0 {
		for colName, ndv := range s.NdvMap {
			if ndv/s.TableCnt > 0.99 {
				cols = append(cols, b.FindColumn(colName))
			}
		}
	}
	return cols
}

func UpdateStatsInfoMap(info *InfoFromZoneMap, columns []int, blockNumTotal int, tableCnt float64, tableDef *plan.TableDef, s *StatsInfoMap) {
	logutil.Infof("need to update statsCache for table %v", tableDef.Name)
	s.BlockNumber = blockNumTotal
	s.TableCnt = tableCnt
	s.tableName = tableDef.Name
	//calc ndv with min,max,distinct value in zonemap, blocknumer and column type
	//set info in statsInfoMap
	for i := range columns {
		colName := tableDef.Cols[columns[i]].Name
		s.NdvMap[colName] = calcNdv(info.MinVal[i], info.MaxVal[i], float64(len(info.ValMap[i])), float64(blockNumTotal), tableCnt, info.DataTypes[i], info.MaybeUniqueMap[i])
		s.DataTypeMap[colName] = info.DataTypes[i].Oid
		switch info.DataTypes[i].Oid {
		case types.T_int8:
			s.MinValMap[colName] = float64(info.MinVal[i].(int8))
			s.MaxValMap[colName] = float64(info.MaxVal[i].(int8))
		case types.T_int16:
			s.MinValMap[colName] = float64(info.MinVal[i].(int16))
			s.MaxValMap[colName] = float64(info.MaxVal[i].(int16))
		case types.T_int32:
			s.MinValMap[colName] = float64(info.MinVal[i].(int32))
			s.MaxValMap[colName] = float64(info.MaxVal[i].(int32))
		case types.T_int64:
			s.MinValMap[colName] = float64(info.MinVal[i].(int64))
			s.MaxValMap[colName] = float64(info.MaxVal[i].(int64))
		case types.T_uint8:
			s.MinValMap[colName] = float64(info.MinVal[i].(uint8))
			s.MaxValMap[colName] = float64(info.MaxVal[i].(uint8))
		case types.T_uint16:
			s.MinValMap[colName] = float64(info.MinVal[i].(uint16))
			s.MaxValMap[colName] = float64(info.MaxVal[i].(uint16))
		case types.T_uint32:
			s.MinValMap[colName] = float64(info.MinVal[i].(uint32))
			s.MaxValMap[colName] = float64(info.MaxVal[i].(uint32))
		case types.T_uint64:
			s.MinValMap[colName] = float64(info.MinVal[i].(uint64))
			s.MaxValMap[colName] = float64(info.MaxVal[i].(uint64))
		case types.T_date:
			s.MinValMap[colName] = float64(info.MinVal[i].(types.Date))
			s.MaxValMap[colName] = float64(info.MaxVal[i].(types.Date))
		}
	}
}

func MakeAllColumns(tableDef *plan.TableDef) []int {
	lenCols := len(tableDef.Cols)
	cols := make([]int, lenCols-1)
	for i := 0; i < lenCols-1; i++ {
		cols[i] = i
	}
	return cols
}

func estimateOutCntBySortOrder(tableCnt, cost float64, sortOrder int) float64 {
	if sortOrder == -1 {
		return cost
	}
	// coefficient is 0.1 when tableCnt equals cost, and 1 when tableCnt >> cost
	coefficient := math.Pow(0.1, cost/tableCnt)

	outCnt := cost * coefficient
	if sortOrder == 0 {
		return outCnt * 0.9
	} else if sortOrder == 1 {
		return outCnt * 0.7
	} else {
		return outCnt * 0.1
	}

}

func getColNdv(col *plan.ColRef, nodeID int32, builder *QueryBuilder) float64 {
	binding := builder.ctxByNode[nodeID].bindingByTag[col.RelPos]
	sc := builder.compCtx.GetStatsCache()
	if sc == nil {
		return -1
	}
	s := sc.GetStatsInfoMap(binding.tableID)
	return s.NdvMap[binding.cols[col.ColPos]]
}

func getExprNdv(expr *plan.Expr, ndvMap map[string]float64, nodeID int32, builder *QueryBuilder) float64 {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funcName := exprImpl.F.Func.ObjName
		switch funcName {
		case "=", ">", ">=", "<=", "<":
			//assume col is on the left side
			return getExprNdv(exprImpl.F.Args[0], ndvMap, nodeID, builder)
		case "year":
			return getExprNdv(exprImpl.F.Args[0], ndvMap, nodeID, builder) / 365
		default:
			return -1
		}
	case *plan.Expr_Col:
		if ndvMap != nil {
			return ndvMap[exprImpl.Col.Name]
		}
		return getColNdv(exprImpl.Col, nodeID, builder)
	}
	return -1
}

func estimateOutCntForEquality(expr *plan.Expr, sortKeyName string, tableCnt, cost float64, ndvMap map[string]float64) float64 {
	// only filter like func(col)>1 , or (col=1) or (col=2) can estimate outcnt
	// and only 1 colRef is allowd in the filter. otherwise, no good method to calculate
	ret, col := CheckFilter(expr)
	if !ret {
		return cost / 100
	}
	sortOrder := util.GetClusterByColumnOrder(sortKeyName, col.Name)
	//if col is clusterby, we assume most of the rows in blocks we read is needed
	//otherwise, deduce selectivity according to ndv
	if sortOrder != -1 {
		return estimateOutCntBySortOrder(tableCnt, cost, sortOrder)
	} else {
		ndv := getExprNdv(expr, ndvMap, 0, nil)
		if ndv > 0 {
			return tableCnt / ndv
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

func estimateOutCntForNonEquality(expr *plan.Expr, funcName, sortKeyName string, tableCnt, cost float64, s *StatsInfoMap) float64 {
	// only filter like func(col)>1 , or (col=1) or (col=2) can estimate outcnt
	// and only 1 colRef is allowd in the filter. otherwise, no good method to calculate
	ret, col := CheckFilter(expr)
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
		ret, col, constExpr, _ := CheckStrictFilter(expr)
		if ret {
			switch s.DataTypeMap[col.Name] {
			case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
				if val, valOk := constExpr.Value.(*plan.Const_I64Val); valOk {
					return calcOutCntByMinMax(funcName, tableCnt, s.MinValMap[col.Name], s.MaxValMap[col.Name], float64(val.I64Val))
				}
			case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
				if val, valOk := constExpr.Value.(*plan.Const_U64Val); valOk {
					return calcOutCntByMinMax(funcName, tableCnt, s.MinValMap[col.Name], s.MaxValMap[col.Name], float64(val.U64Val))
				}
			case types.T_date:
				if val, valOk := constExpr.Value.(*plan.Const_Dateval); valOk {
					return calcOutCntByMinMax(funcName, tableCnt, s.MinValMap[col.Name], s.MaxValMap[col.Name], float64(val.Dateval))
				}
			}
		}
	}
	return cost / 2
}

// estimate output lines for a filter
func EstimateOutCnt(expr *plan.Expr, sortKeyName string, tableCnt, cost float64, s *StatsInfoMap) float64 {
	if expr == nil {
		return cost
	}
	var outcnt float64
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funcName := exprImpl.F.Func.ObjName
		switch funcName {
		case "=":
			outcnt = estimateOutCntForEquality(expr, sortKeyName, tableCnt, cost, s.NdvMap)
		case ">", "<", ">=", "<=":
			//for filters like a>1, no good way to estimate, return 3 * equality
			outcnt = estimateOutCntForNonEquality(expr, funcName, sortKeyName, tableCnt, cost, s)
		case "and":
			//get the smaller one of two children, and tune it down a little bit
			out1 := EstimateOutCnt(exprImpl.F.Args[0], sortKeyName, tableCnt, cost, s)
			out2 := EstimateOutCnt(exprImpl.F.Args[1], sortKeyName, tableCnt, cost, s)
			if canMergeToBetweenAnd(exprImpl.F.Args[0], exprImpl.F.Args[1]) && (out1+out2) > tableCnt {
				outcnt = (out1 + out2) - tableCnt
			} else {
				outcnt = math.Min(out1, out2) * 0.8
			}
		case "or":
			//get the bigger one of two children, and tune it up a little bit
			out1 := EstimateOutCnt(exprImpl.F.Args[0], sortKeyName, tableCnt, cost, s)
			out2 := EstimateOutCnt(exprImpl.F.Args[1], sortKeyName, tableCnt, cost, s)
			if out1 == out2 {
				outcnt = out1 + out2
			} else {
				outcnt = math.Max(out1, out2) * 1.5
			}
		default:
			//no good way to estimate, just 0.1*cost
			outcnt = cost * 0.1
		}
	case *plan.Expr_C:
		outcnt = cost
	}
	if outcnt > cost {
		//outcnt must be smaller than cost
		outcnt = cost
	} else if outcnt < 1 {
		outcnt = 1
	}
	return outcnt
}

func calcNdv(minVal, maxVal any, distinctValNum, blockNumTotal, tableCnt float64, t types.Type, maybeUnique bool) float64 {
	ndv1 := calcNdvUsingMinMax(minVal, maxVal, t)
	ndv2 := calcNdvUsingDistinctValNum(distinctValNum, blockNumTotal, tableCnt)
	if ndv1 <= 0 {
		return ndv2
	}
	if ndv1 > tableCnt {
		ndv1 = tableCnt
	}
	if maybeUnique {
		return ndv1
	} else {
		return ndv1 / 2
	}
}

// treat distinct val in zonemap like a sample , then estimate the ndv
// more blocks, more accurate
func calcNdvUsingDistinctValNum(distinctValNum, blockNumTotal, tableCnt float64) (ndv float64) {
	// coefficient is 0.15 when 1 block, and 1 when many blocks.
	coefficient := math.Pow(0.15, (1 / math.Log2(blockNumTotal*2)))
	ndvRate := (distinctValNum / blockNumTotal)
	if distinctValNum <= 100 || ndvRate < 0.4 {
		// very little distinctValNum, assume ndv is very low
		ndv = (distinctValNum + 2) / coefficient
	} else {
		// assume ndv is high
		ndv = tableCnt * ndvRate * coefficient
		if ndv < 1 {
			ndv = 1
		}
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
		return types.Decimal64ToFloat64(maxVal.(types.Decimal64), t.Scale) - types.Decimal64ToFloat64(minVal.(types.Decimal64), t.Scale) + 1
	case types.T_decimal128:
		return types.Decimal128ToFloat64(maxVal.(types.Decimal128), t.Scale) - types.Decimal128ToFloat64(minVal.(types.Decimal128), t.Scale) + 1
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

func estimateFilterWeight(ctx context.Context, expr *plan.Expr, w float64) float64 {
	switch expr.Typ.Id {
	case int32(types.T_decimal64):
		w += 64
	case int32(types.T_decimal128):
		w += 128
	case int32(types.T_char), int32(types.T_varchar), int32(types.T_text), int32(types.T_json):
		w += 4
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funcImpl := exprImpl.F
		switch funcImpl.Func.GetObjName() {
		case "like":
			w += 10
		case "in":
			w += 5
		case "<", "<=":
			w += 1.1
		default:
			w += 1
		}
		for _, child := range exprImpl.F.Args {
			w += estimateFilterWeight(ctx, child, 0)
		}
	}
	if CheckExprIsMonotonic(ctx, expr) {
		//this is a monotonic filter
		//calc selectivity is too heavy now. will change this in the future
		w *= 0.1
	}
	return w
}

func SortFilterListByStats(ctx context.Context, nodeID int32, builder *QueryBuilder) {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			SortFilterListByStats(ctx, child, builder)
		}
	}
	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		if node.ObjRef != nil && len(node.FilterList) > 1 {
			bat := batch.NewWithSize(0)
			bat.Zs = []int64{1}
			for i := range node.FilterList {
				expr, _ := ConstantFold(bat, DeepCopyExpr(node.FilterList[i]), builder.compCtx.GetProcess())
				if expr != nil {
					node.FilterList[i] = expr
				}
			}
			sort.Slice(node.FilterList, func(i, j int) bool {
				return estimateFilterWeight(builder.GetContext(), node.FilterList[i], 0) <= estimateFilterWeight(builder.GetContext(), node.FilterList[j], 0)
			})
		}
	}
}

func ReCalcNodeStats(nodeID int32, builder *QueryBuilder, recursive bool) {
	node := builder.qry.Nodes[nodeID]
	if recursive {
		if len(node.Children) > 0 {
			for _, child := range node.Children {
				ReCalcNodeStats(child, builder, recursive)
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

		switch node.JoinType {
		case plan.Node_INNER:
			outcnt := leftStats.Outcnt * rightStats.Outcnt / ndv
			if !isCrossJoin {
				outcnt *= selectivity
			}
			node.Stats = &plan.Stats{
				Outcnt:      outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
				Selectivity: selectivity_out,
			}

		case plan.Node_LEFT:
			outcnt := leftStats.Outcnt * rightStats.Outcnt / ndv
			if !isCrossJoin {
				outcnt *= selectivity
				outcnt += leftStats.Outcnt
			}
			node.Stats = &plan.Stats{
				Outcnt:      outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
				Selectivity: selectivity_out,
			}

		case plan.Node_RIGHT:
			outcnt := leftStats.Outcnt * rightStats.Outcnt / ndv
			if !isCrossJoin {
				outcnt *= selectivity
				outcnt += rightStats.Outcnt
			}
			node.Stats = &plan.Stats{
				Outcnt:      outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
				Selectivity: selectivity_out,
			}

		case plan.Node_OUTER:
			outcnt := leftStats.Outcnt * rightStats.Outcnt / ndv
			if !isCrossJoin {
				outcnt *= selectivity
				outcnt += leftStats.Outcnt + rightStats.Outcnt
			}
			node.Stats = &plan.Stats{
				Outcnt:      outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
				Selectivity: selectivity_out,
			}

		case plan.Node_SEMI, plan.Node_ANTI:
			node.Stats = &plan.Stats{
				Outcnt:      leftStats.Outcnt * selectivity,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
				Selectivity: selectivity_out,
			}

		case plan.Node_SINGLE, plan.Node_MARK:
			node.Stats = &plan.Stats{
				Outcnt:      leftStats.Outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
				Selectivity: selectivity_out,
			}
		}

	case plan.Node_AGG:
		if len(node.GroupBy) > 0 {
			input := childStats.Outcnt
			output := 1.0
			for _, groupby := range node.GroupBy {
				output *= getExprNdv(groupby, nil, node.NodeId, builder)
			}
			if output > input {
				output = input
			}
			node.Stats = &plan.Stats{
				Outcnt:      output,
				Cost:        input,
				HashmapSize: output,
				Selectivity: 1,
			}
		} else {
			node.Stats = &plan.Stats{
				Outcnt:      1,
				Cost:        childStats.Cost,
				Selectivity: 1,
			}
		}

	case plan.Node_UNION:
		node.Stats = &plan.Stats{
			Outcnt:      (leftStats.Outcnt + rightStats.Outcnt) * 0.7,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
			Selectivity: 1,
		}
	case plan.Node_UNION_ALL:
		node.Stats = &plan.Stats{
			Outcnt:      leftStats.Outcnt + rightStats.Outcnt,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			Selectivity: 1,
		}
	case plan.Node_INTERSECT:
		node.Stats = &plan.Stats{
			Outcnt:      math.Min(leftStats.Outcnt, rightStats.Outcnt) * 0.5,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
			Selectivity: 1,
		}
	case plan.Node_INTERSECT_ALL:
		node.Stats = &plan.Stats{
			Outcnt:      math.Min(leftStats.Outcnt, rightStats.Outcnt) * 0.7,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
			Selectivity: 1,
		}
	case plan.Node_MINUS:
		minus := math.Max(leftStats.Outcnt, rightStats.Outcnt) - math.Min(leftStats.Outcnt, rightStats.Outcnt)
		node.Stats = &plan.Stats{
			Outcnt:      minus * 0.5,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
			Selectivity: 1,
		}
	case plan.Node_MINUS_ALL:
		minus := math.Max(leftStats.Outcnt, rightStats.Outcnt) - math.Min(leftStats.Outcnt, rightStats.Outcnt)
		node.Stats = &plan.Stats{
			Outcnt:      minus * 0.7,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
			Selectivity: 1,
		}

	case plan.Node_TABLE_SCAN:
		if node.ObjRef != nil {
			expr, num := HandleFiltersForZM(node.FilterList, builder.compCtx.GetProcess())
			node.Stats = builder.compCtx.Stats(node.ObjRef, expr)

			//if there is non monotonic filters
			if num > 0 {
				node.Stats.Selectivity *= 0.15
				node.Stats.Outcnt *= 0.15
			}
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

func NeedStats(tableDef *TableDef) bool {
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

func DefaultStats() *plan.Stats {
	stats := new(Stats)
	stats.TableCnt = 1000
	stats.Cost = 1000
	stats.Outcnt = 1000
	stats.Selectivity = 1
	stats.BlockNum = 1
	return stats
}
