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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const DefaultBlockMaxRows = 8192
const blockThresholdForTpQuery = 32
const costThresholdForTpQuery = 240000
const highNDVcolumnThreshHold = 0.95
const statsCacheInitSize = 128
const statsCacheMaxSize = 8192

var ncpu = runtime.GOMAXPROCS(0)
var BlockThresholdForOneCN = ncpu * blockThresholdForTpQuery
var costThresholdForOneCN = ncpu * costThresholdForTpQuery

type ExecType int

const (
	ExecTypeTP ExecType = iota
	ExecTypeAP_ONECN
	ExecTypeAP_MULTICN
)

type StatsCache struct {
	cache map[uint64]*pb.StatsInfo
}

func NewStatsCache() *StatsCache {
	return &StatsCache{
		cache: make(map[uint64]*pb.StatsInfo, statsCacheInitSize),
	}
}

// GetStatsInfo returns the stats info and if the info in the cache needs to be updated.
func (sc *StatsCache) GetStatsInfo(tableID uint64, create bool) *pb.StatsInfo {
	if sc == nil {
		return nil
	}
	if s, ok := sc.cache[tableID]; ok {
		return s
	}
	if create {
		if len(sc.cache) > statsCacheMaxSize {
			sc.cache = make(map[uint64]*pb.StatsInfo, statsCacheInitSize)
			logutil.Infof("statscache entries more than %v in long session, release memory and create new cachepool", statsCacheMaxSize)
		}
		s := NewStatsInfo()
		sc.cache[tableID] = s
		return s
	} else {
		return nil
	}
}

// SetStatsInfo updates the stats info in the cache.
func (sc *StatsCache) SetStatsInfo(tableID uint64, s *pb.StatsInfo) {
	if sc == nil {
		return
	}
	sc.cache[tableID] = s
}

func NewStatsInfo() *pb.StatsInfo {
	return &pb.StatsInfo{
		NdvMap:             make(map[string]float64),
		MinValMap:          make(map[string]float64),
		MaxValMap:          make(map[string]float64),
		DataTypeMap:        make(map[string]uint64),
		NullCntMap:         make(map[string]uint64),
		SizeMap:            make(map[string]uint64),
		ShuffleRangeMap:    make(map[string]*pb.ShuffleRange),
		BlockNumber:        0,
		ApproxObjectNumber: 0,
		TableCnt:           0,
	}
}

type InfoFromZoneMap struct {
	ColumnZMs            []objectio.ZoneMap
	DataTypes            []types.Type
	ColumnNDVs           []float64
	NullCnts             []int64
	ShuffleRanges        []*pb.ShuffleRange
	ColumnSize           []int64
	BlockNumber          int64
	AccurateObjectNumber int64
	ApproxObjectNumber   int64
	TableCnt             float64
}

func NewInfoFromZoneMap(lenCols int) *InfoFromZoneMap {
	info := &InfoFromZoneMap{
		ColumnZMs:     make([]objectio.ZoneMap, lenCols),
		DataTypes:     make([]types.Type, lenCols),
		ColumnNDVs:    make([]float64, lenCols),
		NullCnts:      make([]int64, lenCols),
		ColumnSize:    make([]int64, lenCols),
		ShuffleRanges: make([]*pb.ShuffleRange, lenCols),
	}
	return info
}

func UpdateStatsInfo(info *InfoFromZoneMap, tableDef *plan.TableDef, s *pb.StatsInfo) {
	start := time.Now()
	defer func() {
		v2.TxnStatementUpdateStatsInfoMapHistogram.Observe(time.Since(start).Seconds())
	}()
	s.ApproxObjectNumber = info.ApproxObjectNumber
	s.AccurateObjectNumber = info.AccurateObjectNumber
	s.BlockNumber = info.BlockNumber
	s.TableCnt = info.TableCnt
	s.TableName = tableDef.Name
	//calc ndv with min,max,distinct value in zonemap, blocknumer and column type
	//set info in statsInfo
	for i, coldef := range tableDef.Cols[:len(tableDef.Cols)-1] {
		colName := coldef.Name
		s.NdvMap[colName] = info.ColumnNDVs[i]
		s.DataTypeMap[colName] = uint64(info.DataTypes[i].Oid)
		s.NullCntMap[colName] = uint64(info.NullCnts[i])
		s.SizeMap[colName] = uint64(info.ColumnSize[i])

		if !info.ColumnZMs[i].IsInited() {
			s.MinValMap[colName] = 0
			s.MaxValMap[colName] = 0
			continue
		}
		switch info.DataTypes[i].Oid {
		case types.T_bit:
			s.MinValMap[colName] = float64(types.DecodeUint64(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeUint64(info.ColumnZMs[i].GetMaxBuf()))
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
		case types.T_time:
			s.MinValMap[colName] = float64(types.DecodeTime(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeTime(info.ColumnZMs[i].GetMaxBuf()))
		case types.T_timestamp:
			s.MinValMap[colName] = float64(types.DecodeTimestamp(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeTimestamp(info.ColumnZMs[i].GetMaxBuf()))
		case types.T_datetime:
			s.MinValMap[colName] = float64(types.DecodeDatetime(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeDatetime(info.ColumnZMs[i].GetMaxBuf()))
		case types.T_char, types.T_varchar, types.T_text, types.T_datalink:
			s.MinValMap[colName] = float64(ByteSliceToUint64(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(ByteSliceToUint64(info.ColumnZMs[i].GetMaxBuf()))
		}

		if info.ShuffleRanges[i] != nil {
			if s.MinValMap[colName] != s.MaxValMap[colName] &&
				s.TableCnt > threshHoldForRangeShuffle &&
				info.ColumnNDVs[i] >= ShuffleThreshHoldOfNDV &&
				!util.JudgeIsCompositeClusterByColumn(colName) &&
				colName != catalog.CPrimaryKeyColName {
				info.ShuffleRanges[i].Eval()
				info.ShuffleRanges[i].ReleaseUnused()
				s.ShuffleRangeMap[colName] = info.ShuffleRanges[i]
			}
			info.ShuffleRanges[i] = nil
		}
	}
}

// cols in one table, return if ndv of  multi column is high enough
func isHighNdvCols(cols []int32, tableDef *TableDef, builder *QueryBuilder) bool {
	if tableDef == nil {
		return false
	}
	// first to check if it is primary key.
	if containsAllPKs(cols, tableDef) {
		return true
	}

	s := builder.getStatsInfoByTableID(tableDef.TblId)
	if s == nil {
		return false
	}
	var totalNDV float64 = 1
	for i := range cols {
		totalNDV *= s.NdvMap[tableDef.Cols[cols[i]].Name]
	}
	return totalNDV > s.TableCnt*highNDVcolumnThreshHold
}

func (builder *QueryBuilder) getColNDVRatio(cols []int32, tableDef *TableDef) float64 {
	if tableDef == nil {
		return 0
	}
	// first to check if it is primary key.
	if containsAllPKs(cols, tableDef) {
		return 1
	}

	s := builder.getStatsInfoByTableID(tableDef.TblId)
	if s == nil {
		return 0
	}
	var totalNDV float64 = 1
	for i := range cols {
		totalNDV *= s.NdvMap[tableDef.Cols[cols[i]].Name]
	}
	result := totalNDV / s.TableCnt
	if result > 1 {
		result = 1
	}
	return result
}

func (builder *QueryBuilder) getStatsInfoByTableID(tableID uint64) *pb.StatsInfo {
	if builder == nil {
		return nil
	}
	sc := builder.compCtx.GetStatsCache()
	if sc == nil {
		return nil
	}
	return sc.GetStatsInfo(tableID, false)
}

func (builder *QueryBuilder) getStatsInfoByCol(col *plan.ColRef) *pb.StatsInfo {
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
	return sc.GetStatsInfo(tableDef.TblId, false)
}

func (builder *QueryBuilder) getColNdv(col *plan.ColRef) float64 {
	s := builder.getStatsInfoByCol(col)
	if s == nil {
		return -1
	}
	return s.NdvMap[col.Name]
}

func getNullSelectivity(arg *plan.Expr, builder *QueryBuilder, isnull bool) float64 {
	switch exprImpl := arg.Expr.(type) {
	case *plan.Expr_Col:
		col := exprImpl.Col
		s := builder.getStatsInfoByCol(col)
		if s == nil {
			break
		}
		nullCnt := float64(s.NullCntMap[col.Name])
		if isnull {
			return nullCnt / s.TableCnt
		} else {
			return 1 - (nullCnt / s.TableCnt)
		}
	}

	if isnull {
		return 0.1
	} else {
		return 0.9
	}
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
		return builder.getColNdv(exprImpl.Col)
	}
	return -1
}

func estimateEqualitySelectivity(expr *plan.Expr, builder *QueryBuilder) float64 {
	// only filter like func(col)=1 or col=? can estimate outcnt
	// and only 1 colRef is allowd in the filter. otherwise, no good method to calculate
	col := extractColRefInFilter(expr)
	if col == nil {
		return 0.01
	}
	ndv := getExprNdv(expr, builder)
	if ndv > 0 {
		return 1 / ndv
	}
	return 0.01
}

func calcSelectivityByMinMax(funcName string, min, max float64, typ types.T, vals []*plan.Literal) (ret float64) {
	switch funcName {
	case ">", ">=":
		if val, ok := getFloat64Value(typ, vals[0]); ok {
			ret = (max - val + 1) / (max - min)
		}
	case "<", "<=":
		if val, ok := getFloat64Value(typ, vals[0]); ok {
			ret = (val - min + 1) / (max - min)
		}
	case "between":
		if lb, ok := getFloat64Value(typ, vals[0]); ok {
			if ub, ok := getFloat64Value(typ, vals[1]); ok {
				ret = (ub - lb + 1) / (max - min)
			}
		}
	default:
		ret = 0.3
	}
	if ret < 0 {
		ret = 0
	}
	if ret > 1 {
		ret = 1
	}
	return ret
}

func getFloat64Value(typ types.T, lit *plan.Literal) (float64, bool) {
	switch typ {
	case types.T_float32:
		if val, valOk := lit.Value.(*plan.Literal_Fval); valOk {
			return float64(val.Fval), true
		}
	case types.T_float64:
		if val, valOk := lit.Value.(*plan.Literal_Dval); valOk {
			return val.Dval, true
		}
	case types.T_int8:
		if val, valOk := lit.Value.(*plan.Literal_I8Val); valOk {
			return float64(val.I8Val), true
		}
	case types.T_int16:
		if val, valOk := lit.Value.(*plan.Literal_I16Val); valOk {
			return float64(val.I16Val), true
		}
	case types.T_int32:
		if val, valOk := lit.Value.(*plan.Literal_I32Val); valOk {
			return float64(val.I32Val), true
		}
	case types.T_int64:
		if val, valOk := lit.Value.(*plan.Literal_I64Val); valOk {
			return float64(val.I64Val), true
		}
	case types.T_uint8:
		if val, valOk := lit.Value.(*plan.Literal_U8Val); valOk {
			return float64(val.U8Val), true
		}
	case types.T_uint16:
		if val, valOk := lit.Value.(*plan.Literal_U16Val); valOk {
			return float64(val.U16Val), true
		}
	case types.T_uint32:
		if val, valOk := lit.Value.(*plan.Literal_U32Val); valOk {
			return float64(val.U32Val), true
		}
	case types.T_uint64:
		if val, valOk := lit.Value.(*plan.Literal_U64Val); valOk {
			return float64(val.U64Val), true
		}
	case types.T_date:
		if val, valOk := lit.Value.(*plan.Literal_Dateval); valOk {
			return float64(val.Dateval), true
		}
	case types.T_datetime:
		if val, valOk := lit.Value.(*plan.Literal_Datetimeval); valOk {
			return float64(val.Datetimeval), true
		}
	}

	return 0, false
}

func estimateNonEqualitySelectivity(expr *plan.Expr, funcName string, builder *QueryBuilder) float64 {
	// only filter like func(col)>1 , or (col=1) or (col=2) can estimate outcnt
	// and only 1 colRef is allowd in the filter. otherwise, no good method to calculate
	col := extractColRefInFilter(expr)
	if col == nil {
		return 0.1
	}
	s := builder.getStatsInfoByCol(col)
	if s == nil {
		return 0.1
	}

	//check strict filter, otherwise can not estimate outcnt by min/max val
	col, litType, literals, colFnName, hasDynamicParam := extractColRefAndLiteralsInFilter(expr)
	if hasDynamicParam {
		// assume dynamic parameter always has low selectivity
		if funcName == "between" {
			return 0.0001
		} else {
			return 0.01
		}
	}
	if col != nil && len(literals) > 0 {
		typ := types.T(s.DataTypeMap[col.Name])
		if !(typ.IsInteger() || typ.IsDateRelate()) {
			return 0.1
		}

		switch colFnName {
		case "":
			return calcSelectivityByMinMax(funcName, s.MinValMap[col.Name], s.MaxValMap[col.Name], typ, literals)
		case "year":
			switch typ {
			case types.T_date:
				minVal := types.Date(s.MinValMap[col.Name])
				maxVal := types.Date(s.MaxValMap[col.Name])
				return calcSelectivityByMinMax(funcName, float64(minVal.Year()), float64(maxVal.Year()), litType, literals)
			case types.T_datetime:
				// TODO
			}
		}
	}

	return 0.1
}

func estimateExprSelectivity(expr *plan.Expr, builder *QueryBuilder) float64 {
	if expr == nil {
		return 1.0
	}
	if expr.Selectivity != 0 {
		return expr.Selectivity // already calculated
	}

	ret := 1.0
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funcName := exprImpl.F.Func.ObjName
		switch funcName {
		case "=":
			ret = estimateEqualitySelectivity(expr, builder)
		case "!=", "<>":
			ret = 0.9
		case ">", "<", ">=", "<=", "between":
			ret = estimateNonEqualitySelectivity(expr, funcName, builder)
		case "and":
			ret = estimateExprSelectivity(exprImpl.F.Args[0], builder)
			if len(exprImpl.F.Args) == 2 {
				sel2 := estimateExprSelectivity(exprImpl.F.Args[1], builder)
				if canMergeToBetweenAnd(exprImpl.F.Args[0], exprImpl.F.Args[1]) && (ret+sel2) > 1 {
					ret = ret + sel2 - 1
				} else {
					ret = andSelectivity(ret, sel2)
				}
			} else {
				for i := 1; i < len(exprImpl.F.Args); i++ {
					sel2 := estimateExprSelectivity(exprImpl.F.Args[i], builder)
					ret = andSelectivity(ret, sel2)
				}
			}
		case "or":
			ret = estimateExprSelectivity(exprImpl.F.Args[0], builder)
			for i := 1; i < len(exprImpl.F.Args); i++ {
				sel2 := estimateExprSelectivity(exprImpl.F.Args[i], builder)
				ret = orSelectivity(ret, sel2)
			}
		case "not":
			ret = 1 - estimateExprSelectivity(exprImpl.F.Args[0], builder)
		case "like":
			ret = 0.2
		case "prefix_eq":
			ret = 0.0001 // should never go here
		case "in", "prefix_in":
			card := 1.0
			switch arg1Impl := exprImpl.F.Args[1].Expr.(type) {
			case *plan.Expr_Vec:
				card = float64(arg1Impl.Vec.Len)
			case *plan.Expr_List:
				card = float64(len(arg1Impl.List.List))
			}
			if funcName == "prefix_in" {
				card *= 10
			}
			ndv := getExprNdv(expr, builder)
			if ndv > card {
				ret = card / ndv
			} else {
				ret = 0.5
			}
		case "prefix_between":
			ret = 0.1
		case "isnull", "is_null":
			ret = getNullSelectivity(exprImpl.F.Args[0], builder, true)
		case "isnotnull", "is_not_null":
			ret = getNullSelectivity(exprImpl.F.Args[0], builder, false)
		default:
			ret = 0.15
		}
	case *plan.Expr_Lit:
		ret = 1.0
	}
	expr.Selectivity = ret
	return ret
}

func estimateFilterWeight(expr *plan.Expr, w float64) float64 {
	switch expr.Typ.Id {
	case int32(types.T_decimal64):
		w += 64
	case int32(types.T_decimal128):
		w += 128
	case int32(types.T_float32), int32(types.T_float64):
		w += 8
	case int32(types.T_char), int32(types.T_varchar), int32(types.T_text), int32(types.T_json), int32(types.T_datalink):
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
func estimateFilterBlockSelectivity(ctx context.Context, expr *plan.Expr, tableDef *plan.TableDef, s *pb.StatsInfo) float64 {
	if !ExprIsZonemappable(ctx, expr) {
		return 1
	}
	col := extractColRefInFilter(expr)
	if col != nil {
		blocksel := calcBlockSelectivityUsingShuffleRange(s.ShuffleRangeMap[col.Name], expr)
		switch GetSortOrder(tableDef, col.ColPos) {
		case 0:
			blocksel = math.Min(blocksel, 0.2)
		case 1:
			return math.Min(blocksel, 0.5)
		case 2:
			return math.Min(blocksel, 0.7)
		}
		return blocksel
	}
	return 1
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

func ReCalcNodeStats(nodeID int32, builder *QueryBuilder, recursive bool, leafNode bool, needResetHashMapStats bool) {
	node := builder.qry.Nodes[nodeID]
	if recursive {
		if len(node.Children) > 0 {
			for _, child := range node.Children {
				ReCalcNodeStats(child, builder, recursive, leafNode, needResetHashMapStats)
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

	if node.Stats == nil {
		if node.NodeType != plan.Node_EXTERNAL_SCAN && node.NodeType != plan.Node_TABLE_SCAN {
			node.Stats = DefaultStats()
		}
	}

	switch node.NodeType {
	case plan.Node_JOIN:
		if needResetHashMapStats {
			resetHashMapStats(node.Stats)
		}

		ndv := math.Min(leftStats.Outcnt, rightStats.Outcnt)
		if ndv < 1 {
			ndv = 1
		}
		//assume all join is not cross join
		//will fix this in the future
		//isCrossJoin := (len(node.OnList) == 0)
		isCrossJoin := false
		selectivity := math.Pow(rightStats.Selectivity, math.Pow(leftStats.Selectivity, 0.2))
		selectivity_out := andSelectivity(leftStats.Selectivity, rightStats.Selectivity)

		for _, pred := range node.OnList {
			if pred.Ndv <= 0 {
				pred.Ndv = getExprNdv(pred, builder)
			}
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
			node.Stats.Outcnt = outcnt
			node.Stats.Cost = leftStats.Cost + rightStats.Cost
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity_out

		case plan.Node_LEFT:
			node.Stats.Outcnt = leftStats.Outcnt
			node.Stats.Cost = leftStats.Cost + rightStats.Cost
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity_out

		case plan.Node_RIGHT:
			node.Stats.Outcnt = rightStats.Outcnt
			node.Stats.Cost = leftStats.Cost + rightStats.Cost
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity_out

		case plan.Node_OUTER:
			node.Stats.Outcnt = leftStats.Outcnt + rightStats.Outcnt
			node.Stats.Cost = leftStats.Cost + rightStats.Cost
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity_out

		case plan.Node_SEMI, plan.Node_INDEX:
			node.Stats.Outcnt = leftStats.Outcnt * selectivity
			node.Stats.Cost = leftStats.Cost + rightStats.Cost
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity_out

		case plan.Node_ANTI:
			node.Stats.Outcnt = leftStats.Outcnt * (1 - rightStats.Selectivity) * 0.5
			node.Stats.Cost = leftStats.Cost + rightStats.Cost
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity_out

		case plan.Node_SINGLE, plan.Node_MARK:
			node.Stats.Outcnt = leftStats.Outcnt
			node.Stats.Cost = leftStats.Cost + rightStats.Cost
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity_out
		}
		node.Stats.BlockNum = leftStats.BlockNum

	case plan.Node_AGG:
		if needResetHashMapStats {
			resetHashMapStats(node.Stats)
		}
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
			node.Stats.Outcnt = outcnt
			node.Stats.Cost = incnt + outcnt
			node.Stats.HashmapStats.HashmapSize = outcnt
			node.Stats.Selectivity = 1
			if len(node.FilterList) > 0 {
				node.Stats.Outcnt *= 0.0001
				node.Stats.Selectivity *= 0.0001
			}
		} else {
			node.Stats.Outcnt = 1
			node.Stats.Cost = childStats.Cost
			node.Stats.HashmapStats.HashmapSize = 1
			node.Stats.Selectivity = 1
		}
		node.Stats.BlockNum = int32(childStats.Outcnt/DefaultBlockMaxRows) + 1

	case plan.Node_UNION:
		if needResetHashMapStats {
			resetHashMapStats(node.Stats)
		}
		node.Stats.Outcnt = (leftStats.Outcnt + rightStats.Outcnt) * 0.7
		node.Stats.Cost = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Selectivity = 1
		node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt

	case plan.Node_UNION_ALL:
		node.Stats.Outcnt = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Cost = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Selectivity = 1

	case plan.Node_INTERSECT:
		if needResetHashMapStats {
			resetHashMapStats(node.Stats)
		}
		node.Stats.Outcnt = math.Min(leftStats.Outcnt, rightStats.Outcnt) * 0.5
		node.Stats.Cost = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Selectivity = 1
		node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt

	case plan.Node_INTERSECT_ALL:
		if needResetHashMapStats {
			resetHashMapStats(node.Stats)
		}
		node.Stats.Outcnt = math.Min(leftStats.Outcnt, rightStats.Outcnt) * 0.7
		node.Stats.Cost = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Selectivity = 1
		node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt

	case plan.Node_MINUS:
		if needResetHashMapStats {
			resetHashMapStats(node.Stats)
		}
		minus := math.Max(leftStats.Outcnt, rightStats.Outcnt) - math.Min(leftStats.Outcnt, rightStats.Outcnt)
		node.Stats.Outcnt = minus * 0.5
		node.Stats.Cost = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Selectivity = 1
		node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt

	case plan.Node_MINUS_ALL:
		if needResetHashMapStats {
			resetHashMapStats(node.Stats)
		}
		minus := math.Max(leftStats.Outcnt, rightStats.Outcnt) - math.Min(leftStats.Outcnt, rightStats.Outcnt)
		node.Stats.Outcnt = minus * 0.7
		node.Stats.Cost = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Selectivity = 1
		node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt

	case plan.Node_VALUE_SCAN:
		if node.RowsetData != nil {
			rowCount := float64(node.RowsetData.RowCount)
			node.Stats.TableCnt = rowCount
			node.Stats.BlockNum = int32(rowCount/float64(options.DefaultBlockMaxRows) + 1)
			node.Stats.Cost = rowCount
			node.Stats.Outcnt = rowCount
			node.Stats.Selectivity = 1
		}

	case plan.Node_SINK_SCAN:
		sourceNode := builder.qry.Steps[node.GetSourceStep()[0]]
		node.Stats = builder.qry.Nodes[sourceNode].Stats

	case plan.Node_RECURSIVE_SCAN:
		sourceNode := builder.qry.Steps[node.GetSourceStep()[0]]
		node.Stats = builder.qry.Nodes[sourceNode].Stats

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
			newStats := calcScanStats(node, builder)
			if needResetHashMapStats {
				resetHashMapStats(newStats)
			}
			node.Stats = newStats
		}

	case plan.Node_FILTER:
		//filters which can not push down to scan nodes. hard to estimate selectivity
		node.Stats.Outcnt = childStats.Outcnt * 0.05
		if node.Stats.Outcnt < 1 {
			node.Stats.Outcnt = 1
		}
		node.Stats.Cost = childStats.Cost
		node.Stats.Selectivity = 0.05

	case plan.Node_FUNCTION_SCAN:
		if !computeFunctionScan(node.TableDef.TblFunc.Name, node.TblFuncExprList, node.Stats) {
			if len(node.Children) > 0 && childStats != nil {
				node.Stats.Outcnt = childStats.Outcnt
				node.Stats.Cost = childStats.Outcnt
				node.Stats.Selectivity = childStats.Selectivity
			}
		}

	case plan.Node_INSERT:
		if len(node.Children) > 0 && childStats != nil {
			node.Stats.Outcnt = childStats.Outcnt
			node.Stats.Cost = childStats.Outcnt
			node.Stats.Selectivity = childStats.Selectivity
			node.Stats.Rowsize = GetRowSizeFromTableDef(node.TableDef, true) * 0.8
		}

	default:
		if len(node.Children) > 0 && childStats != nil {
			node.Stats.Outcnt = childStats.Outcnt
			node.Stats.Cost = childStats.Outcnt
			node.Stats.Selectivity = childStats.Selectivity
			node.Stats.BlockNum = childStats.BlockNum
		}
	}

	// if there is a limit, outcnt is limit number
	if node.Limit != nil {
		limitExpr := DeepCopyExpr(node.Limit)
		if _, ok := limitExpr.Expr.(*plan.Expr_F); ok {
			if !hasParam(limitExpr) {
				limitExpr, _ = ConstantFold(batch.EmptyForConstFoldBatch, limitExpr, builder.compCtx.GetProcess(), true, true)
			}
		}
		if cExpr, ok := limitExpr.Expr.(*plan.Expr_Lit); ok {
			if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
				node.Stats.Outcnt = float64(c.U64Val)
				node.Stats.Selectivity = node.Stats.Outcnt / node.Stats.Cost
			}
		}
	}
}

func computeFunctionScan(name string, exprs []*Expr, nodeStat *Stats) bool {
	if name != "generate_series" {
		return false
	}
	var cost float64
	var canGetCost bool
	if len(exprs) == 2 {
		if exprs[0].Typ.Id != exprs[1].Typ.Id {
			return false
		}
		cost, canGetCost = getCost(exprs[0], exprs[1], nil)
	} else if len(exprs) == 3 {
		if !(exprs[0].Typ.Id == exprs[1].Typ.Id && exprs[1].Typ.Id == exprs[2].Typ.Id) {
			return false
		}
		cost, canGetCost = getCost(exprs[0], exprs[1], exprs[2])
	} else {
		return false
	}
	if !canGetCost {
		return false
	}
	nodeStat.Outcnt = cost
	nodeStat.TableCnt = cost
	nodeStat.Cost = cost
	nodeStat.Selectivity = 1
	return true
}

func getCost(start *Expr, end *Expr, step *Expr) (float64, bool) {
	var startNum, endNum, stepNum float64
	var flag1, flag2, flag3 bool
	getInt32Val := func(e *Expr) (float64, bool) {
		if s, ok := e.Expr.(*plan.Expr_Lit); ok {
			if v, ok := s.Lit.Value.(*plan.Literal_I32Val); ok && !s.Lit.Isnull {
				return float64(v.I32Val), true
			}
		}
		return 0, false
	}
	getInt64Val := func(e *Expr) (float64, bool) {
		if s, ok := e.Expr.(*plan.Expr_Lit); ok {
			if v, ok := s.Lit.Value.(*plan.Literal_I64Val); ok && !s.Lit.Isnull {
				return float64(v.I64Val), true
			}
		}
		return 0, false
	}

	switch start.Typ.Id {
	case int32(types.T_int32):
		startNum, flag1 = getInt32Val(start)
		endNum, flag2 = getInt32Val(end)
		flag3 = true
		if step != nil {
			stepNum, flag3 = getInt32Val(step)
		}
		if !(flag1 && flag2 && flag3) {
			return 0, false
		}
	case int32(types.T_int64):
		startNum, flag1 = getInt64Val(start)
		endNum, flag2 = getInt64Val(end)
		flag3 = true
		if step != nil {
			stepNum, flag3 = getInt64Val(step)
		}
		if !(flag1 && flag2 && flag3) {
			return 0, false
		}
	}
	if step == nil {
		if startNum > endNum {
			stepNum = -1
		} else {
			stepNum = 1
		}
	}
	ret := (endNum - startNum) / stepNum
	if ret < 0 {
		return 0, false
	}
	return ret, true
}

func foldTableScanFilters(proc *process.Process, qry *Query, nodeId int32, foldInExpr bool) {
	node := qry.Nodes[nodeId]
	if node.NodeType == plan.Node_TABLE_SCAN && len(node.FilterList) > 0 {
		for i, e := range node.FilterList {
			foldedExpr, err := ConstantFold(batch.EmptyForConstFoldBatch, e, proc, false, foldInExpr)
			if err == nil && foldedExpr != nil {
				node.FilterList[i] = foldedExpr
			}
		}
	}
	for _, childId := range node.Children {
		foldTableScanFilters(proc, qry, childId, foldInExpr)
	}
}

func recalcStatsByRuntimeFilter(scanNode *plan.Node, joinNode *plan.Node, builder *QueryBuilder) {
	if scanNode.NodeType != plan.Node_TABLE_SCAN {
		return
	}
	if joinNode.NodeType != plan.Node_JOIN && joinNode.NodeType != plan.Node_FUZZY_FILTER {
		return
	}

	if joinNode.JoinType == plan.Node_INDEX || joinNode.NodeType == plan.Node_FUZZY_FILTER {
		scanNode.Stats.Outcnt = builder.qry.Nodes[joinNode.Children[1]].Stats.Outcnt
		if scanNode.Stats.Outcnt > scanNode.Stats.TableCnt {
			scanNode.Stats.Outcnt = scanNode.Stats.TableCnt
		}
		scanNode.Stats.BlockNum = int32(scanNode.Stats.Outcnt/3) + 1
		scanNode.Stats.Cost = float64(scanNode.Stats.BlockNum) * DefaultBlockMaxRows
		if scanNode.Stats.Cost > scanNode.Stats.TableCnt {
			scanNode.Stats.Cost = scanNode.Stats.TableCnt
			scanNode.Stats.BlockNum = int32(scanNode.Stats.TableCnt / DefaultBlockMaxRows)
		}
		scanNode.Stats.Selectivity = scanNode.Stats.Outcnt / scanNode.Stats.TableCnt
		return
	}
	runtimeFilterSel := builder.qry.Nodes[joinNode.Children[1]].Stats.Selectivity
	scanNode.Stats.Cost *= runtimeFilterSel
	scanNode.Stats.Outcnt *= runtimeFilterSel
	if scanNode.Stats.Cost < 1 {
		scanNode.Stats.Cost = 1
	}
	scanNode.Stats.BlockNum = int32(scanNode.Stats.Outcnt/3) + 1
	scanNode.Stats.Selectivity = andSelectivity(scanNode.Stats.Selectivity, runtimeFilterSel)
}

func calcScanStats(node *plan.Node, builder *QueryBuilder) *plan.Stats {
	if builder.skipStats {
		return DefaultStats()
	}
	if InternalTable(node.TableDef) {
		return DefaultStats()
	}
	if shouldReturnMinimalStats(node) {
		return DefaultMinimalStats()
	}

	//ts := timestamp.Timestamp{}
	//if node.ScanTS != nil {
	//	ts = *node.ScanTS
	//}

	scanSnapshot := node.ScanSnapshot
	if scanSnapshot == nil {
		scanSnapshot = &Snapshot{}
	}

	s, err := builder.compCtx.Stats(node.ObjRef, *scanSnapshot)
	if err != nil || s == nil {
		return DefaultStats()
	}

	stats := new(plan.Stats)
	stats.TableCnt = s.TableCnt
	var blockSel float64 = 1

	var blockExprList []*plan.Expr
	for i := range node.FilterList {
		node.FilterList[i].Selectivity = estimateExprSelectivity(node.FilterList[i], builder)
		currentBlockSel := estimateFilterBlockSelectivity(builder.GetContext(), node.FilterList[i], node.TableDef, s)
		if builder.optimizerHints != nil {
			if builder.optimizerHints.blockFilter == 1 { //always trying to pushdown blockfilters if zonemappable
				if ExprIsZonemappable(builder.GetContext(), node.FilterList[i]) {
					copyOfExpr := DeepCopyExpr(node.FilterList[i])
					copyOfExpr.Selectivity = currentBlockSel
					blockExprList = append(blockExprList, copyOfExpr)
				}
			} else if builder.optimizerHints.blockFilter == 2 { // never pushdown blockfilters
				node.BlockFilterList = nil
			} else {
				if currentBlockSel < 1 || strings.HasPrefix(node.TableDef.Name, catalog.IndexTableNamePrefix) {
					copyOfExpr := DeepCopyExpr(node.FilterList[i])
					copyOfExpr.Selectivity = currentBlockSel
					blockExprList = append(blockExprList, copyOfExpr)
				}
			}
		} else {
			if currentBlockSel < 1 || strings.HasPrefix(node.TableDef.Name, catalog.IndexTableNamePrefix) {
				copyOfExpr := DeepCopyExpr(node.FilterList[i])
				copyOfExpr.Selectivity = currentBlockSel
				blockExprList = append(blockExprList, copyOfExpr)
			}
		}
		blockSel = andSelectivity(blockSel, currentBlockSel)
	}
	node.BlockFilterList = blockExprList
	stats.Selectivity = estimateExprSelectivity(colexec.RewriteFilterExprList(node.FilterList), builder)
	stats.Outcnt = stats.Selectivity * stats.TableCnt
	stats.Cost = stats.TableCnt * blockSel
	stats.BlockNum = int32(float64(s.BlockNumber)*blockSel) + 1

	// if there is a limit, outcnt is limit number
	if node.Limit != nil {
		if cExpr, ok := node.Limit.Expr.(*plan.Expr_Lit); ok {
			if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
				stats.Outcnt = float64(c.U64Val)
				stats.BlockNum = int32(((stats.Outcnt / stats.Selectivity) / DefaultBlockMaxRows) + 1)
				stats.Cost = float64(stats.BlockNum * DefaultBlockMaxRows)
			}
		}
	}

	return stats
}

func shouldReturnMinimalStats(node *plan.Node) bool {
	return false
}

func InternalTable(tableDef *TableDef) bool {
	switch tableDef.TblId {
	case catalog.MO_DATABASE_ID, catalog.MO_TABLES_ID, catalog.MO_COLUMNS_ID:
		return true
	}
	if strings.HasPrefix(tableDef.Name, "sys_") {
		return true
	}
	if strings.HasPrefix(tableDef.Name, "mo_") {
		return true
	}
	return false
}

func DefaultHugeStats() *plan.Stats {
	stats := new(Stats)
	stats.TableCnt = 100000000
	stats.Cost = 100000000
	stats.Outcnt = 100000000
	stats.Selectivity = 1
	stats.BlockNum = 10000
	return stats
}

func DefaultBigStats() *plan.Stats {
	stats := new(Stats)
	stats.TableCnt = 10000000
	stats.Cost = float64(costThresholdForOneCN)
	stats.Outcnt = float64(costThresholdForOneCN)
	stats.Selectivity = 1
	stats.BlockNum = int32(BlockThresholdForOneCN)
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

func DefaultMinimalStats() *plan.Stats {
	stats := new(Stats)
	stats.TableCnt = 100000
	stats.Cost = 10
	stats.Outcnt = 10
	stats.Selectivity = 0.0001
	stats.BlockNum = 1
	return stats
}

func resetHashMapStats(stats *plan.Stats) {
	if stats.HashmapStats == nil {
		stats.HashmapStats = &plan.HashMapStats{}
	}
	stats.HashmapStats.HashmapSize = 1
	stats.HashmapStats.HashOnPK = false
	stats.HashmapStats.Shuffle = false
}

func (builder *QueryBuilder) determineBuildAndProbeSide(nodeID int32, recursive bool) {
	if builder.optimizerHints != nil && builder.optimizerHints.joinOrdering != 0 {
		return
	}

	node := builder.qry.Nodes[nodeID]
	if recursive && len(node.Children) > 0 {
		for _, child := range node.Children {
			builder.determineBuildAndProbeSide(child, recursive)
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
		if builder.optimizerHints != nil && builder.optimizerHints.disableRightJoin != 0 {
			node.BuildOnLeft = false
		} else if builder.IsEquiJoin(node) && leftChild.Stats.Outcnt*1.2 < rightChild.Stats.Outcnt && !builder.haveOnDuplicateKey {
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
	if s1 < s2 {
		s1, s2 = s2, s1
	}
	if s1 > 0.15 || s2 > 0.15 || s1*s2 > 0.1 {
		return s1 * s2
	}
	return math.Min(s1, s2) * math.Max(math.Pow(s1, s2), math.Pow(s2, s1))
}

func orSelectivity(s1, s2 float64) float64 {
	var s float64
	if s1 < s2 {
		s1, s2 = s2, s1
	}
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

func HasShuffleInPlan(qry *plan.Query) bool {
	for _, node := range qry.GetNodes() {
		if node.NodeType != plan.Node_TABLE_SCAN && node.Stats.HashmapStats != nil && node.Stats.HashmapStats.Shuffle {
			return true
		}
	}
	return false
}

func GetExecType(qry *plan.Query, txnHaveDDL bool) ExecType {
	ret := ExecTypeTP
	for _, node := range qry.GetNodes() {
		switch node.NodeType {
		case plan.Node_RECURSIVE_CTE, plan.Node_RECURSIVE_SCAN:
			ret = ExecTypeAP_ONECN
		}
		stats := node.Stats
		if stats == nil || stats.BlockNum > int32(BlockThresholdForOneCN) || stats.Cost > float64(costThresholdForOneCN) {
			if !txnHaveDDL {
				return ExecTypeAP_MULTICN
			}
		}
		if stats.BlockNum > blockThresholdForTpQuery || stats.Cost > costThresholdForTpQuery {
			ret = ExecTypeAP_ONECN
		}
		if node.NodeType != plan.Node_TABLE_SCAN && stats.HashmapStats != nil && stats.HashmapStats.Shuffle {
			ret = ExecTypeAP_ONECN
		}
	}
	return ret
}

func GetPlanTitle(qry *plan.Query, txnHaveDDL bool) string {
	switch GetExecType(qry, txnHaveDDL) {
	case ExecTypeTP:
		return "TP QURERY PLAN"
	case ExecTypeAP_ONECN:
		return "AP QUERY PLAN ON ONE CN(" + strconv.Itoa(ncpu) + " core)"
	case ExecTypeAP_MULTICN:
		return "AP QUERY PLAN ON MULTICN(" + strconv.Itoa(ncpu) + " core)"
	}
	return "QUERY PLAN"
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
		ForceOneCN:   stats.ForceOneCN,
	}
}

func calcBlockSelectivityUsingShuffleRange(s *pb.ShuffleRange, expr *plan.Expr) float64 {
	sel := expr.Selectivity
	if s == nil {
		if expr.GetF().Func.ObjName == "isnull" || expr.GetF().Func.ObjName == "is_null" {
			//speicial handle for isnull
			return sel
		}
		if sel <= 0.01 {
			return sel * 100
		} else {
			return 1
		}
	}
	ret := sel * math.Pow(500, math.Pow(s.Overlap, 2))
	if ret > 1 {
		ret = 1
	}
	return ret
}

func (builder *QueryBuilder) canSkipStats() bool {
	if builder.skipStats {
		// if already set to true by other parts, just skip stats
		return true
	}
	//skip stats for select count(*) from xx
	if len(builder.qry.Steps) == 1 && len(builder.qry.Nodes) == 3 {
		project := builder.qry.Nodes[builder.qry.Steps[0]]
		if project.NodeType != plan.Node_PROJECT {
			return false
		}
		agg := builder.qry.Nodes[project.Children[0]]
		if agg.NodeType != plan.Node_AGG {
			return false
		}
		if len(agg.AggList) != 1 || len(agg.GroupBy) != 0 {
			return false
		}
		if agg.AggList[0].GetF() == nil || agg.AggList[0].GetF().Func.ObjName != "starcount" {
			return false
		}
		scan := builder.qry.Nodes[agg.Children[0]]
		return scan.NodeType == plan.Node_TABLE_SCAN
	}
	//skip stats for select * from xx limit 0, including view
	if len(builder.qry.Steps) == 1 {
		project := builder.qry.Nodes[builder.qry.Steps[0]]
		if project.NodeType != plan.Node_PROJECT {
			return false
		}
		if project.Limit != nil {
			if cExpr, ok := project.Limit.Expr.(*plan.Expr_Lit); ok {
				if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
					return c.U64Val == 0
				}
			}
		}
	}
	return false
}

func (builder *QueryBuilder) hintQueryType() {
	if builder.optimizerHints != nil && builder.optimizerHints.execType != 0 {
		for _, node := range builder.qry.GetNodes() {
			switch builder.optimizerHints.execType {
			case 1:
				*node.Stats = *DefaultMinimalStats()
			case 2:
				*node.Stats = *DefaultBigStats()
			case 3:
				*node.Stats = *DefaultHugeStats()
			default:
				panic("wrong optimizer hints for execType!")
			}
		}
		return
	}
}
