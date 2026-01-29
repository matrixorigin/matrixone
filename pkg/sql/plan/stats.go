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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const blockThresholdForTpQuery = 32
const BlockThresholdForOneCN = 512
const costThresholdForOneCN = 160000
const costThresholdForTpQuery = 240000
const highNDVcolumnThreshHold = 0.95
const statsCacheInitSize = 128
const statsCacheMaxSize = 8192

// RowSizeThreshold Regardless of the table,
// the minimum row size is 100.
// However, due to inaccurate statistical information,
// the RowSizeThreshold is tentatively set at 128,
// and it is only used for tables with vector indexes
const RowSizeThreshold = 128
const LargeBlockThresholdForOneCN = 4
const LargeBlockThresholdForMultiCN = 32

// for test
var ForceScanOnMultiCN atomic.Bool

func SetForceScanOnMultiCN(v bool) {
	ForceScanOnMultiCN.Store(v)
}

func GetForceScanOnMultiCN() bool {
	return ForceScanOnMultiCN.Load()
}

type ExecType int

const (
	ExecTypeTP ExecType = iota
	ExecTypeAP_ONECN
	ExecTypeAP_MULTICN
)

// The StatsInfoWrapper caches the stats query result for a table.
type StatsInfoWrapper struct {
	stats     *pb.StatsInfo // last query result (may be nil)
	lastVisit int64         // last visit time (unix seconds), 0 means not cached
}

// Exists returns true if the wrapper has been set (lastVisit > 0).
func (w *StatsInfoWrapper) Exists() bool {
	return w.lastVisit > 0
}

// GetStats returns the cached stats.
func (w *StatsInfoWrapper) GetStats() *pb.StatsInfo {
	return w.stats
}

// GetLastVisit returns the last visit time (unix seconds).
func (w *StatsInfoWrapper) GetLastVisit() int64 {
	return w.lastVisit
}

type StatsCache struct {
	cache map[uint64]StatsInfoWrapper
}

func NewStatsCache() *StatsCache {
	return &StatsCache{
		cache: make(map[uint64]StatsInfoWrapper, statsCacheInitSize),
	}
}

// Get returns the cached wrapper for the table.
// Use wrapper.Exists() to check if it was actually cached.
func (sc *StatsCache) Get(tableID uint64) StatsInfoWrapper {
	if sc == nil {
		return StatsInfoWrapper{}
	}
	return sc.cache[tableID]
}

// Set caches the stats result for the table.
func (sc *StatsCache) Set(tableID uint64, stats *pb.StatsInfo) {
	if sc == nil {
		return
	}
	if len(sc.cache) > statsCacheMaxSize {
		sc.cache = make(map[uint64]StatsInfoWrapper, statsCacheInitSize)
		logutil.Infof("statscache entries more than %v in long session, release memory", statsCacheMaxSize)
	}
	sc.cache[tableID] = StatsInfoWrapper{
		stats:     stats,
		lastVisit: time.Now().Unix(),
	}
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

type TableStatsInfo struct {
	ColumnZMs            []objectio.ZoneMap
	DataTypes            []types.Type
	ColumnNDVs           []float64
	MaxNDVs              []float64
	NDVinMaxObject       []float64 // NDV in the object with maximum row count, per column
	NDVinMinObject       []float64 // NDV in the object with minimum row count, per column
	NullCnts             []int64
	ShuffleRanges        []*pb.ShuffleRange
	ColumnSize           []int64
	MaxObjectRowCount    uint32 // Maximum row count among all objects in the table
	MinObjectRowCount    uint32 // Minimum row count among all objects in the table
	BlockNumber          int64
	AccurateObjectNumber int64
	ApproxObjectNumber   int64
	TableRowCount        float64 // Total row count in the table
}

func NewTableStatsInfo(lenCols int) *TableStatsInfo {
	info := &TableStatsInfo{
		ColumnZMs:      make([]objectio.ZoneMap, lenCols),
		DataTypes:      make([]types.Type, lenCols),
		ColumnNDVs:     make([]float64, lenCols),
		MaxNDVs:        make([]float64, lenCols),
		NDVinMaxObject: make([]float64, lenCols),
		NDVinMinObject: make([]float64, lenCols),
		NullCnts:       make([]int64, lenCols),
		ColumnSize:     make([]int64, lenCols),
		ShuffleRanges:  make([]*pb.ShuffleRange, lenCols),
	}
	return info
}

func AdjustNDV(info *TableStatsInfo, tableDef *TableDef, s *pb.StatsInfo) {
	if info.AccurateObjectNumber > 1 {
		for i, coldef := range tableDef.Cols[:len(tableDef.Cols)-1] {
			if info.ColumnNDVs[i] > s.TableCnt {
				info.ColumnNDVs[i] = s.TableCnt * 0.99 // to avoid a bug
			}
			colName := coldef.Name
			rate := info.ColumnNDVs[i] / info.TableRowCount
			if info.ColumnNDVs[i] < 3 {
				info.ColumnNDVs[i] *= (4 - info.ColumnNDVs[i])
				continue
			}
			if info.MaxNDVs[i] < 50 {
				info.ColumnNDVs[i] = info.MaxNDVs[i]
				continue
			}
			if info.MaxNDVs[i] < 500 && rate < 0.01 {
				info.ColumnNDVs[i] = info.MaxNDVs[i]
				continue
			}
			overlap := 1.0
			if s.ShuffleRangeMap[colName] != nil {
				overlap = s.ShuffleRangeMap[colName].Overlap
			}
			if overlap < overlapThreshold/3 {
				info.ColumnNDVs[i] = info.TableRowCount * rate
				continue
			}
			if GetSortOrder(tableDef, int32(i)) != -1 && overlap < overlapThreshold/2 {
				info.ColumnNDVs[i] = info.TableRowCount * rate
				continue
			}
			rateMin := info.NDVinMinObject[i] / float64(info.MinObjectRowCount)
			rateMax := info.NDVinMaxObject[i] / float64(info.MaxObjectRowCount)
			if rateMin/rateMax > 0.8 && rateMin/rateMax < 1.2 && overlap < overlapThreshold/2 {
				info.ColumnNDVs[i] = info.TableRowCount * rate * (1 - overlap)
				continue
			}

			if info.NDVinMinObject[i] == info.NDVinMaxObject[i] && info.NDVinMaxObject[i] == info.MaxNDVs[i] && overlap > overlapThreshold/2 {
				info.ColumnNDVs[i] = info.MaxNDVs[i]
				continue
			}
			if info.NDVinMinObject[i]/info.NDVinMaxObject[i] > 0.99 && info.NDVinMaxObject[i]/info.MaxNDVs[i] > 0.99 && overlap > overlapThreshold*2/3 {
				info.ColumnNDVs[i] = info.MaxNDVs[i] * 1.1
				continue
			}
			if info.NDVinMinObject[i]/info.NDVinMaxObject[i] > 0.95 && float64(info.MinObjectRowCount)/float64(info.MaxObjectRowCount) < 0.85 && overlap > overlapThreshold {
				if rateMax < 0.3 {
					info.ColumnNDVs[i] = info.MaxNDVs[i] * 1.1
				} else {
					info.ColumnNDVs[i] = info.MaxNDVs[i] / (1 - rateMax)
				}
				continue
			}

			//don't know how to calc ndv, just guess
			if GetSortOrder(tableDef, int32(i)) > 0 && overlap > overlapThreshold/2 {
				if info.AccurateObjectNumber > 100 {
					info.ColumnNDVs[i] /= 80
				} else {
					info.ColumnNDVs[i] = info.MaxNDVs[i] * 3
				}
				continue
			}
			if overlap > overlapThreshold {
				if info.AccurateObjectNumber > 100 {
					info.ColumnNDVs[i] /= 30
				} else {
					info.ColumnNDVs[i] = info.MaxNDVs[i] * 3
				}
			} else {
				info.ColumnNDVs[i] /= math.Pow(float64(info.AccurateObjectNumber), (1-rate)*overlap)
			}
		}
	}

	for i, coldef := range tableDef.Cols[:len(tableDef.Cols)-1] {
		colName := coldef.Name
		s.NdvMap[colName] = info.ColumnNDVs[i]
		if s.NdvMap[colName] > s.TableCnt {
			s.NdvMap[colName] = s.TableCnt * 0.99
		}
	}
}

func UpdateStatsInfo(info *TableStatsInfo, tableDef *plan.TableDef, s *pb.StatsInfo) {
	start := time.Now()
	defer func() {
		v2.TxnStatementUpdateStatsInfoMapHistogram.Observe(time.Since(start).Seconds())
	}()
	s.ApproxObjectNumber = info.ApproxObjectNumber
	s.AccurateObjectNumber = info.AccurateObjectNumber
	s.BlockNumber = info.BlockNumber
	s.TableCnt = info.TableRowCount
	s.TableName = tableDef.Name

	for i, coldef := range tableDef.Cols[:len(tableDef.Cols)-1] {
		colName := coldef.Name
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
		case types.T_float32:
			s.MinValMap[colName] = float64(types.DecodeFloat32(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeFloat32(info.ColumnZMs[i].GetMaxBuf()))
		case types.T_float64:
			s.MinValMap[colName] = float64(types.DecodeFloat64(info.ColumnZMs[i].GetMinBuf()))
			s.MaxValMap[colName] = float64(types.DecodeFloat64(info.ColumnZMs[i].GetMaxBuf()))
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
		case types.T_decimal64:
			// Fix: Use Decimal64ToFloat64 with proper scale to handle negative values correctly
			// Direct cast to float64 treats negative values (stored as two's complement) as large positive numbers
			// IMPORTANT: Use ZoneMap's scale, not TableDef's scale
			// ZoneMap stores the scale from when data was written (may differ from current schema after ALTER TABLE)
			scale := info.ColumnZMs[i].GetScale()
			minDec := types.DecodeDecimal64(info.ColumnZMs[i].GetMinBuf())
			maxDec := types.DecodeDecimal64(info.ColumnZMs[i].GetMaxBuf())
			minFloat := types.Decimal64ToFloat64(minDec, scale)
			maxFloat := types.Decimal64ToFloat64(maxDec, scale)
			s.MinValMap[colName] = minFloat
			s.MaxValMap[colName] = maxFloat

		case types.T_decimal128:
			// Fix: Use actual scale from ZoneMap (not TableDef)
			// This ensures consistency with getMinMaxValueByFloat64 in disttae/stats.go
			scale := info.ColumnZMs[i].GetScale()
			minDec := types.DecodeDecimal128(info.ColumnZMs[i].GetMinBuf())
			maxDec := types.DecodeDecimal128(info.ColumnZMs[i].GetMaxBuf())
			minFloat := types.Decimal128ToFloat64(minDec, scale)
			maxFloat := types.Decimal128ToFloat64(maxDec, scale)
			s.MinValMap[colName] = minFloat
			s.MaxValMap[colName] = maxFloat

		}

		if info.ShuffleRanges[i] != nil {
			if s.MinValMap[colName] != s.MaxValMap[colName] &&
				s.TableCnt > ShuffleThreshHoldOfNDV*2 &&
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

	w := builder.getStatsInfoByTableID(tableDef.TblId)
	if w == nil || w.GetStats() == nil {
		return false
	}
	s := w.GetStats()
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

	w := builder.getStatsInfoByTableID(tableDef.TblId)
	if w == nil || w.GetStats() == nil {
		return 0
	}
	s := w.GetStats()
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

func (builder *QueryBuilder) getStatsInfoByTableID(tableID uint64) *StatsInfoWrapper {
	if builder == nil {
		return nil
	}
	sc := builder.compCtx.GetStatsCache()
	if sc == nil {
		return nil
	}
	w := sc.Get(tableID)
	if !w.Exists() {
		return nil
	}
	return &w
}

func (builder *QueryBuilder) getStatsInfoByCol(col *plan.ColRef) *StatsInfoWrapper {
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
	w := sc.Get(tableDef.TblId)
	if !w.Exists() {
		return nil
	}
	return &w
}

func (builder *QueryBuilder) getColNdv(col *plan.ColRef) float64 {
	w := builder.getStatsInfoByCol(col)
	if w == nil || w.GetStats() == nil {
		return -1
	}
	return w.GetStats().NdvMap[col.Name]
}

//func (builder *QueryBuilder) getColOverlap(col *plan.ColRef) float64 {
//	s := builder.getStatsInfoByCol(col)
//	if s == nil || s.ShuffleRangeMap[col.Name] == nil {
//		return 1.0
//	}
//	return s.ShuffleRangeMap[col.Name].Overlap
//}

func getNullSelectivity(arg *plan.Expr, builder *QueryBuilder, isnull bool) float64 {
	switch exprImpl := arg.Expr.(type) {
	case *plan.Expr_Col:
		col := exprImpl.Col
		w := builder.getStatsInfoByCol(col)
		if w == nil || w.GetStats() == nil {
			break
		}
		s := w.GetStats()
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
		case "%", "mod":
			// For modulo operations like b%N, the NDV is at most N
			if len(exprImpl.F.Args) < 2 {
				return getExprNdv(exprImpl.F.Args[0], builder)
			}
			lit := exprImpl.F.Args[1].GetLit()
			if lit == nil {
				// Second argument is not a literal, fallback to column NDV
				return getExprNdv(exprImpl.F.Args[0], builder)
			}

			var modValue float64
			switch v := lit.Value.(type) {
			case *plan.Literal_I64Val:
				if v.I64Val > 0 {
					modValue = float64(v.I64Val)
				}
			case *plan.Literal_I32Val:
				if v.I32Val > 0 {
					modValue = float64(v.I32Val)
				}
			case *plan.Literal_I16Val:
				if v.I16Val > 0 {
					modValue = float64(v.I16Val)
				}
			case *plan.Literal_I8Val:
				if v.I8Val > 0 {
					modValue = float64(v.I8Val)
				}
			case *plan.Literal_U64Val:
				if v.U64Val > 0 && v.U64Val <= math.MaxInt64 {
					modValue = float64(v.U64Val)
				}
			case *plan.Literal_U32Val:
				if v.U32Val > 0 {
					modValue = float64(v.U32Val)
				}
			case *plan.Literal_U16Val:
				if v.U16Val > 0 {
					modValue = float64(v.U16Val)
				}
			case *plan.Literal_U8Val:
				if v.U8Val > 0 {
					modValue = float64(v.U8Val)
				}
			}

			if modValue > 0 {
				// Take min of column NDV and modulo value for conservative estimate
				colNdv := getExprNdv(exprImpl.F.Args[0], builder)
				if colNdv > 0 {
					return math.Min(colNdv, modValue)
				}
				// Unknown NDV should remain unknown, not become a small certain value
				return colNdv
			}
			// Invalid modValue (zero, negative, or overflow), fallback to column NDV
			return getExprNdv(exprImpl.F.Args[0], builder)
		default:
			return getExprNdv(exprImpl.F.Args[0], builder)
		}
	case *plan.Expr_Col:
		return builder.getColNdv(exprImpl.Col)
	}
	return -1
}

func estimateEqualitySelectivity(expr *plan.Expr, builder *QueryBuilder, s *pb.StatsInfo) float64 {
	// only filter like func(col)=1 or col=? can estimate outcnt
	// and only 1 colRef is allowd in the filter. otherwise, no good method to calculate
	col := extractColRefInFilter(expr)
	if col == nil {
		return 0.01
	}
	if col.Name == catalog.CPrimaryKeyColName {
		if s != nil {
			return 1 / s.TableCnt
		} else {
			return 0.0000001
		}
	}
	ndv := getExprNdv(expr, builder)
	if ndv > 0 {
		return 1 / ndv
	}
	return 0.01
}

// calcSelectivityByMinMaxForDecimal handles decimal types with proper scale conversion
func calcSelectivityByMinMaxForDecimal(funcName string, min, max float64, expr *plan.Expr) (ret float64) {
	if max < min {
		return 0.1
	}

	// Extract literal expressions with type information (which includes scale)
	fn := expr.GetF()
	if fn == nil || len(fn.Args) < 2 {
		return 0.1
	}

	// Get scale from the literal expr's type
	var val1, val2 float64
	var ok bool

	switch funcName {
	case ">", ">=", "<", "<=":
		lit := fn.Args[1].GetLit()
		if lit == nil {
			return 0.1
		}
		// Use the expr's type which contains scale information
		scale := fn.Args[1].Typ.Scale
		val1, ok = getDecimalLiteralValue(lit, scale)
		if !ok {
			return 0.1
		}

		switch funcName {
		case ">", ">=":
			// If value is greater than max, almost no rows will match
			if val1 > max {
				return 0.00000001
			}
			ret = (max - val1 + 1) / (max - min)
		case "<", "<=":
			// If value is less than min, almost no rows will match
			if val1 < min {
				return 0.00000001
			}
			ret = (val1 - min + 1) / (max - min)
		}

	case "between", "in_range":
		lit1 := fn.Args[1].GetLit()
		lit2 := fn.Args[2].GetLit()
		if lit1 == nil || lit2 == nil {
			return 0.1
		}
		scale1 := fn.Args[1].Typ.Scale
		scale2 := fn.Args[2].Typ.Scale
		val1, ok = getDecimalLiteralValue(lit1, scale1)
		if !ok {
			return 0.1
		}
		val2, ok = getDecimalLiteralValue(lit2, scale2)
		if !ok {
			return 0.1
		}
		ret = (val2 - val1 + 1) / (max - min)

	default:
		return 0.1
	}

	if ret < 0 {
		// Value out of range, return low selectivity
		return 0.00000001
	}
	if ret > 1 {
		return 1.0
	}
	return ret
}

// getDecimalLiteralValue extracts the actual float64 value from a decimal literal using its scale
func getDecimalLiteralValue(lit *plan.Literal, scale int32) (float64, bool) {
	if val64, ok := lit.Value.(*plan.Literal_Decimal64Val); ok {
		dec64 := types.Decimal64(val64.Decimal64Val.A)
		return types.Decimal64ToFloat64(dec64, scale), true
	}
	if val128, ok := lit.Value.(*plan.Literal_Decimal128Val); ok {
		dec128 := types.Decimal128{
			B0_63:   uint64(val128.Decimal128Val.A),
			B64_127: uint64(val128.Decimal128Val.B),
		}
		return types.Decimal128ToFloat64(dec128, scale), true
	}
	return 0, false
}

func calcSelectivityByMinMax(funcName string, min, max float64, typ types.T, vals []*plan.Literal) (ret float64) {
	var ok bool
	var val1, val2 float64
	if max < min {
		//something error happend
		return 0.1
	}
	switch funcName {
	case ">", ">=":
		if val1, ok = getFloat64Value(typ, vals[0]); ok {
			ret = (max - val1 + 1) / (max - min)
		}
	case "<", "<=":
		if val1, ok = getFloat64Value(typ, vals[0]); ok {
			ret = (val1 - min + 1) / (max - min)
		}
	case "between", "in_range":
		if val1, ok = getFloat64Value(typ, vals[0]); ok {
			if val2, ok = getFloat64Value(typ, vals[1]); ok {
				ret = (val2 - val1 + 1) / (max - min)
			}
		}
	default:
		ret = 0.1
	}

	if ret < 0 {
		// val out of range, return low sel
		return 0.00000001
	}
	if !ok || ret > 1 {
		//somwthing error happened
		return 0.1
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
	case types.T_time:
		if val, valOk := lit.Value.(*plan.Literal_Timeval); valOk {
			return float64(val.Timeval), true
		}
	case types.T_datetime:
		if val, valOk := lit.Value.(*plan.Literal_Datetimeval); valOk {
			return float64(val.Datetimeval), true
		}
	case types.T_timestamp:
		if val, valOk := lit.Value.(*plan.Literal_Timestampval); valOk {
			return float64(val.Timestampval), true
		}
	case types.T_decimal64:
		if val, valOk := lit.Value.(*plan.Literal_Decimal64Val); valOk {
			// Note: This path is only used for non-decimal column types
			// For decimal columns, use calcSelectivityByMinMaxForDecimal instead
			return float64(val.Decimal64Val.A), true
		}
	case types.T_decimal128:
		if val, valOk := lit.Value.(*plan.Literal_Decimal128Val); valOk {
			d := types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)}
			return float64(types.Decimal128ToFloat64(d, 0)), true
		}
	}

	return 0, false
}

func estimateNonEqualitySelectivity(expr *plan.Expr, funcName string, builder *QueryBuilder) float64 {
	// only filter like func(colRef)>1 , or (colRef=1) or (colRef=2) can estimate outcnt
	// and only 1 colRef is allowd in the filter. otherwise, no good method to calculate
	colRef := extractColRefInFilter(expr)
	if colRef == nil {
		return 0.1
	}
	w := builder.getStatsInfoByCol(colRef)
	if w == nil || w.GetStats() == nil {
		return 0.1
	}

	//check strict filter, otherwise can not estimate outcnt by min/max val
	colRef, litType, literals, colFnName, hasDynamicParam := extractColRefAndLiteralsInFilter(expr)
	if hasDynamicParam {
		// assume dynamic parameter always has low selectivity
		if funcName == "between" || funcName == "in_range" {
			return 0.0001
		} else {
			return 0.01
		}
	}
	s := w.GetStats()
	if colRef != nil && len(literals) > 0 {
		typ := types.T(s.DataTypeMap[colRef.Name])

		switch colFnName {
		case "":
			// CRITICAL FIX: For decimal types, we need to pass the expr to get scale information
			// Decimal literals store internal scaled values, need proper conversion
			if typ == types.T_decimal64 || typ == types.T_decimal128 {
				return calcSelectivityByMinMaxForDecimal(
					funcName, s.MinValMap[colRef.Name], s.MaxValMap[colRef.Name], expr)
			}
			return calcSelectivityByMinMax(
				funcName, s.MinValMap[colRef.Name], s.MaxValMap[colRef.Name], typ, literals)
		case "year":
			switch typ {
			case types.T_date:
				minVal := types.Date(s.MinValMap[colRef.Name])
				maxVal := types.Date(s.MaxValMap[colRef.Name])
				return calcSelectivityByMinMax(funcName, float64(minVal.Year()), float64(maxVal.Year()), litType, literals)
			case types.T_datetime:
				// TODO
			}
		}
	}

	return 0.1
}

func estimateExprSelectivity(expr *plan.Expr, builder *QueryBuilder, s *pb.StatsInfo) float64 {
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
			ret = estimateEqualitySelectivity(expr, builder, s)
		case "!=", "<>":
			ret = 0.9
		case ">", "<", ">=", "<=", "between", "in_range":
			ret = estimateNonEqualitySelectivity(expr, funcName, builder)
		case "and":
			ret = estimateExprSelectivity(exprImpl.F.Args[0], builder, s)
			if len(exprImpl.F.Args) == 2 {
				sel2 := estimateExprSelectivity(exprImpl.F.Args[1], builder, s)
				if canMergeToBetweenAnd(exprImpl.F.Args[0], exprImpl.F.Args[1]) && (ret+sel2) > 1 {
					ret = ret + sel2 - 1
				} else {
					ret = andSelectivity(ret, sel2)
				}
			} else {
				for i := 1; i < len(exprImpl.F.Args); i++ {
					sel2 := estimateExprSelectivity(exprImpl.F.Args[i], builder, s)
					ret = andSelectivity(ret, sel2)
				}
			}
		case "or":
			ret = estimateExprSelectivity(exprImpl.F.Args[0], builder, s)
			for i := 1; i < len(exprImpl.F.Args); i++ {
				sel2 := estimateExprSelectivity(exprImpl.F.Args[i], builder, s)
				ret = orSelectivity(ret, sel2)
			}
		case "not":
			ret = 1 - estimateExprSelectivity(exprImpl.F.Args[0], builder, s)
		case "like":
			ret = 0.2
		case "prefix_eq":
			if containsDynamicParam(expr) {
				if s != nil {
					return 100 / s.TableCnt
				} else {
					return 0.0000001
				}
			} else {
				return 0.0001
			}
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
			ret = 0.001
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
	if expr == nil || expr.GetF() == nil {
		return 0 //something error
	}
	if expr.GetF().Func.ObjName == "prefix_in" || expr.GetF().Func.ObjName == "prefix_eq" {
		return 0 //make prefix_in and prefix_eq always the first filter
	}
	switch expr.Typ.Id {
	case int32(types.T_decimal64):
		w += 8
	case int32(types.T_decimal128):
		w += 16
	case int32(types.T_float32), int32(types.T_float64):
		w += 8
	case int32(types.T_char), int32(types.T_varchar), int32(types.T_text):
		if expr.Typ.Width < types.VarlenaInlineSize {
			w += 1
		} else {
			w += float64(expr.Typ.Width)
		}
	case int32(types.T_json), int32(types.T_datalink):
		w += 32
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funcImpl := exprImpl.F
		objName := funcImpl.Func.GetObjName()
		switch objName {
		case "like":
			w += 32
		case "cast":
			w += 8
		case "in":
			w += 3
		case "or":
			w += 8
		case "<>", "!=":
			w += 2
		case "<", "<=":
			w += 1.5
		default:
			w += 1
		}
		if strings.HasPrefix(objName, "json_") {
			w += 512
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
		sortOrder := GetSortOrder(tableDef, col.ColPos)
		blocksel := calcBlockSelectivityUsingShuffleRange(s, col.Name, expr)
		switch sortOrder {
		case 0:
			blocksel = math.Min(blocksel, 0.2)
		case 1:
			blocksel = math.Min(blocksel, 0.5)
		case 2:
			blocksel = math.Min(blocksel, 0.7)
		}
		return blocksel
	}
	return 1
}

func sortFilterListByStats(ctx context.Context, nodeID int32, builder *QueryBuilder) {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			sortFilterListByStats(ctx, child, builder)
		}
	}
	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		if node.ObjRef != nil && len(node.FilterList) >= 1 {
			sort.Slice(node.FilterList, func(i, j int) bool {
				cost1 := estimateFilterWeight(node.FilterList[i], 0) * node.FilterList[i].Selectivity
				cost2 := estimateFilterWeight(node.FilterList[j], 0) * node.FilterList[j].Selectivity
				return cost1 <= cost2
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
		if node.Stats.HashmapStats.Shuffle {
			return //dont calc shuffle nodes again
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
			if node.JoinType == plan.Node_DEDUP && node.IsRightJoin {
				pred.Ndv = leftStats.Outcnt
			} else if pred.Ndv <= 0 {
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
			node.Stats.BlockNum = leftStats.BlockNum

		case plan.Node_LEFT:
			node.Stats.Outcnt = leftStats.Outcnt
			node.Stats.Cost = leftStats.Cost + rightStats.Cost
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity_out
			node.Stats.BlockNum = leftStats.BlockNum

		case plan.Node_RIGHT:
			node.Stats.Outcnt = rightStats.Outcnt
			node.Stats.Cost = leftStats.Cost + rightStats.Cost
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity
			node.Stats.BlockNum = leftStats.BlockNum

		case plan.Node_DEDUP:
			if !node.IsRightJoin {
				node.Stats.Outcnt = rightStats.Outcnt
				node.Stats.Cost = leftStats.Cost + rightStats.Cost
				node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
				node.Stats.Selectivity = selectivity
				node.Stats.BlockNum = leftStats.BlockNum
			} else {
				node.Stats.Outcnt = leftStats.Outcnt
				node.Stats.Cost = leftStats.Cost + rightStats.Cost
				node.Stats.HashmapStats.HashmapSize = leftStats.Outcnt
				node.Stats.Selectivity = selectivity
				node.Stats.BlockNum = rightStats.BlockNum
			}

		case plan.Node_OUTER:
			node.Stats.Outcnt = leftStats.Outcnt + rightStats.Outcnt
			node.Stats.Cost = leftStats.Cost + rightStats.Cost
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity_out
			node.Stats.BlockNum = leftStats.BlockNum

		case plan.Node_SEMI, plan.Node_INDEX:
			node.Stats.Outcnt = leftStats.Outcnt * selectivity
			node.Stats.Cost = leftStats.Cost + rightStats.Cost
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity_out
			node.Stats.BlockNum = leftStats.BlockNum

		case plan.Node_ANTI:
			node.Stats.Outcnt = leftStats.Outcnt * (1 - rightStats.Selectivity) * 0.5
			node.Stats.Cost = leftStats.Cost + rightStats.Cost
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity_out
			node.Stats.BlockNum = leftStats.BlockNum

		case plan.Node_SINGLE, plan.Node_MARK:
			node.Stats.Outcnt = leftStats.Outcnt
			node.Stats.Cost = leftStats.Cost + rightStats.Cost
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity_out
			node.Stats.BlockNum = leftStats.BlockNum

		case plan.Node_L2: //L2 join is very time-consuming, increase the cost to get more dop
			node.Stats.Outcnt = leftStats.Outcnt
			node.Stats.Cost = (leftStats.Cost + rightStats.Cost) * 8
			node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
			node.Stats.Selectivity = selectivity_out
			node.Stats.BlockNum = leftStats.BlockNum * 8
		}

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
		node.Stats.BlockNum = int32(childStats.Outcnt/objectio.BlockMaxRows) + 1

	case plan.Node_UNION:
		if needResetHashMapStats {
			resetHashMapStats(node.Stats)
		}
		node.Stats.Outcnt = (leftStats.Outcnt + rightStats.Outcnt) * 0.7
		node.Stats.Cost = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Selectivity = 1
		node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
		node.Stats.BlockNum = leftStats.BlockNum

	case plan.Node_UNION_ALL:
		node.Stats.Outcnt = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Cost = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Selectivity = 1
		node.Stats.BlockNum = leftStats.BlockNum

	case plan.Node_INTERSECT:
		if needResetHashMapStats {
			resetHashMapStats(node.Stats)
		}
		node.Stats.Outcnt = math.Min(leftStats.Outcnt, rightStats.Outcnt) * 0.5
		node.Stats.Cost = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Selectivity = 1
		node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
		node.Stats.BlockNum = leftStats.BlockNum

	case plan.Node_INTERSECT_ALL:
		if needResetHashMapStats {
			resetHashMapStats(node.Stats)
		}
		node.Stats.Outcnt = math.Min(leftStats.Outcnt, rightStats.Outcnt) * 0.7
		node.Stats.Cost = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Selectivity = 1
		node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
		node.Stats.BlockNum = leftStats.BlockNum

	case plan.Node_MINUS:
		if needResetHashMapStats {
			resetHashMapStats(node.Stats)
		}
		minus := math.Max(leftStats.Outcnt, rightStats.Outcnt) - math.Min(leftStats.Outcnt, rightStats.Outcnt)
		node.Stats.Outcnt = minus * 0.5
		node.Stats.Cost = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Selectivity = 1
		node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
		node.Stats.BlockNum = leftStats.BlockNum

	case plan.Node_MINUS_ALL:
		if needResetHashMapStats {
			resetHashMapStats(node.Stats)
		}
		minus := math.Max(leftStats.Outcnt, rightStats.Outcnt) - math.Min(leftStats.Outcnt, rightStats.Outcnt)
		node.Stats.Outcnt = minus * 0.7
		node.Stats.Cost = leftStats.Outcnt + rightStats.Outcnt
		node.Stats.Selectivity = 1
		node.Stats.HashmapStats.HashmapSize = rightStats.Outcnt
		node.Stats.BlockNum = leftStats.BlockNum

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
			if builder.isRestore {
				node.Stats = DefaultHugeStats()
			} else {
				if len(node.BindingTags) > 0 {
					builder.tag2Table[node.BindingTags[0]] = node.TableDef
				}
				newStats := calcScanStats(node, builder)
				if needResetHashMapStats {
					resetHashMapStats(newStats)
				}
				node.Stats = newStats
			}
		}

	case plan.Node_FILTER:
		//filters which can not push down to scan nodes. hard to estimate selectivity
		node.Stats.Outcnt = childStats.Outcnt * 0.05
		if node.Stats.Outcnt < 1 {
			node.Stats.Outcnt = 1
		}
		node.Stats.Cost = childStats.Cost
		node.Stats.Selectivity = 0.05
		node.Stats.BlockNum = childStats.BlockNum

	case plan.Node_FUNCTION_SCAN:
		if !computeFunctionScan(node.TableDef.TblFunc.Name, node.TblFuncExprList, node.Stats) {
			if len(node.Children) > 0 && childStats != nil {
				node.Stats.Outcnt = childStats.Outcnt
				node.Stats.Cost = childStats.Outcnt
				node.Stats.Selectivity = childStats.Selectivity
				node.Stats.BlockNum = childStats.BlockNum
			}
		}

	case plan.Node_INSERT:
		if len(node.Children) > 0 && childStats != nil {
			node.Stats.BlockNum = childStats.BlockNum
			node.Stats.Outcnt = childStats.Outcnt
			node.Stats.Cost = childStats.Outcnt
			node.Stats.Selectivity = childStats.Selectivity
			node.Stats.Rowsize = GetRowSizeFromTableDef(node.TableDef, true) * 0.8
		}

	case plan.Node_APPLY:
		node.Stats.Outcnt = leftStats.Outcnt
		node.Stats.Cost = leftStats.Outcnt
		node.Stats.Selectivity = leftStats.Selectivity
		node.Stats.BlockNum = leftStats.BlockNum

	default:
		if len(node.Children) > 0 && childStats != nil {
			node.Stats.Outcnt = childStats.Outcnt
			node.Stats.Cost = childStats.Outcnt
			node.Stats.Selectivity = childStats.Selectivity
			node.Stats.BlockNum = childStats.BlockNum
		}
	}

	// if there is a limit, outcnt is limit number
	if node.Limit != nil && node.NodeType != plan.Node_TABLE_SCAN {
		// Fast path: if Limit is already a literal, no need to deep copy
		if cExpr, ok := node.Limit.Expr.(*plan.Expr_Lit); ok {
			if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
				node.Stats.Outcnt = float64(c.U64Val)
				node.Stats.Selectivity = node.Stats.Outcnt / node.Stats.Cost
			}
		} else {
			// Slow path: need to fold the expression
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
	} else if node.NodeType == plan.Node_FUNCTION_SCAN && node.IndexReaderParam != nil {
		if node.IndexReaderParam.Limit != nil {
			// Fast path: if Limit is already a literal, no need to deep copy
			if cExpr, ok := node.IndexReaderParam.Limit.Expr.(*plan.Expr_Lit); ok {
				if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
					node.Stats.Outcnt = float64(c.U64Val)
					node.Stats.Selectivity = node.Stats.Outcnt / node.Stats.Cost
				}
			} else {
				// Slow path: need to fold the expression
				limitExpr := DeepCopyExpr(node.IndexReaderParam.Limit)
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
	}
}

func computeFunctionScan(name string, exprs []*Expr, nodeStat *Stats) bool {
	if name != "generate_series" {
		return false
	}
	var cost float64
	var canGetCost bool
	if len(exprs) == 1 {
		cost, canGetCost = getCost(nil, exprs[0], nil)
	} else if len(exprs) == 2 {
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

	switch end.Typ.Id {
	case int32(types.T_int32):
		if start == nil {
			startNum, flag1 = 0, true
		} else {
			startNum, flag1 = getInt32Val(start)
		}
		endNum, flag2 = getInt32Val(end)
		flag3 = true
		if step != nil {
			stepNum, flag3 = getInt32Val(step)
		}
		if !(flag1 && flag2 && flag3) {
			return 0, false
		}
	case int32(types.T_int64):
		if start == nil {
			startNum, flag1 = 0, true
		} else {
			startNum, flag1 = getInt64Val(start)
		}
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
	ret := (endNum - startNum + 1) / stepNum
	if ret < 0 {
		return 0, false
	}
	return ret, true
}

func transposeTableScanFilters(proc *process.Process, qry *Query, nodeId int32) {
	node := qry.Nodes[nodeId]
	if node.NodeType == plan.Node_TABLE_SCAN && len(node.FilterList) > 0 {
		for i, e := range node.FilterList {
			transposedExpr, err := ConstantTranspose(e, proc)
			if err == nil && transposedExpr != nil {
				node.FilterList[i] = transposedExpr
			}
		}
	}
	for _, childId := range node.Children {
		transposeTableScanFilters(proc, qry, childId)
	}
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

	if joinNode.JoinType == plan.Node_INDEX || joinNode.JoinType == plan.Node_DEDUP || joinNode.NodeType == plan.Node_FUZZY_FILTER {
		scanNode.Stats.Outcnt = builder.qry.Nodes[joinNode.Children[1]].Stats.Outcnt
		if scanNode.Stats.Outcnt > scanNode.Stats.TableCnt {
			scanNode.Stats.Outcnt = scanNode.Stats.TableCnt
		}
		newBlockNum := scanNode.Stats.Outcnt
		if newBlockNum > 64 {
			newBlockNum = (scanNode.Stats.Outcnt / 2)
		} else if newBlockNum > 256 {
			newBlockNum = (scanNode.Stats.Outcnt / 4)
		}
		if newBlockNum < float64(scanNode.Stats.BlockNum) {
			scanNode.Stats.BlockNum = int32(newBlockNum)
		}
		scanNode.Stats.Cost = float64(scanNode.Stats.BlockNum) * objectio.BlockMaxRows
		if scanNode.Stats.Cost > scanNode.Stats.TableCnt {
			scanNode.Stats.Cost = scanNode.Stats.TableCnt
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
	newBlockNum := int32(scanNode.Stats.Outcnt/3) + 1
	if newBlockNum < scanNode.Stats.BlockNum {
		scanNode.Stats.BlockNum = newBlockNum
	}
	scanNode.Stats.Selectivity = andSelectivity(scanNode.Stats.Selectivity, runtimeFilterSel)
}

func calcScanStats(node *plan.Node, builder *QueryBuilder) *plan.Stats {
	if builder.isRestore {
		return DefaultHugeStats()
	}
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

	var scanSnapshot *plan.Snapshot
	if node.ScanSnapshot != nil {
		scanSnapshot = node.ScanSnapshot
	}

	s, err := builder.compCtx.Stats(node.ObjRef, scanSnapshot)
	if err != nil || s == nil {
		return DefaultStats()
	}

	stats := new(plan.Stats)
	stats.TableCnt = s.TableCnt
	var blockSel float64 = 1

	var blockExprList []*plan.Expr
	for i := range node.FilterList {
		node.FilterList[i].Selectivity = estimateExprSelectivity(node.FilterList[i], builder, s)
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
					if ExprIsZonemappable(builder.GetContext(), node.FilterList[i]) {
						copyOfExpr := DeepCopyExpr(node.FilterList[i])
						copyOfExpr.Selectivity = currentBlockSel
						blockExprList = append(blockExprList, copyOfExpr)
					}
				}
			}
		} else {
			if currentBlockSel < 1 || strings.HasPrefix(node.TableDef.Name, catalog.IndexTableNamePrefix) {
				if ExprIsZonemappable(builder.GetContext(), node.FilterList[i]) {
					copyOfExpr := DeepCopyExpr(node.FilterList[i])
					copyOfExpr.Selectivity = currentBlockSel
					blockExprList = append(blockExprList, copyOfExpr)
				}
			}
		}
		blockSel = andSelectivity(blockSel, currentBlockSel)
	}
	node.BlockFilterList = blockExprList
	stats.Selectivity = estimateExprSelectivity(colexec.RewriteFilterExprList(node.FilterList), builder, s)
	stats.Outcnt = stats.Selectivity * stats.TableCnt
	stats.Cost = stats.TableCnt * blockSel
	stats.BlockNum = int32(float64(s.BlockNumber)*blockSel) + 1
	// estimate average row size from collected table stats: sum(SizeMap)/TableCnt
	// SizeMap stores approximate persisted bytes per column (using OriginSize); divide by total rows to get bytes/row
	var totalSize uint64
	{
		for _, v := range s.SizeMap {
			totalSize += v
		}
		if stats.TableCnt > 0 && totalSize > 0 {
			stats.Rowsize = float64(totalSize) / stats.TableCnt
		} else {
			// Fallback: use table definition to estimate row size when SizeMap is empty or TableCnt is 0
			if node.TableDef != nil {
				stats.Rowsize = GetRowSizeFromTableDef(node.TableDef, true) * 0.8
			} else {
				stats.Rowsize = 0
			}
		}
	}

	return stats
}

func forceScanNodeStatsTP(nodeID int32, builder *QueryBuilder) {
	stats := builder.qry.Nodes[nodeID].Stats
	if stats.Outcnt > 1000 {
		stats.Outcnt = 1000
	}
	if stats.Cost > 1000 {
		stats.Cost = 1000
	}
	if stats.BlockNum > 16 {
		stats.BlockNum = 16
	}
	stats.Selectivity = stats.Outcnt / stats.TableCnt
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
	stats.Rowsize = 10000
	stats.HashmapStats = &plan.HashMapStats{}
	return stats
}

func DefaultBigStats() *plan.Stats {
	stats := new(Stats)
	stats.TableCnt = 10000000
	stats.Cost = float64(costThresholdForOneCN + 1)
	stats.Outcnt = float64(costThresholdForOneCN + 1)
	stats.Selectivity = 1
	stats.BlockNum = int32(BlockThresholdForOneCN + 1)
	stats.Rowsize = 1000
	stats.HashmapStats = &plan.HashMapStats{}
	return stats
}

func IsDefaultStats(stats *plan.Stats) bool {
	return stats.Cost == 1000 && stats.TableCnt == 1000 && stats.Outcnt == 1000 && stats.Selectivity == 1 && stats.BlockNum == 1 && stats.Rowsize == 100
}

func DefaultStats() *plan.Stats {
	stats := new(Stats)
	stats.TableCnt = 1000
	stats.Cost = 1000
	stats.Outcnt = 1000
	stats.Selectivity = 1
	stats.BlockNum = 1
	stats.Rowsize = 100
	stats.HashmapStats = &plan.HashMapStats{}
	return stats
}

func DefaultMinimalStats() *plan.Stats {
	stats := new(Stats)
	stats.TableCnt = 100000
	stats.Cost = 10
	stats.Outcnt = 10
	stats.Selectivity = 0.0001
	stats.BlockNum = 1
	stats.Rowsize = 1
	stats.HashmapStats = &plan.HashMapStats{}
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
		factor1 := 1.0
		factor2 := 1.0
		if leftChild.NodeType == plan.Node_TABLE_SCAN && rightChild.NodeType == plan.Node_TABLE_SCAN {
			w1 := builder.getStatsInfoByTableID(leftChild.TableDef.TblId)
			w2 := builder.getStatsInfoByTableID(rightChild.TableDef.TblId)
			if w1 != nil && w2 != nil && w1.GetStats() != nil && w2.GetStats() != nil {
				var t1size, t2size uint64
				for _, v := range w1.GetStats().SizeMap {
					t1size += v
				}
				factor1 = math.Pow(float64(t1size), 0.1)
				for _, v := range w2.GetStats().SizeMap {
					t2size += v
				}
				factor2 = math.Pow(float64(t2size), 0.1)
			}
		}
		if leftChild.Stats.Outcnt*factor1 < rightChild.Stats.Outcnt*factor2 {
			node.Children[0], node.Children[1] = node.Children[1], node.Children[0]
		}

	case plan.Node_LEFT, plan.Node_SEMI, plan.Node_ANTI, plan.Node_SINGLE:
		//right joins does not support non equal join for now
		if builder.optimizerHints != nil && builder.optimizerHints.disableRightJoin != 0 {
			node.IsRightJoin = false
		} else if builder.IsEquiJoin(node) && leftChild.Stats.Outcnt*1.2 < rightChild.Stats.Outcnt && !builder.haveOnDuplicateKey {
			node.IsRightJoin = true
		}

	case plan.Node_DEDUP:
		if node.OnDuplicateAction != plan.Node_FAIL || node.DedupJoinCtx != nil {
			node.IsRightJoin = false
		} else if builder.optimizerHints != nil && builder.optimizerHints.disableRightJoin != 0 {
			node.IsRightJoin = false
		} else if rightChild.Stats.Outcnt > 100 && leftChild.Stats.Outcnt < rightChild.Stats.Outcnt {
			node.IsRightJoin = true
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
	if s1 > 0.02 && s2 > 0.02 {
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

// dop tuning constants
const (
	// base block-to-core mapping for dop estimation
	dopBlocksBaseUnit    int32 = 16 // default: every ~16 blocks add a core
	dopBlocksPrepareUnit int32 = 64 // prepare: more conservative
)

func calcDOP(ncpu int32, stats *plan.Stats, isPrepare bool) int32 {
	if ncpu <= 0 {
		return 1
	}

	baseUnit := dopBlocksBaseUnit
	if isPrepare {
		baseUnit = dopBlocksPrepareUnit
	}

	blocks := stats.BlockNum
	var ret int32 = 1
	if blocks > 0 {
		ret = blocks/baseUnit + 1
	}

	rs := stats.Rowsize
	if rs >= RowSizeThreshold {
		// very wide rows: be aggressive
		ret = stats.BlockNum
	}

	if ret > ncpu {
		ret = ncpu
	}
	if ret < 1 {
		ret = 1
	}
	return ret
}

// set node dop and left child recursively
func setNodeDOP(p *plan.Plan, rootID int32, dop int32) {
	qry := p.GetQuery()
	node := qry.Nodes[rootID]
	if len(node.Children) > 0 {
		setNodeDOP(p, node.Children[0], dop)
	}
	if node.NodeType == plan.Node_JOIN &&
		node.Stats != nil &&
		node.Stats.HashmapStats != nil &&
		node.Stats.HashmapStats.Shuffle {
		setNodeDOP(p, node.Children[1], dop)
	}
	if node.Stats != nil {
		node.Stats.Dop = dop
	}
}

func CalcNodeDOP(p *plan.Plan, rootID int32, ncpu int32, lencn int) {
	qry := p.GetQuery()
	node := qry.Nodes[rootID]
	for i := range node.Children {
		CalcNodeDOP(p, node.Children[i], ncpu, lencn)
	}

	// Check if node has distinct aggregation, which should run in single CPU
	hasDistinctAgg := false
	if node.NodeType == plan.Node_AGG && len(node.AggList) > 0 {
		for _, agg := range node.AggList {
			if f, ok := agg.Expr.(*plan.Expr_F); ok {
				if (uint64(f.F.Func.Obj) & function.Distinct) != 0 {
					hasDistinctAgg = true
					break
				}
			}
		}
	}

	if hasDistinctAgg {
		// distinct aggregation should run in only one node and without any parallel
		if node.Stats == nil {
			// If Stats is nil, create it first
			// This should be rare for AGG nodes, but we handle it for safety
			node.Stats = DefaultStats()
		}
		setNodeDOP(p, rootID, 1)
		node.Stats.ForceOneCN = true
		return
	}

	if node.Stats.HashmapStats != nil && node.Stats.HashmapStats.Shuffle && node.NodeType != plan.Node_TABLE_SCAN {
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_DEDUP {
			setNodeDOP(p, rootID, ncpu)
		} else {
			// there was a weird calculate shuffle dop logic here, which does not really make any sense.
			// just use ncpu.
			dop := ncpu
			childDop := qry.Nodes[node.Children[0]].Stats.Dop
			if dop < childDop {
				dop = childDop
			}
			setNodeDOP(p, rootID, dop)
		}
	} else {
		node.Stats.Dop = calcDOP(ncpu, node.Stats, p.IsPrepare)
	}
}

func CalcQueryDOP(p *plan.Plan, ncpu int32, lencn int, typ ExecType) {
	qry := p.GetQuery()
	if typ == ExecTypeTP || ncpu == 1 {
		for i := range qry.Nodes {
			qry.Nodes[i].Stats.Dop = 1
		}
		return
	}
	for i := range qry.Steps {
		CalcNodeDOP(p, qry.Steps[i], ncpu, lencn)
	}
}

func GetExecType(qry *plan.Query, txnHaveDDL bool, isPrepare bool) ExecType {
	if GetForceScanOnMultiCN() {
		return ExecTypeAP_MULTICN
	}
	ret := ExecTypeTP
	for _, node := range qry.GetNodes() {
		switch node.NodeType {
		case plan.Node_RECURSIVE_CTE, plan.Node_RECURSIVE_SCAN:
			ret = ExecTypeAP_ONECN
		}
		stats := node.Stats
		if stats == nil || stats.BlockNum > int32(BlockThresholdForOneCN) && stats.Cost > float64(costThresholdForOneCN) {
			if txnHaveDDL {
				return ExecTypeAP_ONECN
			} else {
				return ExecTypeAP_MULTICN
			}
		}
		if isPrepare {
			if stats.BlockNum > blockThresholdForTpQuery*4 || stats.Cost > costThresholdForTpQuery*4 {
				ret = ExecTypeAP_ONECN
			}
		} else {
			if stats.BlockNum > blockThresholdForTpQuery || stats.Cost > costThresholdForTpQuery {
				ret = ExecTypeAP_ONECN
			}
		}
		if node.NodeType == plan.Node_TABLE_SCAN &&
			// due to the inaccuracy of stats.Rowsize, currently only vector index tables are supported
			(node.TableDef.TableType == catalog.SystemSI_IVFFLAT_TblType_Entries || node.TableDef.TableType == catalog.Hnsw_TblType_Storage) &&
			stats.Rowsize > RowSizeThreshold &&
			stats.BlockNum > LargeBlockThresholdForOneCN {
			ret = ExecTypeAP_ONECN
			if stats.BlockNum > LargeBlockThresholdForMultiCN {
				ret = ExecTypeAP_MULTICN
			}
		}
		if node.NodeType != plan.Node_TABLE_SCAN && stats.HashmapStats != nil && stats.HashmapStats.Shuffle {
			ret = ExecTypeAP_ONECN
		}
	}
	return ret
}

func GetPlanTitle(qry *plan.Query, txnHaveDDL bool) string {
	ncpu := system.GoMaxProcs()
	switch GetExecType(qry, txnHaveDDL, false) {
	case ExecTypeTP:
		return "TP QUERY PLAN"
	case ExecTypeAP_ONECN:
		return "AP QUERY PLAN ON ONE CN(" + strconv.Itoa(ncpu) + " core)"
	case ExecTypeAP_MULTICN:
		return "AP QUERY PLAN ON MULTICN(" + strconv.Itoa(ncpu) + " core)"
	}
	return "QUERY PLAN"
}

func GetPhyPlanTitle(qry *plan.Query, txnHaveDDL bool) string {
	ncpu := system.GoMaxProcs()
	switch GetExecType(qry, txnHaveDDL, false) {
	case ExecTypeTP:
		return "TP QUERY PHYPLAN"
	case ExecTypeAP_ONECN:
		return "AP QUERY PHYPLAN ON ONE CN(" + strconv.Itoa(ncpu) + " core)"
	case ExecTypeAP_MULTICN:
		return "AP QUERY PHYPLAN ON MULTICN(" + strconv.Itoa(ncpu) + " core)"
	}
	return "QUERY PHYPLAN"
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

func getOverlap(s *pb.StatsInfo, colname string) float64 {
	if s == nil || s.ShuffleRangeMap[colname] == nil {
		return 1.0
	}
	return s.ShuffleRangeMap[colname].Overlap
}

func calcBlockSelectivityUsingShuffleRange(s *pb.StatsInfo, colname string, expr *plan.Expr) float64 {
	sel := expr.Selectivity
	switch expr.GetF().Func.ObjName {
	case "isnull", "is_null", "prefix_eq", "prefix_in": //special handle
		return sel
	}
	overlap := getOverlap(s, colname)
	if overlap < overlapThreshold/3 {
		//very good overlap
		return sel
	}
	_, _, _, _, hasDynamicParam := extractColRefAndLiteralsInFilter(expr)
	if hasDynamicParam {
		// assume dynamic parameter always has low selectivity
		if sel <= 0.02 {
			return sel * 50
		} else {
			return 1
		}
	}
	if overlap > overlapThreshold {
		if sel <= 0.002 {
			return sel * 500
		} else {
			return 1
		}
	}
	ret := sel * 100 / (1 - overlap)
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
