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
	"math"
	"math/bits"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	threshHoldForShuffleGroup       = 64000
	threshHoldForRightJoinShuffle   = 8192
	threshHoldForShuffleJoin        = 120000
	threshHoldForHybirdShuffle      = 4000000
	threshHoldForHashShuffle        = 2000000
	ShuffleThreshHoldOfNDV          = 50000
	ShuffleTypeThreshHoldLowerLimit = 16
	ShuffleTypeThreshHoldUpperLimit = 1024

	overlapThreshold = 0.95
	uniformThreshold = 0.3
)

const (
	ShuffleToRegIndex        int32 = 0
	ShuffleToLocalMatchedReg int32 = 1
	ShuffleToMultiMatchedReg int32 = 2
)

// convert first 8 bytes to uint64, slice might be less than 8 bytes
func ByteSliceToUint64(bytes []byte) uint64 {
	var result uint64 = 0
	i := 0
	length := len(bytes)
	for ; i < 8; i++ {
		result = result * 256
		if i < length {
			result += uint64(bytes[i])
		}
	}
	return result
}

// convert first 8 bytes to uint64. vec.area must be nil
// if varlena length less than 8 bytes, should have filled zero in varlena
func VarlenaToUint64Inline(v *types.Varlena) uint64 {
	return bits.ReverseBytes64(*(*uint64)(unsafe.Add(unsafe.Pointer(&v[0]), 1)))
}

// convert first 8 bytes to uint64
func VarlenaToUint64(v *types.Varlena, area []byte) uint64 {
	svlen := (*v)[0]
	if svlen <= types.VarlenaInlineSize {
		return VarlenaToUint64Inline(v)
	} else {
		voff, _ := v.OffsetLen()
		return bits.ReverseBytes64(*(*uint64)(unsafe.Pointer(&area[voff])))
	}
}

func SimpleCharHashToRange(bytes []byte, upperLimit uint64) uint64 {
	lenBytes := len(bytes)
	if lenBytes == 0 {
		// always hash empty string to first bucket
		return 0
	}
	if lenBytes == 1 {
		return uint64(bytes[0]) % upperLimit
	}
	//sample 7 bytes
	h := ((uint64(bytes[0])+1)*(uint64(bytes[lenBytes/4])+uint64(bytes[lenBytes/2])+uint64(bytes[lenBytes*3/4])+1) +
		(uint64(bytes[lenBytes-1])+1)*(uint64(bytes[1])+uint64(bytes[lenBytes-2])+1))
	return hashtable.Int64HashWithFixedSeed(h) % upperLimit
}

// IVFObjectIDHashToRange maps the complete physical ObjectID to an IVF owner.
// xxHash64 is deterministic across processes and provides uniform mixing for
// production UUIDv7 ObjectIDs, whose timestamp prefix changes slowly.
func IVFObjectIDHashToRange(objectID types.Objectid, upperLimit uint64) uint64 {
	return xxhash.Sum64(objectID[:]) % upperLimit
}

func SimpleInt64HashToRange(i uint64, upperLimit uint64) uint64 {
	return hashtable.Int64HashWithFixedSeed(i) % upperLimit
}

func initRangesShuffleParam(rsp *engine.RangesShuffleParam, typ types.T, bucketNum int) {
	if rsp.Init && rsp.ShuffleRangeBuckets == bucketNum {
		return
	}
	rsp.Init = true
	rsp.ShuffleRangeBuckets = bucketNum
	rsp.ShuffleRangeInt64 = nil
	rsp.ShuffleRangeUint64 = nil
	switch typ {
	case types.T_int64, types.T_int32, types.T_int16:
		rsp.ShuffleRangeInt64 = ShuffleRangeReEvalSigned(rsp.Node.Stats.HashmapStats.Ranges, bucketNum, rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
		rsp.ShuffleRangeUint64 = ShuffleRangeReEvalUnsigned(rsp.Node.Stats.HashmapStats.Ranges, bucketNum, rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
	}
}

func shuffleByZonemap(rsp *engine.RangesShuffleParam, zm objectio.ZoneMap, bucketNum int) uint64 {
	initRangesShuffleParam(rsp, zm.GetType(), bucketNum)

	var shuffleIDX uint64
	if len(rsp.ShuffleRangeUint64) > 0 {
		shuffleIDX = GetRangeShuffleIndexForZMUnsignedSlice(rsp.ShuffleRangeUint64, zm)
	} else if len(rsp.ShuffleRangeInt64) > 0 {
		shuffleIDX = GetRangeShuffleIndexForZMSignedSlice(rsp.ShuffleRangeInt64, zm)
	} else {
		shuffleIDX = GetRangeShuffleIndexForZM(rsp.Node.Stats.HashmapStats.ShuffleColMin, rsp.Node.Stats.HashmapStats.ShuffleColMax, zm, uint64(bucketNum))
	}
	return shuffleIDX
}

func shuffleByValueExtractedFromZonemap(rsp *engine.RangesShuffleParam, zm objectio.ZoneMap, bucketNum int) uint64 {
	t := types.T(rsp.Node.Stats.HashmapStats.ShuffleColIdx) // actually this is specially used for sort key column type
	initRangesShuffleParam(rsp, t, bucketNum)

	var shuffleIDX uint64
	if len(rsp.ShuffleRangeUint64) > 0 {
		shuffleIDX = GetRangeShuffleIndexForValuesExtractedFromZMUnsignedSlice(rsp.ShuffleRangeUint64, zm, t)
	} else if len(rsp.ShuffleRangeInt64) > 0 {
		shuffleIDX = GetRangeShuffleIndexForValuesExtractedFromZMSignedSlice(rsp.ShuffleRangeInt64, zm, t)
	} else {
		shuffleIDX = GetRangeShuffleIndexForExtractedZM(rsp.Node.Stats.HashmapStats.ShuffleColMin, rsp.Node.Stats.HashmapStats.ShuffleColMax, zm, uint64(bucketNum), t)
	}
	return shuffleIDX
}

func CalcRangeShuffleIDXForObj(rsp *engine.RangesShuffleParam, objstats *objectio.ObjectStats, bucketNum int) uint64 {
	zm := objstats.SortKeyZoneMap()
	if len(rsp.Node.TableDef.Pkey.Names) == 1 {
		initRangesShuffleParam(rsp, zm.GetType(), bucketNum)
	} else {
		initRangesShuffleParam(rsp, types.T(rsp.Node.Stats.HashmapStats.ShuffleColIdx), bucketNum)
	}
	if !zm.IsInited() {
		// an object with all null will send to shuffleIDX 0
		return 0
	}
	if len(rsp.Node.TableDef.Pkey.Names) == 1 {
		return shuffleByZonemap(rsp, zm, bucketNum)
	} else {
		return shuffleByValueExtractedFromZonemap(rsp, zm, bucketNum)
	}
}

// sampledRangeFallbackBounds returns plan-level bounds for the legacy min/max
// range path. They are distribution anchors, not whole-table extrema: SQL range
// selectivity must continue to require complete min/max provenance. Encoding
// the anchors in the plan keeps object ownership identical across CN versions
// when there are too few quantiles for the runtime bucket count.
func sampledRangeFallbackBounds(typ types.T, ranges []float64) (int64, int64, bool) {
	if len(ranges) < 2 || !shuffleRangeValueSafe(typ, ranges[0]) ||
		!shuffleRangeValueSafe(typ, ranges[len(ranges)-1]) || ranges[0] >= ranges[len(ranges)-1] {
		return 0, 0, false
	}
	return int64(ranges[0]), int64(ranges[len(ranges)-1]), true
}

func ShouldSkipObjByShuffle(rsp *engine.RangesShuffleParam, objstats *objectio.ObjectStats) bool {
	if rsp == nil || rsp.CNCNT <= 1 || rsp.Node == nil {
		return false
	}
	if rsp.ShuffleByObjectID {
		objID := objstats.ObjectLocation().ObjectId()
		return IVFObjectIDHashToRange(objID, uint64(rsp.CNCNT)) != uint64(rsp.CNIDX)
	}
	if objstats.GetAppendable() {
		//aobj always shuffle to local CN
		return !rsp.IsLocalCN
	}
	if rsp.Node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Range {
		//shuffle by range
		return CalcRangeShuffleIDXForObj(rsp, objstats, int(rsp.CNCNT)) != uint64(rsp.CNIDX)
	}
	//shuffle by hash
	objID := objstats.ObjectLocation().ObjectId()
	return SimpleCharHashToRange(objID[:], uint64(rsp.CNCNT)) != uint64(rsp.CNIDX)
}

func GetCenterValueForZMSigned(zm objectio.ZoneMap) int64 {
	switch zm.GetType() {
	case types.T_int64:
		return types.DecodeInt64(zm.GetMinBuf())/2 + types.DecodeInt64(zm.GetMaxBuf())/2
	case types.T_int32:
		return int64(types.DecodeInt32(zm.GetMinBuf()))/2 + int64(types.DecodeInt32(zm.GetMaxBuf()))/2
	case types.T_int16:
		return int64(types.DecodeInt16(zm.GetMinBuf()))/2 + int64(types.DecodeInt16(zm.GetMaxBuf()))/2
	default:
		panic("wrong type!")
	}
}

func GetCenterValueExtractFromZMSigned(zm objectio.ZoneMap, t types.T) int64 {
	idx := 0 //for now, it's always 0
	minelms, _ := types.Unpack(zm.GetMinBuf())
	maxelms, _ := types.Unpack(zm.GetMaxBuf())
	minval := minelms[idx]
	maxval := maxelms[idx]
	switch t {
	case types.T_int64:
		return minval.(int64)/2 + maxval.(int64)/2
	case types.T_int32:
		return int64(minval.(int32)/2 + maxval.(int32)/2)
	case types.T_int16:
		return int64(minval.(int16)/2 + maxval.(int16)/2)
	default:
		panic("wrong type!")
	}
}

func GetCenterValueForZMUnsigned(zm objectio.ZoneMap) uint64 {
	switch zm.GetType() {
	case types.T_uint64:
		return types.DecodeUint64(zm.GetMinBuf())/2 + types.DecodeUint64(zm.GetMaxBuf())/2
	case types.T_uint32:
		return uint64(types.DecodeUint32(zm.GetMinBuf()))/2 + uint64(types.DecodeUint32(zm.GetMaxBuf()))/2
	case types.T_uint16:
		return uint64(types.DecodeUint16(zm.GetMinBuf()))/2 + uint64(types.DecodeUint16(zm.GetMaxBuf()))/2
	case types.T_varchar, types.T_char, types.T_text:
		return ByteSliceToUint64(zm.GetMinBuf())/2 + ByteSliceToUint64(zm.GetMaxBuf())/2
	default:
		panic("wrong type!")
	}
}

func GetCenterValueExtractFromZMUnsigned(zm objectio.ZoneMap, t types.T) uint64 {
	idx := 0 //for now, it's always 0
	minelms, _ := types.Unpack(zm.GetMinBuf())
	maxelms, _ := types.Unpack(zm.GetMaxBuf())
	minval := minelms[idx]
	maxval := maxelms[idx]
	switch t {
	case types.T_uint64:
		return minval.(uint64)/2 + maxval.(uint64)/2
	case types.T_uint32:
		return uint64(minval.(uint32)/2 + maxval.(uint32)/2)
	case types.T_uint16:
		return uint64(minval.(uint16)/2 + maxval.(uint16)/2)
	case types.T_varchar, types.T_char, types.T_text:
		return ByteSliceToUint64(minval.([]byte))/2 + ByteSliceToUint64(maxval.([]byte))/2
	default:
		panic("wrong type!")
	}
}

func GetRangeShuffleIndexForZM(minVal, maxVal int64, zm objectio.ZoneMap, upplerLimit uint64) uint64 {
	switch zm.GetType() {
	case types.T_int64, types.T_int32, types.T_int16:
		return GetRangeShuffleIndexSignedMinMax(minVal, maxVal, GetCenterValueForZMSigned(zm), upplerLimit)
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		return GetRangeShuffleIndexUnsignedMinMax(uint64(minVal), uint64(maxVal), GetCenterValueForZMUnsigned(zm), upplerLimit)
	}
	logutil.Infof("unsupported zm type %v", zm.GetType())
	panic("unsupported shuffle type!")
}

func GetRangeShuffleIndexForExtractedZM(minVal, maxVal int64, zm objectio.ZoneMap, upplerLimit uint64, t types.T) uint64 {
	switch t {
	case types.T_int64, types.T_int32, types.T_int16:
		return GetRangeShuffleIndexSignedMinMax(minVal, maxVal, GetCenterValueExtractFromZMSigned(zm, t), upplerLimit)
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		return GetRangeShuffleIndexUnsignedMinMax(uint64(minVal), uint64(maxVal), GetCenterValueExtractFromZMUnsigned(zm, t), upplerLimit)
	}
	panic("unsupported shuffle type!")
}

func GetRangeShuffleIndexForZMSignedSlice(val []int64, zm objectio.ZoneMap) uint64 {
	switch zm.GetType() {
	case types.T_int64, types.T_int32, types.T_int16:
		return GetRangeShuffleIndexSignedSlice(val, GetCenterValueForZMSigned(zm))
	}
	panic("wrong type!")
}

func GetRangeShuffleIndexForValuesExtractedFromZMSignedSlice(val []int64, zm objectio.ZoneMap, t types.T) uint64 {
	switch t {
	case types.T_int64, types.T_int32, types.T_int16:
		return GetRangeShuffleIndexSignedSlice(val, GetCenterValueExtractFromZMSigned(zm, t))
	}
	panic("wrong type!")
}

func GetRangeShuffleIndexForZMUnsignedSlice(val []uint64, zm objectio.ZoneMap) uint64 {
	switch zm.GetType() {
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		return GetRangeShuffleIndexUnsignedSlice(val, GetCenterValueForZMUnsigned(zm))
	}
	panic("wrong type!")
}

func GetRangeShuffleIndexForValuesExtractedFromZMUnsignedSlice(val []uint64, zm objectio.ZoneMap, t types.T) uint64 {
	switch t {
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		return GetRangeShuffleIndexUnsignedSlice(val, GetCenterValueExtractFromZMUnsigned(zm, t))
	}
	panic("wrong type!")
}

func GetRangeShuffleIndexSignedMinMax(minVal, maxVal, currentVal int64, upplerLimit uint64) uint64 {
	if upplerLimit == 0 {
		return 0
	}
	if currentVal <= minVal {
		return 0
	} else if currentVal >= maxVal {
		return upplerLimit - 1
	} else {
		step := uint64(maxVal-minVal) / upplerLimit
		if step == 0 {
			return 0
		}
		ret := uint64(currentVal-minVal) / step
		if ret >= upplerLimit {
			return upplerLimit - 1
		}
		return ret
	}
}

func GetRangeShuffleIndexUnsignedMinMax(minVal, maxVal, currentVal uint64, upplerLimit uint64) uint64 {
	if upplerLimit == 0 {
		return 0
	}
	if currentVal <= minVal {
		return 0
	} else if currentVal >= maxVal {
		return upplerLimit - 1
	} else {
		step := (maxVal - minVal) / upplerLimit
		if step == 0 {
			return 0
		}
		ret := (currentVal - minVal) / step
		if ret >= upplerLimit {
			return upplerLimit - 1
		}
		return ret
	}
}

func GetRangeShuffleIndexSignedSlice(val []int64, currentVal int64) uint64 {
	if currentVal <= val[0] {
		return 0
	}
	left := 0
	right := len(val) - 1
	for left < right {
		mid := (left + right) >> 1
		if currentVal > val[mid] {
			left = mid + 1
		} else {
			right = mid
		}
	}
	if currentVal > val[right] {
		right += 1
	}
	return uint64(right)
}

func GetRangeShuffleIndexUnsignedSlice(val []uint64, currentVal uint64) uint64 {
	if currentVal <= val[0] {
		return 0
	}
	left := 0
	right := len(val) - 1
	for left < right {
		mid := (left + right) >> 1
		if currentVal > val[mid] {
			left = mid + 1
		} else {
			right = mid
		}
	}
	if currentVal > val[right] {
		right += 1
	}
	return uint64(right)
}

func GetHashColumn(expr *plan.Expr) (*plan.ColRef, int32) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		// support shuffle on serial_full/serial function expressions used in secondary index joins
		if exprImpl.F.Func.ObjName == "serial_full" || exprImpl.F.Func.ObjName == "serial" {
			return nil, expr.Typ.Id
		}
		return nil, -1
	case *plan.Expr_Col:
		return exprImpl.Col, expr.Typ.Id
	}
	return nil, -1
}

func reusableShuffleChild(
	col *plan.ColRef,
	node *plan.Node,
	builder *QueryBuilder,
	afterRemap bool,
) (*plan.Node, bool) {
	if col == nil || node == nil || builder == nil || builder.qry == nil ||
		len(node.Children) == 0 {
		return nil, false
	}
	childID := node.Children[0]
	if childID < 0 || int(childID) >= len(builder.qry.Nodes) {
		return nil, false
	}
	child := builder.qry.Nodes[childID]
	if child == nil || child.NodeType != plan.Node_AGG || child.Stats == nil ||
		child.Stats.HashmapStats == nil || !child.Stats.HashmapStats.Shuffle ||
		child.Stats.HashmapStats.ShuffleColIdx < 0 ||
		int(child.Stats.HashmapStats.ShuffleColIdx) >= len(child.GroupBy) {
		return nil, false
	}

	shuffleColIdx := child.Stats.HashmapStats.ShuffleColIdx
	if afterRemap {
		// Aggregate group keys are exposed as RelPos -1. ColPos retains the
		// original group-by index even when ProjectList is compacted, while the
		// join column position refers to the compacted output slot.
		if col.RelPos != 0 || col.ColPos < 0 || int(col.ColPos) >= len(child.ProjectList) {
			return nil, false
		}
		projectCol := child.ProjectList[col.ColPos].GetCol()
		if projectCol == nil || projectCol.RelPos != -1 || projectCol.ColPos != shuffleColIdx {
			return nil, false
		}
		return child, true
	}

	if builder.tag2Table[col.RelPos] != nil || len(child.BindingTags) == 0 ||
		col.RelPos != child.BindingTags[0] || col.ColPos != shuffleColIdx {
		return nil, false
	}
	groupCol := child.GroupBy[shuffleColIdx].GetCol()
	if groupCol == nil || builder.tag2Table[groupCol.RelPos] == nil {
		return nil, false
	}
	return child, true
}

func resetShuffleStrategy(hashmapStats *plan.HashMapStats) {
	hashmapStats.ShuffleType = plan.ShuffleType_Hash
	hashmapStats.ShuffleTypeForMultiCN = plan.ShuffleTypeForMultiCN_Simple
	hashmapStats.ShuffleColMin = 0
	hashmapStats.ShuffleColMax = 0
	hashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
	hashmapStats.Nullcnt = 0
	hashmapStats.Ranges = nil
}

func reuseShuffleStrategy(hashmapStats *plan.HashMapStats, child *plan.Node) {
	childStats := child.Stats.HashmapStats
	hashmapStats.ShuffleMethod = plan.ShuffleMethod_Reuse
	hashmapStats.ShuffleType = childStats.ShuffleType
	hashmapStats.ShuffleTypeForMultiCN = childStats.ShuffleTypeForMultiCN
	hashmapStats.ShuffleColMin = childStats.ShuffleColMin
	hashmapStats.ShuffleColMax = childStats.ShuffleColMax
	hashmapStats.Ranges = childStats.Ranges
	hashmapStats.Nullcnt = childStats.Nullcnt
}

// restoreRangeStrategyAfterRemap carries only range boundaries derived for the
// same join condition before remapping. remapAllColRefs mutates OnList
// expressions in place and never reorders them, so ShuffleColIdx remains the
// stable condition identity across tempOptimizeForDML. Physical Reuse and the
// multi-CN Hybrid decision are deliberately excluded: both depend on the
// current child topology and must be proved again.
func restoreRangeStrategyAfterRemap(
	hashmapStats, previous *plan.HashMapStats,
	candidateIdx int32,
) bool {
	if previous == nil || previous.ShuffleColIdx != candidateIdx ||
		previous.ShuffleType != plan.ShuffleType_Range {
		return false
	}
	hashmapStats.ShuffleType = plan.ShuffleType_Range
	hashmapStats.ShuffleColMin = previous.ShuffleColMin
	hashmapStats.ShuffleColMax = previous.ShuffleColMax
	hashmapStats.Ranges = previous.Ranges
	hashmapStats.Nullcnt = previous.Nullcnt
	return true
}

func maybeSorted(node *plan.Node, builder *QueryBuilder, tag int32) bool {
	// for scan node, primary key and cluster by may be sorted
	if node.NodeType == plan.Node_TABLE_SCAN {
		return node.BindingTags[0] == tag
	}
	// for inner join, if left child may be sorted, then inner join may be sorted
	if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_INNER {
		leftChild := builder.qry.Nodes[node.Children[0]]
		return maybeSorted(leftChild, builder, tag)
	}
	return false
}

func determineShuffleType(col *plan.ColRef, node *plan.Node, builder *QueryBuilder) {
	determineShuffleTypeWithColRefMode(col, node, builder, false)
}

func determineShuffleTypeWithColRefMode(
	col *plan.ColRef,
	node *plan.Node,
	builder *QueryBuilder,
	afterRemap bool,
) {
	// Every planning pass starts from a normal hash strategy. Candidate-specific
	// range/reuse state must be proved again for the current column.
	resetShuffleStrategy(node.Stats.HashmapStats)

	if col == nil || builder == nil {
		return
	}

	if child, reusable := reusableShuffleChild(col, node, builder, afterRemap); reusable {
		reuseShuffleStrategy(node.Stats.HashmapStats, child)
		return
	}
	determineNonReusableShuffleType(col, node, builder, afterRemap)
}

func determineNonReusableShuffleType(
	col *plan.ColRef,
	node *plan.Node,
	builder *QueryBuilder,
	afterRemap bool,
) {
	if col == nil || builder == nil {
		return
	}
	// Global binding tags and table statistics are no longer addressable after
	// remapping. If reuse was not structurally re-proved above, hash shuffle is
	// the only strategy that can be derived from the late plan safely.
	if afterRemap {
		return
	}

	tableDef, ok := builder.tag2Table[col.RelPos]
	if !ok {
		return
	}

	colName := tableDef.Cols[col.ColPos].Name

	// for shuffle join, if left child is not sorted, the cost will be very high
	// should use complex shuffle type
	if node.NodeType == plan.Node_JOIN {
		leftSorted := true
		if GetSortOrder(tableDef, col.ColPos) != 0 {
			leftSorted = false
		}
		if !maybeSorted(builder.qry.Nodes[node.Children[0]], builder, col.RelPos) {
			leftSorted = false
		}
		if !leftSorted {
			leftCost := builder.qry.Nodes[node.Children[0]].Stats.Outcnt
			rightCost := builder.qry.Nodes[node.Children[1]].Stats.Outcnt
			if node.IsRightJoin {
				// its better for right join to go shuffle, but can not go complex shuffle
				if node.JoinType != plan.Node_DEDUP && leftCost > ShuffleTypeThreshHoldUpperLimit*rightCost {
					return
				}
			} else if leftCost > ShuffleTypeThreshHoldLowerLimit*rightCost {
				node.Stats.HashmapStats.ShuffleTypeForMultiCN = plan.ShuffleTypeForMultiCN_Hybrid
			}
		}
	}

	w := builder.getStatsInfoByTableID(tableDef.TblId)
	if w == nil || w.GetStats() == nil {
		return
	}
	s := w.GetStats()
	colStats := validateColumnStats(s, tableDef, colName)
	colID, ok := findColumnPosition(tableDef, colName)
	if !ok {
		return
	}
	typ := types.T(tableDef.Cols[colID].Typ.Id)
	shuffleRange := s.ShuffleRangeMap[colName]
	ranges := shouldUseShuffleRanges(shuffleRange, colStats.shuffleBoundsSafe)
	rangesSafe := shuffleRangesSafe(typ, shuffleRange, ranges)
	if node.NodeType == plan.Node_AGG {
		if shouldUseHashShuffle(s.ShuffleRangeMap[colName]) {
			return
		}
	}
	if !colStats.shuffleBoundsSafe && !rangesSafe {
		return
	}
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
	if colStats.shuffleBoundsSafe {
		node.Stats.HashmapStats.ShuffleColMin = int64(colStats.minVal)
		node.Stats.HashmapStats.ShuffleColMax = int64(colStats.maxVal)
	} else if minVal, maxVal, ok := sampledRangeFallbackBounds(typ, ranges); ok {
		node.Stats.HashmapStats.ShuffleColMin = minVal
		node.Stats.HashmapStats.ShuffleColMax = maxVal
	} else {
		resetShuffleStrategy(node.Stats.HashmapStats)
		return
	}
	if rangesSafe {
		node.Stats.HashmapStats.Ranges = ranges
	}
	node.Stats.HashmapStats.Nullcnt = int64(colStats.nullCnt)
}

// to determine if join need to go shuffle
func determineShuffleForJoin(node *plan.Node, builder *QueryBuilder) {
	determineShuffleForJoinWithColRefMode(node, builder, false)
}

// shuffleJoinBuildSizeForAdmission keeps the point estimate used by join
// ordering separate from the memory-risk estimate used to admit shuffle. A
// residual FILTER is currently estimated with a fixed heuristic rather than
// column statistics, so its input cardinality is the conservative build-size
// estimate for this physical decision.
func shuffleJoinBuildSizeForAdmission(node *plan.Node, builder *QueryBuilder, afterRemap bool) float64 {
	buildSize := node.Stats.HashmapStats.HashmapSize
	if afterRemap || node.IsRightJoin || len(node.Children) != 2 {
		return buildSize
	}

	build := builder.qry.Nodes[node.Children[1]]
	if build.NodeType != plan.Node_FILTER || len(build.Children) != 1 {
		return buildSize
	}
	input := builder.qry.Nodes[build.Children[0]]
	if input.Stats != nil && input.Stats.Outcnt >= threshHoldForHashShuffle &&
		input.Stats.Outcnt > buildSize {
		return input.Stats.Outcnt
	}
	return buildSize
}

func isSupportedShuffleJoinKeyType(typ int32) bool {
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16,
		types.T_uint64, types.T_uint32, types.T_uint16,
		types.T_varchar, types.T_char, types.T_text:
		return true
	default:
		return false
	}
}

func shuffleJoinCandidateSurvivesRecheck(node *plan.Node, ndv, admissionBuildSize float64) bool {
	hashmapStats := node.Stats.HashmapStats
	if hashmapStats.ShuffleType == plan.ShuffleType_Hash && admissionBuildSize < threshHoldForHashShuffle {
		return false
	}
	if hashmapStats.ShuffleType == plan.ShuffleType_Range && hashmapStats.Ranges == nil &&
		hashmapStats.ShuffleColMax-hashmapStats.ShuffleColMin < 100000 {
		return false
	}
	if hashmapStats.ShuffleMethod != plan.ShuffleMethod_Reuse &&
		ndv >= 0 && ndv < ShuffleThreshHoldOfNDV {
		return false
	}
	if hashmapStats.ShuffleType == plan.ShuffleType_Hash &&
		node.JoinType == plan.Node_DEDUP && node.IsRightJoin {
		return false
	}
	return true
}

// planShuffleJoinCandidate applies the existing candidate-specific shuffle
// rules to a copy of the join statistics. Candidate selection must use the
// same range/hash recheck as the final plan; otherwise an apparently valid
// condition can be rejected later and hide a usable condition after it. The
// returned statistics are reused by the caller so the selected candidate is
// not planned twice.
func planShuffleJoinCandidate(
	node *plan.Node,
	builder *QueryBuilder,
	condition *plan.Expr,
	leftHashCol, rightHashCol *plan.ColRef,
	afterRemap bool,
	candidateIdx int32,
	previousHashmapStats *plan.HashMapStats,
	admissionBuildSize float64,
) (plan.HashMapStats, bool, bool) {
	candidateNode := *node
	candidateStats := *node.Stats
	// HashmapSize and HashOnPK describe the join itself. All shuffle-strategy
	// fields are candidate-local and must not leak from a previous key or pass.
	candidateHashmapStats := plan.HashMapStats{
		HashmapSize:   node.Stats.HashmapStats.HashmapSize,
		HashOnPK:      node.Stats.HashmapStats.HashOnPK,
		ShuffleColIdx: -1,
	}
	candidateNode.Stats = &candidateStats
	candidateStats.HashmapStats = &candidateHashmapStats
	resetShuffleStrategy(&candidateHashmapStats)

	isExprBasedShuffle := leftHashCol == nil || rightHashCol == nil
	if isExprBasedShuffle {
		// Expressions cannot reuse an aggregate column partition. Shortcut the
		// same known-low NDV guard used by the final check.
		if condition.Ndv >= 0 && condition.Ndv < ShuffleThreshHoldOfNDV {
			return candidateHashmapStats, false, false
		}
	} else {
		child, reusable := reusableShuffleChild(leftHashCol, &candidateNode, builder, afterRemap)
		if reusable {
			reuseShuffleStrategy(&candidateHashmapStats, child)
		} else {
			// Reuse is the only exception to the low-NDV guard. Check it once,
			// before looking up range statistics for a candidate that cannot win.
			if condition.Ndv >= 0 && condition.Ndv < ShuffleThreshHoldOfNDV {
				return candidateHashmapStats, false, false
			}
			if !afterRemap || !restoreRangeStrategyAfterRemap(
				&candidateHashmapStats, previousHashmapStats, candidateIdx,
			) {
				determineNonReusableShuffleType(leftHashCol, &candidateNode, builder, afterRemap)
			}
		}
	}
	pointEligible := shuffleJoinCandidateSurvivesRecheck(
		&candidateNode, condition.Ndv, node.Stats.HashmapStats.HashmapSize,
	)
	riskEligible := pointEligible
	if !pointEligible && condition.Ndv >= ShuffleThreshHoldOfNDV {
		riskEligible = shuffleJoinCandidateSurvivesRecheck(
			&candidateNode, condition.Ndv, admissionBuildSize,
		)
	}
	return candidateHashmapStats, pointEligible, riskEligible
}

// selectShuffleJoinCondition keeps the first condition that the current plan
// can actually shuffle on after the existing range/hash recheck. It only scans
// later conditions when an earlier condition is unsupported or rejected by
// those rules. This removes predicate-order-dependent eligibility without
// changing established valid plans.
func selectShuffleJoinCondition(
	node *plan.Node,
	builder *QueryBuilder,
	onList []*plan.Expr,
	leftTags, rightTags map[int32]bool,
	afterRemap bool,
	previousHashmapStats *plan.HashMapStats,
	admissionBuildSize float64,
) (int, plan.HashMapStats, bool) {
	firstSupportedIdx := -1
	var firstSupportedStats plan.HashMapStats
	firstRiskEligibleIdx := -1
	var firstRiskEligibleStats plan.HashMapStats

	for i, condition := range onList {
		fn := condition.GetF()
		if fn == nil || len(fn.Args) != 2 {
			continue
		}

		isEqui := isEquiCond(condition, leftTags, rightTags)
		if afterRemap {
			isEqui = isEquiCond2(condition)
		}
		if !isEqui {
			continue
		}

		leftHashCol, leftType := GetHashColumn(fn.Args[0])
		rightHashCol, rightType := GetHashColumn(fn.Args[1])
		if (leftHashCol == nil && leftType == -1) ||
			(rightHashCol == nil && rightType == -1) ||
			!isSupportedShuffleJoinKeyType(leftType) {
			continue
		}

		candidateStats, pointEligible, riskEligible := planShuffleJoinCandidate(
			node, builder, condition, leftHashCol, rightHashCol, afterRemap,
			int32(i), previousHashmapStats, admissionBuildSize,
		)
		if firstSupportedIdx == -1 {
			firstSupportedIdx = i
			firstSupportedStats = candidateStats
		}
		if pointEligible {
			return i, candidateStats, true
		}
		if riskEligible && firstRiskEligibleIdx == -1 {
			firstRiskEligibleIdx = i
			firstRiskEligibleStats = candidateStats
		}
	}

	if firstRiskEligibleIdx != -1 {
		return firstRiskEligibleIdx, firstRiskEligibleStats, true
	}
	return firstSupportedIdx, firstSupportedStats, false
}

// determineShuffleForJoinWithColRefMode plans join shuffle either before or
// after column remapping. Normal optimizer plans use binding tags to identify
// the two join sides. Late DML/index-maintenance plans are appended after
// createQuery has remapped column references to local RelPos 0/1, so they must
// use the positional form instead.
func determineShuffleForJoinWithColRefMode(node *plan.Node, builder *QueryBuilder, afterRemap bool) {
	var previousHashmapStats *plan.HashMapStats
	if afterRemap {
		previous := *node.Stats.HashmapStats
		previousHashmapStats = &previous
	}
	// do not shuffle by default
	node.Stats.HashmapStats.Shuffle = false
	node.Stats.HashmapStats.ShuffleColIdx = -1
	resetShuffleStrategy(node.Stats.HashmapStats)
	if node.NodeType != plan.Node_JOIN {
		return
	}

	switch node.JoinType {
	case plan.Node_DEDUP:
		dedupJoinCtx := node.GetDedupJoinCtx()
		if len(dedupJoinCtx.GetOldColCaptureList()) > 0 {
			return
		}
		if (node.OnDuplicateAction == plan.Node_FAIL || node.OnDuplicateAction == plan.Node_IGNORE) && len(dedupJoinCtx.GetOldColList()) > 0 {
			return
		}

		if node.IsRightJoin {
			leftChild := builder.qry.Nodes[node.Children[0]]
			if leftChild.Stats.Outcnt <= 200000 {
				return
			}
		} else {
			rightChild := builder.qry.Nodes[node.Children[1]]
			if rightChild.Stats.Outcnt > 320000 && !dedupJoinUsesUnsupportedFloatShuffle(node) {
				// Large DEDUP joins normally use hash shuffle. FLOAT hash shuffle is
				// not supported yet, so those joins stay single-CN and spill locally.
				node.Stats.HashmapStats.Shuffle = true
				node.Stats.HashmapStats.ShuffleColIdx = 0
				node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash
			}

			return
		}

	case plan.Node_INNER, plan.Node_ANTI, plan.Node_SEMI, plan.Node_LEFT, plan.Node_RIGHT, plan.Node_OUTER, plan.Node_MARK:

	default:
		return
	}

	// for now, if join children is merge group or filter, do not allow shuffle
	if dontShuffle(builder.qry.Nodes[node.Children[0]], builder) || dontShuffle(builder.qry.Nodes[node.Children[1]], builder) {
		return
	}

	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = true
	}
	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = true
	}
	if node.JoinType == plan.Node_MARK && !markJoinSupportsShuffle(node, builder, leftTags, rightTags, afterRemap) {
		return
	}
	admissionBuildSize := shuffleJoinBuildSizeForAdmission(node, builder, afterRemap)
	idx, candidateHashmapStats, candidateEligible := selectShuffleJoinCondition(
		node, builder, node.OnList, leftTags, rightTags, afterRemap,
		previousHashmapStats, admissionBuildSize,
	)
	if idx == -1 {
		return
	}
	admittedBuildSize := node.Stats.HashmapStats.HashmapSize
	if node.IsRightJoin {
		if node.Stats.HashmapStats.HashmapSize < threshHoldForRightJoinShuffle {
			return
		}
	} else {
		leftchild := builder.qry.Nodes[node.Children[0]]
		rightchild := builder.qry.Nodes[node.Children[1]]
		factor := math.Pow((leftchild.Stats.Outcnt / rightchild.Stats.Outcnt), 0.4)
		threshold := threshHoldForShuffleJoin * factor
		if admittedBuildSize < threshold && candidateEligible &&
			node.OnList[idx].Ndv >= ShuffleThreshHoldOfNDV {
			admittedBuildSize = admissionBuildSize
		}
		if admittedBuildSize < threshold {
			return
		}
	}

	// get the column of left child
	var expr0, expr1 *plan.Expr
	cond := node.OnList[idx]
	switch condImpl := cond.Expr.(type) {
	case *plan.Expr_F:
		expr0 = condImpl.F.Args[0]
		expr1 = condImpl.F.Args[1]
	}

	leftHashCol, typ := GetHashColumn(expr0)
	if leftHashCol == nil && typ == -1 {
		return
	}
	rightHashCol, rightTyp := GetHashColumn(expr1)
	if rightHashCol == nil && rightTyp == -1 {
		return
	}

	// Only integer and string keys are supported by the shuffle executor.
	isExprBasedShuffle := leftHashCol == nil || rightHashCol == nil
	if isSupportedShuffleJoinKeyType(typ) {
		candidateHashmapStats.ShuffleColIdx = int32(idx)
		candidateHashmapStats.Shuffle = true
		*node.Stats.HashmapStats = candidateHashmapStats
		// For expression-based shuffle (serial_full/serial in join condition):
		// Force hash shuffle because range shuffle depends on column stats (min/max/ranges)
		// which don't apply to expression results. Hash shuffle works universally.
		if isExprBasedShuffle {
			node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash
			if node.OnList[idx].Ndv < 0 {
				node.OnList[idx].Ndv = node.Stats.HashmapStats.HashmapSize
			}
		}
	}

	//recheck shuffle plan
	if node.Stats.HashmapStats.Shuffle {
		if !shuffleJoinCandidateSurvivesRecheck(node, node.OnList[idx].Ndv, admittedBuildSize) {
			node.Stats.HashmapStats.Shuffle = false
		}

		if node.JoinType == plan.Node_DEDUP && node.IsRightJoin && node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Range {
			rightChild := builder.qry.Nodes[node.Children[1]]
			rightChild.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
			rightChild.Stats.HashmapStats.ShuffleColIdx = node.Stats.HashmapStats.ShuffleColIdx
			rightChild.Stats.HashmapStats.ShuffleColMin = node.Stats.HashmapStats.ShuffleColMin
			rightChild.Stats.HashmapStats.ShuffleColMax = node.Stats.HashmapStats.ShuffleColMax
			rightChild.Stats.HashmapStats.Ranges = node.Stats.HashmapStats.Ranges
		}
	}
}

// markJoinSupportsShuffle reports whether bucket-local hash state is enough to
// preserve MARK's three-valued result. Unlike broadcast MARK joins, shuffle
// buckets do not share the global build row count or the global build-NULL
// fact. Requiring every equality operand to be effectively NOT NULL after
// child materialization removes both global dependencies: exact matches are
// co-located and every non-match is FALSE.
func markJoinSupportsShuffle(
	node *plan.Node,
	builder *QueryBuilder,
	leftTags, rightTags map[int32]bool,
	afterRemap bool,
) bool {
	if node == nil || node.JoinType != plan.Node_MARK ||
		len(node.Children) != 2 || len(node.OnList) == 0 {
		return false
	}
	var left, right *plan.Node
	if afterRemap {
		if builder == nil || builder.qry == nil || len(node.Children) != 2 ||
			node.Children[0] < 0 || int(node.Children[0]) >= len(builder.qry.Nodes) ||
			node.Children[1] < 0 || int(node.Children[1]) >= len(builder.qry.Nodes) {
			return false
		}
		left = builder.qry.Nodes[node.Children[0]]
		right = builder.qry.Nodes[node.Children[1]]
	}
	for _, condition := range node.OnList {
		fn := condition.GetF()
		if fn == nil || len(fn.Args) != 2 {
			return false
		}
		isEqui := isEquiCond(condition, leftTags, rightTags)
		if afterRemap {
			isEqui = isEquiCond2(condition)
		}
		if !isEqui {
			return false
		}
		if afterRemap {
			if !IsJoinExprProvenNotNullable(fn.Args[0], left, right) ||
				!IsJoinExprProvenNotNullable(fn.Args[1], left, right) {
				return false
			}
		} else {
			for _, arg := range fn.Args {
				leftRef := exprRefsAnyTag(arg, leftTags)
				rightRef := exprRefsAnyTag(arg, rightTags)
				if leftRef == rightRef {
					return false
				}
				childID := node.Children[1]
				if leftRef {
					childID = node.Children[0]
				}
				if !builder.exprEffectivelyNotNullableBeforeRemap(arg, childID) {
					return false
				}
			}
		}
	}
	return true
}

func (builder *QueryBuilder) exprEffectivelyNotNullableBeforeRemap(expr *plan.Expr, nodeID int32) bool {
	return exprProvenNotNullableWithColResolver(expr, func(colExpr *plan.Expr) bool {
		if colExpr == nil || !colExpr.Typ.NotNullable {
			return false
		}
		return builder.colRefEffectivelyNotNullableBeforeRemap(colExpr.GetCol(), nodeID)
	})
}

func (builder *QueryBuilder) colRefEffectivelyNotNullableBeforeRemap(
	col *plan.ColRef,
	nodeID int32,
) bool {
	if builder == nil || builder.qry == nil || col == nil ||
		nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return false
	}

	node := builder.qry.Nodes[nodeID]
	if node == nil {
		return false
	}

	switch node.NodeType {
	case plan.Node_SINK_SCAN:
		if len(node.BindingTags) > 0 && node.BindingTags[0] == col.RelPos {
			return builder.sinkScanOutputEffectivelyNotNullableBeforeRemap(
				node,
				int(col.ColPos),
			)
		}
	case plan.Node_RECURSIVE_SCAN, plan.Node_RECURSIVE_CTE:
		if len(node.BindingTags) > 0 && node.BindingTags[0] == col.RelPos {
			if col.ColPos < 0 || int(col.ColPos) >= len(node.ProjectList) {
				return false
			}
			return node.ProjectList[col.ColPos].Typ.NotNullable
		}
	}

	if expr, childID, materialized := materializedOutputExprBeforeRemap(node, col); materialized {
		if expr == nil || childID < 0 {
			return false
		}
		return builder.exprEffectivelyNotNullableBeforeRemap(
			expr,
			childID,
		)
	}

	// WINDOW and its PARTITION helper reuse the window binding tag while
	// stacking operators, and FILL passes the time-window binding through.
	// None of those tags proves the referenced value non-NULL: trace the
	// materialized slot into its producer instead of accepting this generic
	// bind-time fast path.
	if node.NodeType != plan.Node_WINDOW &&
		node.NodeType != plan.Node_PARTITION &&
		node.NodeType != plan.Node_FILL {
		for _, bindingTag := range node.BindingTags {
			if bindingTag == col.RelPos {
				return true
			}
		}
	}

	for childIdx, childID := range node.Children {
		if !builder.nodeContainsBindingTag(childID, col.RelPos) {
			continue
		}
		if nodeNullExtendsChild(node, childIdx) {
			return false
		}
		return builder.colRefEffectivelyNotNullableBeforeRemap(col, childID)
	}

	return false
}

func (builder *QueryBuilder) sinkScanOutputEffectivelyNotNullableBeforeRemap(
	node *plan.Node,
	colPos int,
) bool {
	if builder == nil || builder.qry == nil || node == nil ||
		node.NodeType != plan.Node_SINK_SCAN || colPos < 0 ||
		len(node.SourceStep) == 0 {
		return false
	}
	for _, sourceStep := range node.SourceStep {
		if sourceStep < 0 || int(sourceStep) >= len(builder.qry.Steps) ||
			!builder.outputSlotEffectivelyNotNullableBeforeRemap(
				builder.qry.Steps[sourceStep],
				colPos,
			) {
			return false
		}
	}
	return true
}

// materializedOutputExprBeforeRemap resolves output slots whose runtime value
// is computed from a child expression. Bind-time column types are insufficient
// at these boundaries because an outer join below the materializer can make a
// NOT NULL base column nullable.
//
// The bool distinguishes "this node owns the binding but the slot is invalid"
// from "this node does not own the binding". The former must fail closed rather
// than fall through to the generic binding-tag check.
func materializedOutputExprBeforeRemap(
	node *plan.Node,
	col *plan.ColRef,
) (expr *plan.Expr, childID int32, materialized bool) {
	if node == nil || col == nil || len(node.Children) != 1 {
		return nil, -1, false
	}

	childID = node.Children[0]
	switch node.NodeType {
	case plan.Node_PROJECT, plan.Node_MATERIAL:
		if len(node.BindingTags) == 0 || node.BindingTags[0] != col.RelPos {
			return nil, -1, false
		}
		if col.ColPos < 0 || int(col.ColPos) >= len(node.ProjectList) {
			return nil, -1, true
		}
		return node.ProjectList[col.ColPos], childID, true

	case plan.Node_AGG, plan.Node_SAMPLE:
		if len(node.BindingTags) < 2 {
			return nil, -1, false
		}
		if node.BindingTags[0] == col.RelPos {
			if col.ColPos < 0 || int(col.ColPos) >= len(node.GroupBy) {
				return nil, -1, true
			}
			return node.GroupBy[col.ColPos], childID, true
		}
		if node.BindingTags[1] == col.RelPos {
			if col.ColPos < 0 || int(col.ColPos) >= len(node.AggList) {
				return nil, -1, true
			}
			return node.AggList[col.ColPos], childID, true
		}
		return nil, -1, false

	case plan.Node_TIME_WINDOW:
		if len(node.BindingTags) < 2 {
			return nil, -1, false
		}
		if node.BindingTags[0] == col.RelPos {
			if col.ColPos < 0 || int(col.ColPos) >= len(node.AggList) {
				return nil, -1, true
			}
			return node.AggList[col.ColPos], childID, true
		}
		if node.BindingTags[1] == col.RelPos {
			for _, partitionExpr := range node.TimeWindowPartitionBy {
				partitionCol := partitionExpr.GetCol()
				if partitionCol != nil &&
					partitionCol.RelPos == col.RelPos &&
					partitionCol.ColPos == col.ColPos {
					return partitionExpr, childID, true
				}
			}
			return nil, -1, true
		}
		return nil, -1, false

	case plan.Node_WINDOW:
		if len(node.BindingTags) == 0 || node.BindingTags[0] != col.RelPos ||
			col.ColPos != node.GetWindowIdx() {
			return nil, -1, false
		}
		if len(node.WinSpecList) != 1 {
			return nil, -1, true
		}
		return node.WinSpecList[0], childID, true
	}

	return nil, -1, false
}

func (builder *QueryBuilder) nodeContainsBindingTag(nodeID, tag int32) bool {
	if builder == nil || builder.qry == nil ||
		nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return false
	}
	node := builder.qry.Nodes[nodeID]
	if node == nil {
		return false
	}
	for _, bindingTag := range node.BindingTags {
		if bindingTag == tag {
			return true
		}
	}
	for _, childID := range node.Children {
		if builder.nodeContainsBindingTag(childID, tag) {
			return true
		}
	}
	return false
}

func dedupJoinUsesUnsupportedFloatShuffle(node *plan.Node) bool {
	if len(node.OnList) == 0 {
		return false
	}
	condition := node.OnList[0].GetF()
	if condition == nil || len(condition.Args) == 0 {
		return false
	}
	keyType := types.T(condition.Args[0].Typ.Id)
	return keyType == types.T_float32 || keyType == types.T_float64
}

// find mergegroup or mergegroup->filter node
func dontShuffle(node *plan.Node, builder *QueryBuilder) bool {
	if node.NodeType == plan.Node_AGG && !node.Stats.HashmapStats.Shuffle {
		return true
	}
	if node.NodeType == plan.Node_FILTER {
		if builder.qry.Nodes[node.Children[0]].NodeType == plan.Node_AGG && !builder.qry.Nodes[node.Children[0]].Stats.HashmapStats.Shuffle {
			return true
		}
	}
	return false
}

// to determine if groupby need to go shuffle
func determineShuffleForGroupBy(node *plan.Node, builder *QueryBuilder) {
	// do not shuffle by default
	node.Stats.HashmapStats.ShuffleColIdx = -1

	if node.NodeType != plan.Node_AGG {
		return
	}
	if len(node.GroupBy) == 0 {
		return
	}

	child := builder.qry.Nodes[node.Children[0]]

	// for now, if agg children is agg or filter, do not allow shuffle
	if dontShuffle(child, builder) {
		return
	}

	factor := 1 / math.Pow((node.Stats.Outcnt/node.Stats.Selectivity/child.Stats.Outcnt), 0.8)
	if node.Stats.HashmapStats.HashmapSize < threshHoldForShuffleGroup*factor {
		return
	}

	// Any logical group-by column is constant for a physical equality key, so
	// it remains a valid distribution key even when it is omitted from the
	// local hash table. Preserve the highest-NDV choice to avoid skewing a
	// composite primary key on one of its lower-cardinality components.
	idx := 0
	highestNDV := node.GroupBy[idx].Ndv
	for i := range node.GroupBy {
		if node.GroupBy[i].Ndv > highestNDV {
			highestNDV = node.GroupBy[i].Ndv
			idx = i
		}
	}
	if highestNDV < ShuffleThreshHoldOfNDV {
		return
	}

	hashCol, typ := GetHashColumn(node.GroupBy[idx])
	if hashCol == nil {
		return
	}
	//for now ,only support integer and string type
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		node.Stats.HashmapStats.ShuffleColIdx = int32(idx)
		node.Stats.HashmapStats.Shuffle = true
		determineShuffleType(hashCol, node, builder)
		if node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Hash && node.Stats.HashmapStats.HashmapSize < threshHoldForHashShuffle {
			node.Stats.HashmapStats.Shuffle = false
		}
	}

	//shuffle join-> shuffle group ,if they use the same hask key, the group can reuse the shuffle method
	if child.NodeType == plan.Node_JOIN {
		if node.Stats.HashmapStats.Shuffle && child.Stats.HashmapStats.Shuffle {
			// shuffle group can reuse shuffle join
			if node.Stats.HashmapStats.ShuffleType == child.Stats.HashmapStats.ShuffleType && node.Stats.HashmapStats.ShuffleTypeForMultiCN == child.Stats.HashmapStats.ShuffleTypeForMultiCN {
				groupHashCol, _ := GetHashColumn(node.GroupBy[node.Stats.HashmapStats.ShuffleColIdx])
				switch exprImpl := child.OnList[child.Stats.HashmapStats.ShuffleColIdx].Expr.(type) {
				case *plan.Expr_F:
					for _, arg := range exprImpl.F.Args {
						joinHashCol, _ := GetHashColumn(arg)
						if joinHashCol != nil && groupHashCol != nil && groupHashCol.RelPos == joinHashCol.RelPos && groupHashCol.ColPos == joinHashCol.ColPos {
							node.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Reuse
							return
						}
					}
				}
			}
		}
	}

}

// default shuffle type for scan is hash
// for table with primary key, and ndv of first column in primary key is high enough, use range shuffle
// only support integer type
func determineShuffleForScan(node *plan.Node, builder *QueryBuilder) {
	node.Stats.HashmapStats.Shuffle = true
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash
	if builder.optimizerHints != nil && builder.optimizerHints.determineShuffle == 2 { // always go hashshuffle for scan
		return
	}
	w := builder.getStatsInfoByTableID(node.TableDef.TblId)
	if w == nil || w.GetStats() == nil {
		return
	}

	var firstSortColName string
	if node.TableDef.ClusterBy != nil {
		firstSortColName = util.GetClusterByFirstColumn(node.TableDef.ClusterBy.Name)
	} else if node.TableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return
	} else {
		firstSortColName = node.TableDef.Pkey.Names[0]
	}

	s := w.GetStats()
	colStats := validateColumnStats(s, node.TableDef, firstSortColName)
	if !colStats.ndvKnown || colStats.ndv < ShuffleThreshHoldOfNDV {
		return
	}
	firstSortColID, ok := node.TableDef.Name2ColIndex[firstSortColName]
	if !ok {
		return
	}
	typ := types.T(node.TableDef.Cols[firstSortColID].Typ.Id)
	shuffleRange := s.ShuffleRangeMap[firstSortColName]
	ranges := shouldUseShuffleRanges(shuffleRange, colStats.shuffleBoundsSafe)
	rangesSafe := shuffleRangesSafe(typ, shuffleRange, ranges)
	if !colStats.shuffleBoundsSafe && !rangesSafe {
		return
	}
	switch typ {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64,
		types.T_uint32, types.T_uint16, types.T_char, types.T_varchar, types.T_text:
		node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
		node.Stats.HashmapStats.ShuffleColIdx = node.TableDef.Cols[firstSortColID].Typ.Id // actually this is specially used for sort key column type
		if colStats.shuffleBoundsSafe {
			node.Stats.HashmapStats.ShuffleColMin = int64(colStats.minVal)
			node.Stats.HashmapStats.ShuffleColMax = int64(colStats.maxVal)
		} else if minVal, maxVal, ok := sampledRangeFallbackBounds(typ, ranges); ok {
			node.Stats.HashmapStats.ShuffleColMin = minVal
			node.Stats.HashmapStats.ShuffleColMax = maxVal
		} else {
			resetShuffleStrategy(node.Stats.HashmapStats)
			return
		}
		if rangesSafe {
			node.Stats.HashmapStats.Ranges = ranges
		}
		node.Stats.HashmapStats.Nullcnt = int64(colStats.nullCnt)
	}
}

func determineShuffleMethod(nodeID int32, builder *QueryBuilder) {
	determineShuffleMethodWithColRefMode(nodeID, builder, false)
}

func determineShuffleMethodAfterRemap(nodeID int32, builder *QueryBuilder) {
	determineShuffleMethodWithColRefMode(nodeID, builder, true)
}

func determineShuffleMethodWithColRefMode(nodeID int32, builder *QueryBuilder, afterRemap bool) {
	if builder.optimizerHints != nil && builder.optimizerHints.determineShuffle == 1 {
		return
	}
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			determineShuffleMethodWithColRefMode(child, builder, afterRemap)
		}
	}
	switch node.NodeType {
	case plan.Node_AGG:
		determineShuffleForGroupBy(node, builder)
	case plan.Node_TABLE_SCAN:
		determineShuffleForScan(node, builder)
	case plan.Node_JOIN:
		determineShuffleForJoinWithColRefMode(node, builder, afterRemap)
	default:
	}
}

// second pass of determine shuffle
func determineShuffleMethod2(nodeID, parentID int32, builder *QueryBuilder) {
	if builder.optimizerHints != nil && builder.optimizerHints.determineShuffle == 1 {
		return
	}
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			determineShuffleMethod2(child, nodeID, builder)
		}
	}
	if parentID == -1 {
		return
	}
	parent := builder.qry.Nodes[parentID]

	if node.NodeType == plan.Node_JOIN && node.Stats.HashmapStats.ShuffleTypeForMultiCN == plan.ShuffleTypeForMultiCN_Hybrid {
		if parent.NodeType == plan.Node_AGG && parent.Stats.HashmapStats.ShuffleMethod == plan.ShuffleMethod_Reuse {
			return
		}
		if node.Stats.HashmapStats.HashmapSize <= threshHoldForHybirdShuffle {
			node.Stats.HashmapStats.Shuffle = false
			if parent.NodeType == plan.Node_AGG {
				parent.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
			}
		}
	}
}

func shouldUseHashShuffle(s *pb.ShuffleRange) bool {
	if s == nil || math.IsNaN(s.Overlap) {
		return true
	}
	if s.Overlap > overlapThreshold && s.Result == nil {
		return true
	}
	return false
}

func shouldUseShuffleRanges(s *pb.ShuffleRange, completeBoundsSafe bool) []float64 {
	if s == nil || math.IsNaN(s.Uniform) || s.Result == nil {
		return nil
	}
	// Complete min/max can partition a uniform domain directly. When those
	// bounds are unavailable (for example, object-sampled stats), the sampled
	// quantiles are the only range-shuffle boundary with explicit provenance.
	// Use them even for a uniform distribution instead of silently degrading to
	// hash shuffle.
	if !completeBoundsSafe || s.Uniform < uniformThreshold {
		return s.Result
	}
	return nil
}
