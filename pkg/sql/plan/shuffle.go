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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
	shuffleDistinctGroupMinNDV      = 64

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
	// Keep the legacy sampled hash for mixed-version shuffle compatibility.
	h := ((uint64(bytes[0])+1)*(uint64(bytes[lenBytes/4])+uint64(bytes[lenBytes/2])+uint64(bytes[lenBytes*3/4])+1) +
		(uint64(bytes[lenBytes-1])+1)*(uint64(bytes[1])+uint64(bytes[lenBytes-2])+1))
	return hashtable.Int64HashWithFixedSeed(h) % upperLimit
}

// StableCharHashToRange maps a complete logical key identically across
// processes and CPU feature sets. MORPCVersion33 freezes this mapping for one
// execution, and its remote Shuffle wire marker makes older CNs fail before
// execution instead of silently choosing a different owner.
func StableCharHashToRange(bytes []byte, upperLimit uint64) uint64 {
	if len(bytes) == 0 {
		return 0
	}
	return hashtable.StableBytesHash(bytes) % upperLimit
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

func shuffleByZonemap(rsp *engine.RangesShuffleParam, zm objectio.ZoneMap, bucketNum int) uint64 {
	if !rsp.Init {
		rsp.Init = true
		switch zm.GetType() {
		case types.T_int64, types.T_int32, types.T_int16:
			rsp.ShuffleRangeInt64 = ShuffleRangeReEvalSigned(rsp.Node.Stats.HashmapStats.Ranges, bucketNum, rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
		case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
			rsp.ShuffleRangeUint64 = ShuffleRangeReEvalUnsigned(rsp.Node.Stats.HashmapStats.Ranges, bucketNum, rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
		}
	}

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
	if !rsp.Init {
		rsp.Init = true
		switch t {
		case types.T_int64, types.T_int32, types.T_int16:
			rsp.ShuffleRangeInt64 = ShuffleRangeReEvalSigned(rsp.Node.Stats.HashmapStats.Ranges, bucketNum, rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
		case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
			rsp.ShuffleRangeUint64 = ShuffleRangeReEvalUnsigned(rsp.Node.Stats.HashmapStats.Ranges, bucketNum, rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
		}
	}

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
	if currentVal <= minVal {
		return 0
	} else if currentVal >= maxVal {
		return upplerLimit - 1
	} else {
		step := uint64(maxVal-minVal) / upplerLimit
		ret := uint64(currentVal-minVal) / step
		if ret >= upplerLimit {
			return upplerLimit - 1
		}
		return ret
	}
}

func GetRangeShuffleIndexUnsignedMinMax(minVal, maxVal, currentVal uint64, upplerLimit uint64) uint64 {
	if currentVal <= minVal {
		return 0
	} else if currentVal >= maxVal {
		return upplerLimit - 1
	} else {
		step := (maxVal - minVal) / upplerLimit
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
	if reusableJoinShuffleChild(col, node, child, builder, afterRemap) {
		return child, true
	}
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

// reusableJoinShuffleChild proves both distribution lineage through a join and
// that the resulting ownership scope satisfies the consumer. A shuffled join
// remains partitioned by its logical-left equality key. Hybrid shuffle owns
// that key only within each CN: another join can reuse it because its build side
// is sent to every CN's matching bucket, but an aggregate needs one owner for
// the key across the whole cluster.
//
// Keep this deliberately narrower than general equivalence propagation:
// right/full preserving joins, expressions, and keys projected from the build
// side fail closed because unmatched rows do not preserve those properties.
func reusableJoinShuffleChild(
	col *plan.ColRef,
	consumer *plan.Node,
	child *plan.Node,
	builder *QueryBuilder,
	afterRemap bool,
) bool {
	if col == nil || consumer == nil || child == nil || builder == nil || builder.qry == nil ||
		child.NodeType != plan.Node_JOIN || child.IsRightJoin ||
		child.Stats == nil || child.Stats.HashmapStats == nil ||
		!child.Stats.HashmapStats.Shuffle {
		return false
	}
	// Join-chain lineage reuse belongs to the outer/ANTI rollout cohort.
	// Aggregate reuse predates that cohort and keeps its established rollback
	// behavior, subject to the stricter ownership check below.
	if consumer.NodeType == plan.Node_JOIN && builder.outerAntiPlanningDisabled() {
		return false
	}
	if consumer.NodeType == plan.Node_AGG &&
		child.Stats.HashmapStats.ShuffleTypeForMultiCN == plan.ShuffleTypeForMultiCN_Hybrid {
		return false
	}
	switch child.JoinType {
	case plan.Node_INNER, plan.Node_LEFT, plan.Node_SEMI, plan.Node_ANTI:
	default:
		return false
	}

	shuffleIdx := child.Stats.HashmapStats.ShuffleColIdx
	if shuffleIdx < 0 || int(shuffleIdx) >= len(child.OnList) {
		return false
	}
	fn := child.OnList[shuffleIdx].GetF()
	if fn == nil || len(fn.Args) != 2 {
		return false
	}

	if afterRemap {
		if col.RelPos != 0 || col.ColPos < 0 || int(col.ColPos) >= len(child.ProjectList) {
			return false
		}
		outputCol := child.ProjectList[col.ColPos].GetCol()
		if outputCol == nil {
			return false
		}
		for _, arg := range fn.Args {
			keyCol := arg.GetCol()
			if keyCol != nil && keyCol.RelPos == 0 &&
				outputCol.RelPos == keyCol.RelPos && outputCol.ColPos == keyCol.ColPos {
				return true
			}
		}
		return false
	}

	if len(child.Children) != 2 {
		return false
	}
	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(child.Children[0]) {
		leftTags[tag] = true
	}
	for _, arg := range fn.Args {
		keyCol := arg.GetCol()
		if keyCol != nil && leftTags[keyCol.RelPos] &&
			col.RelPos == keyCol.RelPos && col.ColPos == keyCol.ColPos {
			return true
		}
	}
	return false
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
	if node.NodeType == plan.Node_AGG {
		if shouldUseHashShuffle(s.ShuffleRangeMap[colName]) {
			return
		}
	}
	minVal, hasMin := s.MinValMap[colName]
	maxVal, hasMax := s.MaxValMap[colName]
	if !hasMin || !hasMax {
		return
	}
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
	node.Stats.HashmapStats.ShuffleColMin = int64(minVal)
	node.Stats.HashmapStats.ShuffleColMax = int64(maxVal)
	node.Stats.HashmapStats.Ranges = shouldUseShuffleRanges(s.ShuffleRangeMap[colName], colName)
	node.Stats.HashmapStats.Nullcnt = int64(s.NullCntMap[colName])
}

// to determine if join need to go shuffle
func determineShuffleForJoin(node *plan.Node, builder *QueryBuilder) {
	determineShuffleForJoinWithColRefMode(node, builder, false)
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

func shuffleJoinCandidateSurvivesRecheck(node *plan.Node, ndv float64) bool {
	hashmapStats := node.Stats.HashmapStats
	if hashmapStats.ShuffleType == plan.ShuffleType_Hash && hashmapStats.HashmapSize < threshHoldForHashShuffle {
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
) (plan.HashMapStats, bool) {
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
			return candidateHashmapStats, false
		}
	} else {
		child, reusable := reusableShuffleChild(leftHashCol, &candidateNode, builder, afterRemap)
		if reusable {
			reuseShuffleStrategy(&candidateHashmapStats, child)
		} else {
			// Reuse is the only exception to the low-NDV guard. Check it once,
			// before looking up range statistics for a candidate that cannot win.
			if condition.Ndv >= 0 && condition.Ndv < ShuffleThreshHoldOfNDV {
				return candidateHashmapStats, false
			}
			if !afterRemap || !restoreRangeStrategyAfterRemap(
				&candidateHashmapStats, previousHashmapStats, candidateIdx,
			) {
				determineNonReusableShuffleType(leftHashCol, &candidateNode, builder, afterRemap)
			}
		}
	}
	return candidateHashmapStats, shuffleJoinCandidateSurvivesRecheck(&candidateNode, condition.Ndv)
}

// selectShuffleJoinCondition prefers a condition that can reuse the probe's
// existing partitioning. Among conditions that require a new shuffle, it keeps
// the first eligible condition so predicate order remains the stable tie-break.
func selectShuffleJoinCondition(
	node *plan.Node,
	builder *QueryBuilder,
	onList []*plan.Expr,
	leftTags, rightTags map[int32]bool,
	afterRemap bool,
	previousHashmapStats *plan.HashMapStats,
) (int, plan.HashMapStats) {
	preferReuse := !builder.outerAntiPlanningDisabled()
	firstSupportedIdx := -1
	var firstSupportedStats plan.HashMapStats
	firstEligibleIdx := -1
	var firstEligibleStats plan.HashMapStats

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

		candidateStats, eligible := planShuffleJoinCandidate(
			node, builder, condition, leftHashCol, rightHashCol, afterRemap,
			int32(i), previousHashmapStats,
		)
		if firstSupportedIdx == -1 {
			firstSupportedIdx = i
			firstSupportedStats = candidateStats
		}
		if eligible {
			if !preferReuse || candidateStats.ShuffleMethod == plan.ShuffleMethod_Reuse {
				return i, candidateStats
			}
			if firstEligibleIdx == -1 {
				firstEligibleIdx = i
				firstEligibleStats = candidateStats
			}
		}
	}

	if firstEligibleIdx != -1 {
		return firstEligibleIdx, firstEligibleStats
	}
	return firstSupportedIdx, firstSupportedStats
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

	case plan.Node_INNER, plan.Node_ANTI, plan.Node_SEMI, plan.Node_LEFT, plan.Node_RIGHT, plan.Node_OUTER, plan.Node_MARK,
		plan.Node_ASOF, plan.Node_ASOF_LEFT:

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
	idx, candidateHashmapStats := selectShuffleJoinCondition(
		node, builder, node.OnList, leftTags, rightTags, afterRemap,
		previousHashmapStats,
	)
	if idx == -1 {
		return
	}
	if node.IsRightJoin {
		if node.Stats.HashmapStats.HashmapSize < threshHoldForRightJoinShuffle {
			return
		}
	} else {
		leftchild := builder.qry.Nodes[node.Children[0]]
		rightchild := builder.qry.Nodes[node.Children[1]]
		factor := math.Pow((leftchild.Stats.Outcnt / rightchild.Stats.Outcnt), 0.4)
		if node.Stats.HashmapStats.HashmapSize < threshHoldForShuffleJoin*factor {
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
		if !shuffleJoinCandidateSurvivesRecheck(node, node.OnList[idx].Ndv) {
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
	if builder != nil {
		if _, ok := builder.distinctKeyLocalPreAggs[node]; ok {
			resetShuffleStrategy(node.Stats.HashmapStats)
			node.Stats.HashmapStats.Shuffle = false
			return
		}
		if idx, ok := builder.distinctKeyShuffleCols[node]; ok &&
			idx >= 0 && int(idx) < len(node.GroupBy) {
			resetShuffleStrategy(node.Stats.HashmapStats)
			node.Stats.HashmapStats.Shuffle = true
			node.Stats.HashmapStats.ShuffleColIdx = idx
			return
		}
	}
	// Non-COUNT DISTINCT states cannot be combined by MergeGroup today. DOP
	// planning therefore keeps the complete aggregate on one CN. A shuffle in
	// front of that single owner adds hashing and dispatch without exposing any
	// parallel aggregate owner, so it is never a useful group-shuffle topology.
	if RequiresSingleStageDistinctAgg(node) {
		node.Stats.HashmapStats.Shuffle = false
		return
	}

	child := builder.qry.Nodes[node.Children[0]]

	// for now, if agg children is agg or filter, do not allow shuffle
	if dontShuffle(child, builder) {
		return
	}

	factor := 1 / math.Pow((node.Stats.Outcnt/node.Stats.Selectivity/child.Stats.Outcnt), 0.8)
	standardShuffle := node.Stats.HashmapStats.HashmapSize >= threshHoldForShuffleGroup*factor

	// The ordinary group estimate only accounts for the final number of groups.
	// A mergeable COUNT(DISTINCT) also retains its exact argument set, which can
	// be orders of magnitude larger. Compare that state with the input using the
	// same reduction-factor model: shuffling a large input is justified only when
	// the retained exact state is itself a material fraction of that input.
	_, distinctStateShuffle := shouldShuffleDistinctState(node, child, builder)
	if !standardShuffle && !distinctStateShuffle {
		return
	}

	// Any logical group-by column is constant for a physical equality key, so
	// it remains a valid distribution key even when it is omitted from the
	// local hash table. Preserve the highest-NDV choice to avoid skewing a
	// composite primary key on one of its lower-cardinality components.
	idx := -1
	highestNDV := float64(0)
	for i := range node.GroupBy {
		// Grouping-set branches replace inactive keys with the rollup
		// constant inside Group. A shuffle must happen before Group, so an
		// inactive key is not a valid distribution key: all rows would be
		// partitioned by their raw values and then collapse to one logical
		// group without a downstream MergeGroup. An empty grouping set has no
		// safe key and must retain the ordinary merge topology.
		if i < len(node.GroupingFlag) && !node.GroupingFlag[i] {
			continue
		}
		if idx < 0 || node.GroupBy[i].Ndv > highestNDV {
			highestNDV = node.GroupBy[i].Ndv
			idx = i
		}
	}
	if idx < 0 {
		return
	}
	minimumGroupNDV := float64(ShuffleThreshHoldOfNDV)
	if distinctStateShuffle {
		// Keep enough logical groups to expose useful parallel ownership.
		// Hash/range shuffle then sends every row for one logical group to exactly
		// one Group operator, removing the exact-state MergeGroup altogether.
		minimumGroupNDV = shuffleDistinctGroupMinNDV
		highestNDV = estimateNDVAfterSelection(highestNDV, child.Stats)
	}
	if highestNDV < minimumGroupNDV {
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
		if node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Hash &&
			node.Stats.HashmapStats.HashmapSize < threshHoldForHashShuffle &&
			!distinctStateShuffle {
			node.Stats.HashmapStats.Shuffle = false
		}
	}

}

func countDistinctStateNDV(node *plan.Node, builder *QueryBuilder) float64 {
	maxNDV := float64(-1)
	if node == nil {
		return maxNDV
	}
	for _, expr := range node.AggList {
		if expr == nil {
			continue
		}
		agg := expr.GetF()
		if agg == nil || agg.Func == nil ||
			uint64(agg.Func.Obj)&function.Distinct == 0 {
			continue
		}
		baseID := int64(uint64(agg.Func.Obj) & function.DistinctMask)
		functionID, _ := function.DecodeOverloadID(baseID)
		if functionID != function.COUNT {
			continue
		}
		for _, arg := range agg.Args {
			if arg == nil {
				continue
			}
			// Aggregate arguments do not normally have Expr.Ndv populated by
			// ReCalcNodeStats. Resolve the estimate through the same table/expression
			// statistics path used elsewhere in the planner. Retain Expr.Ndv as a
			// fallback for remapped or synthesized expressions whose source column
			// is no longer available in tag2Table.
			maxNDV = max(maxNDV, arg.Ndv, getExprNdv(arg, builder))
		}
	}
	return maxNDV
}

func shouldShuffleDistinctState(
	node *plan.Node,
	child *plan.Node,
	builder *QueryBuilder,
) (float64, bool) {
	if child == nil || child.Stats == nil || child.Stats.Outcnt <= 0 {
		return -1, false
	}
	distinctStateRows := estimateNDVAfterSelection(
		countDistinctStateNDV(node, builder), child.Stats)
	if distinctStateRows <= 0 {
		return distinctStateRows, false
	}
	distinctRatio := distinctStateRows / child.Stats.Outcnt
	distinctFactor := 1 / math.Pow(distinctRatio, 0.8)
	return distinctStateRows,
		distinctStateRows >= threshHoldForShuffleGroup*distinctFactor
}

// shouldUseDistinctKeyPreAggregation selects the complement of complete final-
// group ownership. The selected topology preserves the existing local pair
// Group, then distributes only its surviving (group keys, DISTINCT key) rows.
func shouldUseDistinctKeyPreAggregation(node *plan.Node, builder *QueryBuilder) bool {
	if node == nil || builder == nil || builder.qry == nil || builder.compCtx == nil ||
		len(node.Children) != 1 || node.Children[0] < 0 ||
		int(node.Children[0]) >= len(builder.qry.Nodes) {
		return false
	}
	child := builder.qry.Nodes[node.Children[0]]
	distinctRows, shouldShuffle := shouldShuffleDistinctState(node, child, builder)
	if !shouldShuffle || distinctRows < shuffleDistinctGroupMinNDV {
		return false
	}
	if len(node.GroupBy) == 0 {
		return true
	}
	highestGroupNDV := float64(-1)
	activeGroupKeys := 0
	for i, groupBy := range node.GroupBy {
		if i < len(node.GroupingFlag) && !node.GroupingFlag[i] {
			continue
		}
		activeGroupKeys++
		ndv := max(groupBy.Ndv, getExprNdv(groupBy, builder))
		estimatedNDV := estimateNDVAfterSelection(ndv, child.Stats)
		if estimatedNDV <= 0 {
			return false
		}
		highestGroupNDV = max(highestGroupNDV, estimatedNDV)
	}
	return activeGroupKeys > 0 && highestGroupNDV < shuffleDistinctGroupMinNDV
}

func estimateNDVAfterSelection(ndv float64, stats *plan.Stats) float64 {
	if ndv <= 0 || math.IsNaN(ndv) || math.IsInf(ndv, 0) ||
		stats == nil || stats.Outcnt <= 0 ||
		math.IsNaN(stats.Outcnt) || math.IsInf(stats.Outcnt, 0) {
		return -1
	}
	estimate := min(ndv, stats.Outcnt)
	selectivity := stats.Selectivity
	// Zero is also the protobuf/default value for an unavailable estimate. Do
	// not turn missing statistics into a certain empty result.
	if selectivity > 0 && selectivity < 1 &&
		!math.IsNaN(selectivity) && !math.IsInf(selectivity, 0) {
		estimate = min(estimate, ndv*math.Pow(selectivity, 0.8))
	}
	return estimate
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
	if s.NdvMap[firstSortColName] < ShuffleThreshHoldOfNDV {
		return
	}
	minVal, hasMin := s.MinValMap[firstSortColName]
	maxVal, hasMax := s.MaxValMap[firstSortColName]
	if !hasMin || !hasMax {
		return
	}
	firstSortColID, ok := node.TableDef.Name2ColIndex[firstSortColName]
	if !ok {
		return
	}
	switch types.T(node.TableDef.Cols[firstSortColID].Typ.Id) {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64,
		types.T_uint32, types.T_uint16, types.T_char, types.T_varchar, types.T_text:
		node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
		node.Stats.HashmapStats.ShuffleColIdx = node.TableDef.Cols[firstSortColID].Typ.Id // actually this is specially used for sort key column type
		node.Stats.HashmapStats.ShuffleColMin = int64(minVal)
		node.Stats.HashmapStats.ShuffleColMax = int64(maxVal)
		node.Stats.HashmapStats.Ranges = shouldUseShuffleRanges(s.ShuffleRangeMap[firstSortColName], firstSortColName)
		node.Stats.HashmapStats.Nullcnt = int64(s.NullCntMap[firstSortColName])
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
			// Hybrid keeps the probe key local to each CN. It can feed another
			// hybrid join, but a grouped aggregate needs one cluster-global owner
			// for every key. Normalize stale or future invalid reuse decisions.
			parent.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
			parent.Stats.HashmapStats.ShuffleTypeForMultiCN = plan.ShuffleTypeForMultiCN_Simple
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

func shouldUseShuffleRanges(s *pb.ShuffleRange, colname string) []float64 {
	if s == nil || math.IsNaN(s.Uniform) || s.Result == nil {
		return nil
	}
	if s.Uniform < uniformThreshold {
		return s.Result
	}
	return nil
}
