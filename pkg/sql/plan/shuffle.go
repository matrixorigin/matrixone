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

	"github.com/matrixorigin/matrixone/pkg/sql/util"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
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

func SimpleInt64HashToRange(i uint64, upperLimit uint64) uint64 {
	return hashtable.Int64HashWithFixedSeed(i) % upperLimit
}

func shuffleByZonemap(rsp *engine.RangesShuffleParam, zm objectio.ZoneMap) uint64 {
	if !rsp.Init {
		rsp.Init = true
		switch zm.GetType() {
		case types.T_int64, types.T_int32, types.T_int16:
			rsp.ShuffleRangeInt64 = ShuffleRangeReEvalSigned(rsp.Node.Stats.HashmapStats.Ranges, int(rsp.CNCNT), rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
		case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
			rsp.ShuffleRangeUint64 = ShuffleRangeReEvalUnsigned(rsp.Node.Stats.HashmapStats.Ranges, int(rsp.CNCNT), rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
		}
	}

	var shuffleIDX uint64
	if rsp.ShuffleRangeUint64 != nil {
		shuffleIDX = GetRangeShuffleIndexForZMUnsignedSlice(rsp.ShuffleRangeUint64, zm)
	} else if rsp.ShuffleRangeInt64 != nil {
		shuffleIDX = GetRangeShuffleIndexForZMSignedSlice(rsp.ShuffleRangeInt64, zm)
	} else {
		shuffleIDX = GetRangeShuffleIndexForZM(rsp.Node.Stats.HashmapStats.ShuffleColMin, rsp.Node.Stats.HashmapStats.ShuffleColMax, zm, uint64(rsp.CNCNT))
	}
	return shuffleIDX
}

func shuffleByValueExtractedFromZonemap(rsp *engine.RangesShuffleParam, zm objectio.ZoneMap) uint64 {
	t := types.T(rsp.Node.Stats.HashmapStats.ShuffleColIdx) // actually this is specially used for sort key column type
	if !rsp.Init {
		rsp.Init = true
		switch t {
		case types.T_int64, types.T_int32, types.T_int16:
			rsp.ShuffleRangeInt64 = ShuffleRangeReEvalSigned(rsp.Node.Stats.HashmapStats.Ranges, int(rsp.CNCNT), rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
		case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
			rsp.ShuffleRangeUint64 = ShuffleRangeReEvalUnsigned(rsp.Node.Stats.HashmapStats.Ranges, int(rsp.CNCNT), rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
		}
	}

	var shuffleIDX uint64
	if rsp.ShuffleRangeUint64 != nil {
		shuffleIDX = GetRangeShuffleIndexForValuesExtractedFromZMUnsignedSlice(rsp.ShuffleRangeUint64, zm, t)
	} else if rsp.ShuffleRangeInt64 != nil {
		shuffleIDX = GetRangeShuffleIndexForValuesExtractedFromZMSignedSlice(rsp.ShuffleRangeInt64, zm, t)
	} else {
		shuffleIDX = GetRangeShuffleIndexForExtractedZM(rsp.Node.Stats.HashmapStats.ShuffleColMin, rsp.Node.Stats.HashmapStats.ShuffleColMax, zm, uint64(rsp.CNCNT), t)
	}
	return shuffleIDX
}

func ShouldSkipObjByShuffle(rsp *engine.RangesShuffleParam, objstats *objectio.ObjectStats) bool {
	if rsp == nil || rsp.CNCNT <= 1 || rsp.Node == nil {
		return false
	}
	if objstats.GetAppendable() {
		//aobj always shuffle to local CN
		return !rsp.IsLocalCN
	}
	if rsp.Node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Range {
		zm := objstats.SortKeyZoneMap()
		if !zm.IsInited() {
			// an object with all null will send to first CN
			return rsp.CNIDX != 0
		}
		if len(rsp.Node.TableDef.Pkey.Names) == 1 {
			shuffleIDX := shuffleByZonemap(rsp, zm)
			return shuffleIDX != uint64(rsp.CNIDX)
		} else {
			shuffleIDX := shuffleByValueExtractedFromZonemap(rsp, zm)
			return shuffleIDX != uint64(rsp.CNIDX)
		}
	}

	//shuffle by hash
	objID := objstats.ObjectLocation().ObjectId()
	index := SimpleCharHashToRange(objID[:], uint64(rsp.CNCNT))
	return index != uint64(rsp.CNIDX)
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
		//do not support shuffle on expr for now. will improve this in the future
		return nil, -1
	case *plan.Expr_Col:
		return exprImpl.Col, expr.Typ.Id
	}
	return nil, -1
}

func maybeSorted(n *plan.Node, builder *QueryBuilder, tag int32) bool {
	// for scan node, primary key and cluster by may be sorted
	if n.NodeType == plan.Node_TABLE_SCAN {
		return n.BindingTags[0] == tag
	}
	// for inner join, if left child may be sorted, then inner join may be sorted
	if n.NodeType == plan.Node_JOIN && n.JoinType == plan.Node_INNER {
		leftChild := builder.qry.Nodes[n.Children[0]]
		return maybeSorted(leftChild, builder, tag)
	}
	return false
}

func determineShuffleType(col *plan.ColRef, node *plan.Node, builder *QueryBuilder) {
	// hash by default
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash

	if builder == nil {
		return
	}
	tableDef, ok := builder.tag2Table[col.RelPos]

	if !ok {
		//todo: disable shuffle reuse for now
		/*
			child := builder.qry.Nodes[node.Children[0]]
			if child.NodeType == plan.Node_AGG && child.Stats.HashmapStats.Shuffle && col.RelPos == child.BindingTags[0] {
				col = child.GroupBy[col.ColPos].GetCol()
				if col == nil {
					return
				}
				_, ok = builder.tag2Table[col.RelPos]
				if !ok {
					return
				}
				node.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Reuse
				node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
				node.Stats.HashmapStats.HashmapSize = child.Stats.HashmapStats.HashmapSize
				node.Stats.HashmapStats.ShuffleColMin = child.Stats.HashmapStats.ShuffleColMin
				node.Stats.HashmapStats.ShuffleColMax = child.Stats.HashmapStats.ShuffleColMax
				node.Stats.HashmapStats.Ranges = child.Stats.HashmapStats.Ranges
				node.Stats.HashmapStats.Nullcnt = child.Stats.HashmapStats.Nullcnt
			}*/
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
			if node.BuildOnLeft {
				// its better for right join to go shuffle, but can not go complex shuffle
				if node.BuildOnLeft && leftCost > ShuffleTypeThreshHoldUpperLimit*rightCost {
					return
				}
			} else if leftCost > ShuffleTypeThreshHoldLowerLimit*rightCost {
				node.Stats.HashmapStats.ShuffleTypeForMultiCN = plan.ShuffleTypeForMultiCN_Hybrid
			}
		}
	}

	s := builder.getStatsInfoByTableID(tableDef.TblId)
	if s == nil {
		return
	}
	if node.NodeType == plan.Node_AGG {
		if shouldUseHashShuffle(s.ShuffleRangeMap[colName]) {
			return
		}
	}
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
	node.Stats.HashmapStats.ShuffleColMin = int64(s.MinValMap[colName])
	node.Stats.HashmapStats.ShuffleColMax = int64(s.MaxValMap[colName])
	node.Stats.HashmapStats.Ranges = shouldUseShuffleRanges(s.ShuffleRangeMap[colName], colName)
	node.Stats.HashmapStats.Nullcnt = int64(s.NullCntMap[colName])
}

// to determine if join need to go shuffle
func determineShuffleForJoin(n *plan.Node, builder *QueryBuilder) {
	// do not shuffle by default
	n.Stats.HashmapStats.ShuffleColIdx = -1
	if n.NodeType != plan.Node_JOIN {
		return
	}
	switch n.JoinType {
	case plan.Node_DEDUP:
		rightchild := builder.qry.Nodes[n.Children[1]]
		if rightchild.Stats.Outcnt > 320000 {
			//dedup join always go hash shuffle, optimize this in the future
			n.Stats.HashmapStats.Shuffle = true
			n.Stats.HashmapStats.ShuffleColIdx = 0
			n.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash
		}
		return

	case plan.Node_INNER, plan.Node_ANTI, plan.Node_SEMI, plan.Node_LEFT, plan.Node_RIGHT:
	default:
		return
	}

	// for now, if join children is merge group or filter, do not allow shuffle
	if dontShuffle(builder.qry.Nodes[n.Children[0]], builder) || dontShuffle(builder.qry.Nodes[n.Children[1]], builder) {
		return
	}

	idx := 0
	if !builder.IsEquiJoin(n) {
		return
	}
	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(n.Children[0]) {
		leftTags[tag] = true
	}
	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(n.Children[1]) {
		rightTags[tag] = true
	}
	// for now ,only support the first join condition
	for i := range n.OnList {
		if isEquiCond(n.OnList[i], leftTags, rightTags) {
			idx = i
			break
		}
	}

	if n.BuildOnLeft {
		if n.Stats.HashmapStats.HashmapSize < threshHoldForRightJoinShuffle {
			return
		}
	} else {
		leftchild := builder.qry.Nodes[n.Children[0]]
		rightchild := builder.qry.Nodes[n.Children[1]]
		factor := math.Pow((leftchild.Stats.Outcnt / rightchild.Stats.Outcnt), 0.4)
		if n.Stats.HashmapStats.HashmapSize < threshHoldForShuffleJoin*factor {
			return
		}
	}

	// get the column of left child
	var expr0, expr1 *plan.Expr
	cond := n.OnList[idx]
	switch condImpl := cond.Expr.(type) {
	case *plan.Expr_F:
		expr0 = condImpl.F.Args[0]
		expr1 = condImpl.F.Args[1]
	}

	leftHashCol, typ := GetHashColumn(expr0)
	if leftHashCol == nil {
		return
	}
	rightHashCol, _ := GetHashColumn(expr1)
	if rightHashCol == nil {
		return
	}
	//for now ,only support integer and string type
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		n.Stats.HashmapStats.ShuffleColIdx = int32(idx)
		n.Stats.HashmapStats.Shuffle = true
		determineShuffleType(leftHashCol, n, builder)
	}

	//recheck shuffle plan
	if n.Stats.HashmapStats.Shuffle {
		if n.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Hash && n.Stats.HashmapStats.HashmapSize < threshHoldForHashShuffle {
			n.Stats.HashmapStats.Shuffle = false
		}
		if n.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Range && n.Stats.HashmapStats.Ranges == nil && n.Stats.HashmapStats.ShuffleColMax-n.Stats.HashmapStats.ShuffleColMin < 100000 {
			n.Stats.HashmapStats.Shuffle = false
		}
		if n.Stats.HashmapStats.ShuffleMethod != plan.ShuffleMethod_Reuse {
			highestNDV := n.OnList[idx].Ndv
			if highestNDV < ShuffleThreshHoldOfNDV {
				n.Stats.HashmapStats.Shuffle = false
			}
		}
	}
}

// find mergegroup or mergegroup->filter node
func dontShuffle(n *plan.Node, builder *QueryBuilder) bool {
	if n.NodeType == plan.Node_AGG && !n.Stats.HashmapStats.Shuffle {
		return true
	}
	if n.NodeType == plan.Node_FILTER {
		if builder.qry.Nodes[n.Children[0]].NodeType == plan.Node_AGG && !builder.qry.Nodes[n.Children[0]].Stats.HashmapStats.Shuffle {
			return true
		}
	}
	return false
}

// to determine if groupby need to go shuffle
func determineShuffleForGroupBy(n *plan.Node, builder *QueryBuilder) {
	// do not shuffle by default
	n.Stats.HashmapStats.ShuffleColIdx = -1

	if n.NodeType != plan.Node_AGG {
		return
	}
	if len(n.GroupBy) == 0 {
		return
	}

	child := builder.qry.Nodes[n.Children[0]]

	// for now, if agg children is agg or filter, do not allow shuffle
	if dontShuffle(child, builder) {
		return
	}

	factor := 1 / math.Pow((n.Stats.Outcnt/n.Stats.Selectivity/child.Stats.Outcnt), 0.8)
	if n.Stats.HashmapStats.HashmapSize < threshHoldForShuffleGroup*factor {
		return
	}

	//find the highest ndv
	highestNDV := n.GroupBy[0].Ndv
	idx := 0
	for i := range n.GroupBy {
		if n.GroupBy[i].Ndv > highestNDV {
			highestNDV = n.GroupBy[i].Ndv
			idx = i
		}
	}
	if highestNDV < ShuffleThreshHoldOfNDV {
		return
	}

	hashCol, typ := GetHashColumn(n.GroupBy[idx])
	if hashCol == nil {
		return
	}
	//for now ,only support integer and string type
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		n.Stats.HashmapStats.ShuffleColIdx = int32(idx)
		n.Stats.HashmapStats.Shuffle = true
		determineShuffleType(hashCol, n, builder)
		if n.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Hash && n.Stats.HashmapStats.HashmapSize < threshHoldForHashShuffle {
			n.Stats.HashmapStats.Shuffle = false
		}
	}

	//shuffle join-> shuffle group ,if they use the same hask key, the group can reuse the shuffle method
	if child.NodeType == plan.Node_JOIN {
		if n.Stats.HashmapStats.Shuffle && child.Stats.HashmapStats.Shuffle {
			// shuffle group can reuse shuffle join
			if n.Stats.HashmapStats.ShuffleType == child.Stats.HashmapStats.ShuffleType && n.Stats.HashmapStats.ShuffleTypeForMultiCN == child.Stats.HashmapStats.ShuffleTypeForMultiCN {
				groupHashCol, _ := GetHashColumn(n.GroupBy[n.Stats.HashmapStats.ShuffleColIdx])
				switch exprImpl := child.OnList[child.Stats.HashmapStats.ShuffleColIdx].Expr.(type) {
				case *plan.Expr_F:
					for _, arg := range exprImpl.F.Args {
						joinHashCol, _ := GetHashColumn(arg)
						if groupHashCol.RelPos == joinHashCol.RelPos && groupHashCol.ColPos == joinHashCol.ColPos {
							n.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Reuse
							return
						}
					}
				}
			}
		}
	}

}

func GetShuffleDop(ncpu int, lencn int, hashmapSize float64) (dop int) {
	if ncpu <= 4 {
		ncpu = 4
	}
	maxret := ncpu * 4
	if maxret > 64 {
		maxret = 64 // to avoid a hang bug, fix this in the future
	}
	// these magic number comes from hashmap resize factor. see hashtable/common.go, in maxElemCnt function
	ret1 := int(hashmapSize/float64(lencn)/12800000) + 1
	if ret1 >= maxret {
		return maxret
	}

	ret2 := int(hashmapSize/float64(lencn)/6000000) + 1
	if ret2 >= maxret {
		return ret1
	}
	ret3 := int(hashmapSize/float64(lencn)/320000) + 1
	if ret3 <= ncpu/2 {
		return ncpu
	}
	if ret3 >= maxret-1 {
		return maxret
	}
	if ret3 <= ncpu {
		if ncpu*2 > maxret {
			return maxret
		} else {
			return ncpu * 2
		}
	}
	ret4 := (ret3/ncpu + 1) * ncpu
	if ret4 > maxret {
		return maxret
	} else {
		return ret4
	}
}

// default shuffle type for scan is hash
// for table with primary key, and ndv of first column in primary key is high enough, use range shuffle
// only support integer type
func determineShuffleForScan(n *plan.Node, builder *QueryBuilder) {
	n.Stats.HashmapStats.Shuffle = true
	n.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash
	if builder.optimizerHints != nil && builder.optimizerHints.determineShuffle == 2 { // always go hashshuffle for scan
		return
	}
	s := builder.getStatsInfoByTableID(n.TableDef.TblId)
	if s == nil {
		return
	}

	var firstSortColName string
	if n.TableDef.ClusterBy != nil {
		firstSortColName = util.GetClusterByFirstColumn(n.TableDef.ClusterBy.Name)
	} else if n.TableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return
	} else {
		firstSortColName = n.TableDef.Pkey.Names[0]
	}

	if s.NdvMap[firstSortColName] < ShuffleThreshHoldOfNDV {
		return
	}
	firstSortColID, ok := n.TableDef.Name2ColIndex[firstSortColName]
	if !ok {
		return
	}
	switch types.T(n.TableDef.Cols[firstSortColID].Typ.Id) {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_char, types.T_varchar, types.T_text:
		n.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
		n.Stats.HashmapStats.ShuffleColIdx = n.TableDef.Cols[firstSortColID].Typ.Id // actually this is specially used for sort key column type
		n.Stats.HashmapStats.ShuffleColMin = int64(s.MinValMap[firstSortColName])
		n.Stats.HashmapStats.ShuffleColMax = int64(s.MaxValMap[firstSortColName])
		n.Stats.HashmapStats.Ranges = shouldUseShuffleRanges(s.ShuffleRangeMap[firstSortColName], firstSortColName)
		n.Stats.HashmapStats.Nullcnt = int64(s.NullCntMap[firstSortColName])
	}
}

func determineShuffleMethod(nodeID int32, builder *QueryBuilder) {
	if builder.optimizerHints != nil && builder.optimizerHints.determineShuffle == 1 {
		return
	}
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			determineShuffleMethod(child, builder)
		}
	}
	switch node.NodeType {
	case plan.Node_AGG:
		determineShuffleForGroupBy(node, builder)
	case plan.Node_TABLE_SCAN:
		determineShuffleForScan(node, builder)
	case plan.Node_JOIN:
		determineShuffleForJoin(node, builder)
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

func shouldUseShuffleRanges(s *pb.ShuffleRange, colname string) []float64 {
	if s == nil || math.IsNaN(s.Uniform) || s.Result == nil {
		return nil
	}
	if s.Uniform < uniformThreshold {
		return s.Result
	}
	return nil
}
