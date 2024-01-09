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
	"math/bits"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	HashMapSizeForShuffle           = 160000
	threshHoldForHybirdShuffle      = 4000000
	MAXShuffleDOP                   = 64
	ShuffleThreshHoldOfNDV          = 50000
	ShuffleTypeThreshHoldLowerLimit = 16
	ShuffleTypeThreshHoldUpperLimit = 1024
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
	//sample five bytes
	h := (uint64(bytes[0])*(uint64(bytes[lenBytes/4])+uint64(bytes[lenBytes/2])+uint64(bytes[lenBytes*3/4])) + uint64(bytes[lenBytes-1]))
	return hashtable.Int64HashWithFixedSeed(h) % upperLimit
}

func SimpleInt64HashToRange(i uint64, upperLimit uint64) uint64 {
	return hashtable.Int64HashWithFixedSeed(i) % upperLimit
}

func GetRangeShuffleIndexForZM(minVal, maxVal int64, zm objectio.ZoneMap, upplerLimit uint64) uint64 {
	switch zm.GetType() {
	case types.T_int64:
		return GetRangeShuffleIndexSigned(minVal, maxVal, types.DecodeInt64(zm.GetMinBuf()), upplerLimit)
	case types.T_int32:
		return GetRangeShuffleIndexSigned(minVal, maxVal, int64(types.DecodeInt32(zm.GetMinBuf())), upplerLimit)
	case types.T_int16:
		return GetRangeShuffleIndexSigned(minVal, maxVal, int64(types.DecodeInt16(zm.GetMinBuf())), upplerLimit)
	case types.T_uint64:
		return GetRangeShuffleIndexUnsigned(uint64(minVal), uint64(maxVal), types.DecodeUint64(zm.GetMinBuf()), upplerLimit)
	case types.T_uint32:
		return GetRangeShuffleIndexUnsigned(uint64(minVal), uint64(maxVal), uint64(types.DecodeUint32(zm.GetMinBuf())), upplerLimit)
	case types.T_uint16:
		return GetRangeShuffleIndexUnsigned(uint64(minVal), uint64(maxVal), uint64(types.DecodeUint16(zm.GetMinBuf())), upplerLimit)
	case types.T_varchar, types.T_char, types.T_text:
		return GetRangeShuffleIndexUnsigned(uint64(minVal), uint64(maxVal), ByteSliceToUint64(zm.GetMinBuf()), upplerLimit)
	}
	panic("unsupported shuffle type!")
}

func GetRangeShuffleIndexSigned(minVal, maxVal, currentVal int64, upplerLimit uint64) uint64 {
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

func GetRangeShuffleIndexUnsigned(minVal, maxVal, currentVal uint64, upplerLimit uint64) uint64 {
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

func determinShuffleType(col *plan.ColRef, n *plan.Node, builder *QueryBuilder) {
	// hash by default
	n.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash

	if builder == nil {
		return
	}
	tableDef, ok := builder.tag2Table[col.RelPos]
	if !ok {
		return
	}
	colName := tableDef.Cols[col.ColPos].Name

	// for shuffle join, if left child is not sorted, the cost will be very high
	// should use complex shuffle type
	if n.NodeType == plan.Node_JOIN {
		leftSorted := true
		if GetSortOrder(tableDef, colName) != 0 {
			leftSorted = false
		}
		if !maybeSorted(builder.qry.Nodes[n.Children[0]], builder, col.RelPos) {
			leftSorted = false
		}
		if !leftSorted {
			leftCost := builder.qry.Nodes[n.Children[0]].Stats.Outcnt
			rightCost := builder.qry.Nodes[n.Children[1]].Stats.Outcnt
			if n.BuildOnLeft {
				// its better for right join to go shuffle, but can not go complex shuffle
				if n.BuildOnLeft && leftCost > ShuffleTypeThreshHoldUpperLimit*rightCost {
					return
				}
			} else if leftCost > ShuffleTypeThreshHoldLowerLimit*rightCost {
				n.Stats.HashmapStats.ShuffleTypeForMultiCN = plan.ShuffleTypeForMultiCN_Hybrid
			}
		}
	}

	s := getStatsInfoByTableID(tableDef.TblId, builder)
	if s == nil {
		return
	}
	n.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
	n.Stats.HashmapStats.ShuffleColMin = int64(s.MinValMap[colName])
	n.Stats.HashmapStats.ShuffleColMax = int64(s.MaxValMap[colName])

}

// to determine if join need to go shuffle
func determinShuffleForJoin(n *plan.Node, builder *QueryBuilder) {
	// do not shuffle by default
	n.Stats.HashmapStats.ShuffleColIdx = -1
	if n.NodeType != plan.Node_JOIN {
		return
	}
	switch n.JoinType {
	case plan.Node_INNER, plan.Node_ANTI, plan.Node_SEMI, plan.Node_LEFT, plan.Node_RIGHT:
	default:
		return
	}

	// for now, if join children is agg or filter, do not allow shuffle
	if isAggOrFilter(builder.qry.Nodes[n.Children[0]], builder) || isAggOrFilter(builder.qry.Nodes[n.Children[1]], builder) {
		return
	}

	if n.Stats.HashmapStats.HashmapSize < HashMapSizeForShuffle {
		return
	}
	idx := 0
	if !builder.IsEquiJoin(n) {
		return
	}
	leftTags := make(map[int32]emptyType)
	for _, tag := range builder.enumerateTags(n.Children[0]) {
		leftTags[tag] = emptyStruct
	}
	rightTags := make(map[int32]emptyType)
	for _, tag := range builder.enumerateTags(n.Children[1]) {
		rightTags[tag] = emptyStruct
	}
	// for now ,only support the first join condition
	for i := range n.OnList {
		if isEquiCond(n.OnList[i], leftTags, rightTags) {
			idx = i
			break
		}
	}

	//find the highest ndv
	highestNDV := n.OnList[idx].Ndv
	if highestNDV < ShuffleThreshHoldOfNDV {
		return
	}

	// get the column of left child
	var expr *plan.Expr
	cond := n.OnList[idx]
	switch condImpl := cond.Expr.(type) {
	case *plan.Expr_F:
		expr = condImpl.F.Args[0]
	}

	hashCol, typ := GetHashColumn(expr)
	if hashCol == nil {
		return
	}
	//for now ,only support integer and string type
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		n.Stats.HashmapStats.ShuffleColIdx = int32(idx)
		n.Stats.HashmapStats.Shuffle = true
		determinShuffleType(hashCol, n, builder)
	}
}

// find agg or agg->filter node
func isAggOrFilter(n *plan.Node, builder *QueryBuilder) bool {
	if n.NodeType == plan.Node_AGG {
		return true
	} else if n.NodeType == plan.Node_FILTER {
		if builder.qry.Nodes[n.Children[0]].NodeType == plan.Node_AGG {
			return true
		}
	}
	return false
}

// to determine if groupby need to go shuffle
func determinShuffleForGroupBy(n *plan.Node, builder *QueryBuilder) {
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
	if isAggOrFilter(child, builder) {
		return
	}

	if n.Stats.HashmapStats.HashmapSize < HashMapSizeForShuffle {
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
		determinShuffleType(hashCol, n, builder)
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
			// shuffle group can not follow shuffle join, need to reshuffle
			n.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Reshuffle
		}
	}

}

func GetShuffleDop() (dop int) {
	return MAXShuffleDOP
}

// default shuffle type for scan is hash
// for table with primary key, and ndv of first column in primary key is high enough, use range shuffle
// only support integer type
func determinShuffleForScan(n *plan.Node, builder *QueryBuilder) {
	n.Stats.HashmapStats.Shuffle = true
	n.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash
	if n.TableDef.Pkey != nil {
		firstColName := n.TableDef.Pkey.Names[0]
		firstColID := n.TableDef.Name2ColIndex[firstColName]
		s := getStatsInfoByTableID(n.TableDef.TblId, builder)
		if s == nil {
			return
		}
		if s.NdvMap[firstColName] < ShuffleThreshHoldOfNDV {
			return
		}
		switch types.T(n.TableDef.Cols[firstColID].Typ.Id) {
		case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16:
			n.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
			n.Stats.HashmapStats.ShuffleColIdx = int32(n.TableDef.Cols[firstColID].Seqnum)
			n.Stats.HashmapStats.ShuffleColMin = int64(s.MinValMap[firstColName])
			n.Stats.HashmapStats.ShuffleColMax = int64(s.MaxValMap[firstColName])
		case types.T_char, types.T_varchar, types.T_text:
			n.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
			n.Stats.HashmapStats.ShuffleColIdx = int32(n.TableDef.Cols[firstColID].Seqnum)
			n.Stats.HashmapStats.ShuffleColMin = int64(s.MinValMap[firstColName])
			n.Stats.HashmapStats.ShuffleColMax = int64(s.MaxValMap[firstColName])
		}
	}
}

func determineShuffleMethod(nodeID int32, builder *QueryBuilder) {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			determineShuffleMethod(child, builder)
		}
	}
	switch node.NodeType {
	case plan.Node_AGG:
		determinShuffleForGroupBy(node, builder)
	case plan.Node_TABLE_SCAN:
		determinShuffleForScan(node, builder)
	case plan.Node_JOIN:
		determinShuffleForJoin(node, builder)
	default:
	}
}

// second pass of determine shuffle
func determineShuffleMethod2(nodeID, parentID int32, builder *QueryBuilder) {
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
			if parent.NodeType == plan.Node_AGG && parent.Stats.HashmapStats.ShuffleMethod == plan.ShuffleMethod_Reshuffle {
				parent.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
			}
		}
	}
}
