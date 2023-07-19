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
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	HashMapSizeForBucket = 500000
	MAXShuffleDOP        = 64
	ShuffleThreshHold    = 50000
)

func SimpleCharHashToRange(bytes []byte, upperLimit uint64) uint64 {
	lenBytes := len(bytes)
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
		for _, arg := range exprImpl.F.Args {
			col, typ := GetHashColumn(arg)
			if col != nil {
				return col, typ
			}
		}
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
	n.Stats.ShuffleType = plan.ShuffleType_Hash

	if builder == nil {
		return
	}
	tableDef, ok := builder.tag2Table[col.RelPos]
	if !ok {
		return
	}

	colName := tableDef.Cols[col.ColPos].Name
	if GetSortOrder(tableDef, colName) != 0 {
		return
	}
	if !maybeSorted(builder.qry.Nodes[n.Children[0]], builder, col.RelPos) {
		return
	}
	sc := builder.compCtx.GetStatsCache()
	if sc == nil {
		return
	}
	s := sc.GetStatsInfoMap(tableDef.TblId)
	n.Stats.ShuffleType = plan.ShuffleType_Range
	n.Stats.ShuffleColMin = int64(s.MinValMap[colName])
	n.Stats.ShuffleColMax = int64(s.MaxValMap[colName])

}

// to determine if join need to go shuffle
func determinShuffleForJoin(n *plan.Node, builder *QueryBuilder) {
	// do not shuffle by default
	n.Stats.ShuffleColIdx = -1
	if n.NodeType != plan.Node_JOIN || n.JoinType != plan.Node_INNER {
		return
	}
	// for now ,only support one join condition
	if len(n.OnList) != 1 {
		return
	}
	if !builder.IsEquiJoin(n) {
		return
	}
	if n.Stats.HashmapSize < HashMapSizeForBucket {
		return
	}

	idx := 0
	//find the highest ndv
	highestNDV := n.OnList[idx].Ndv
	if highestNDV < ShuffleThreshHold {
		return
	}

	// get the column of left child
	hashCol, typ := GetHashColumn(n.OnList[idx])
	if hashCol == nil {
		return
	}
	//for now ,only support integer and string type
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16:
		n.Stats.ShuffleColIdx = int32(idx)
		n.Stats.Shuffle = true
		determinShuffleType(hashCol, n, builder)
	case types.T_varchar, types.T_char, types.T_text:
		// for now, do not support hash shuffle join. will support it in the future
		//n.Stats.ShuffleColIdx = int32(idx)
		//n.Stats.Shuffle = true
	}
}

// to determine if groupby need to go shuffle
func determinShuffleForGroupBy(n *plan.Node, builder *QueryBuilder) {
	// do not shuffle by default
	n.Stats.ShuffleColIdx = -1

	if n.NodeType != plan.Node_AGG {
		return
	}
	if len(n.GroupBy) == 0 {
		return
	}
	if n.Stats.HashmapSize < HashMapSizeForBucket {
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
	if highestNDV < ShuffleThreshHold {
		return
	}

	hashCol, typ := GetHashColumn(n.GroupBy[idx])
	if hashCol == nil {
		return
	}
	//for now ,only support integer and string type
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16:
		n.Stats.ShuffleColIdx = int32(idx)
		n.Stats.Shuffle = true
		determinShuffleType(hashCol, n, builder)
	case types.T_varchar, types.T_char, types.T_text:
		n.Stats.ShuffleColIdx = int32(idx)
		n.Stats.Shuffle = true
	}
}

func GetShuffleDop() (dop int) {
	return MAXShuffleDOP
}

// default shuffle type for scan is hash
// for table with primary key, and ndv of first column in primary key is high enough, use range shuffle
// only support integer type
func determinShuffleForScan(n *plan.Node, builder *QueryBuilder) {
	n.Stats.Shuffle = true
	n.Stats.ShuffleType = plan.ShuffleType_Hash
	if n.TableDef.Pkey != nil {
		firstColName := n.TableDef.Pkey.Names[0]
		firstColID := n.TableDef.Name2ColIndex[firstColName]
		sc := builder.compCtx.GetStatsCache()
		if sc == nil {
			return
		}
		s := sc.GetStatsInfoMap(n.TableDef.TblId)
		if s.NdvMap[firstColName] < ShuffleThreshHold {
			return
		}
		switch types.T(n.TableDef.Cols[firstColID].Typ.Id) {
		case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16:
			n.Stats.ShuffleType = plan.ShuffleType_Range
			n.Stats.ShuffleColIdx = int32(n.TableDef.Cols[firstColID].Seqnum)
			n.Stats.ShuffleColMin = int64(s.MinValMap[firstColName])
			n.Stats.ShuffleColMax = int64(s.MaxValMap[firstColName])
		}
	}
}

func findShuffleNode(nodeID int32, builder *QueryBuilder) bool {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			if findShuffleNode(child, builder) {
				return true
			}
		}
	}
	if node.Stats.Shuffle && node.NodeType != plan.Node_TABLE_SCAN {
		return true
	}
	return false
}

func determineShuffleMethod(nodeID int32, builder *QueryBuilder) {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			determineShuffleMethod(child, builder)
			// for now, only one node can go shuffle
			// will fix this in the future
			if findShuffleNode(child, builder) {
				return
			}
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
		node.Stats.ShuffleColIdx = -1
	}
}
