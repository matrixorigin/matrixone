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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	HashMapSizeForBucket = 250000
	MAXShuffleDOP        = 64
	ShuffleThreshHold    = 50000
)

func SimpleCharHashToRange(bytes []byte, upperLimit uint64) uint64 {
	lenBytes := len(bytes)
	//sample five bytes
	return (uint64(bytes[0])*(uint64(bytes[lenBytes/4])+uint64(bytes[lenBytes/2])+uint64(bytes[lenBytes*3/4])) + uint64(bytes[lenBytes-1])) % upperLimit
}

func SimpleInt64HashToRange(i uint64, upperLimit uint64) uint64 {
	return hashtable.Wyhash64(i) % upperLimit
}

func GetHashColumnIdx(expr *plan.Expr) int {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			idx := GetHashColumnIdx(arg)
			if idx != -1 {
				return idx
			}
		}
	case *plan.Expr_Col:
		return int(exprImpl.Col.ColPos)
	}
	return -1
}

// to judge if groupby need to go shuffle
// if true, return index of which column to shuffle
// else, return -1
func GetShuffleIndexForGroupBy(n *plan.Node) int {
	if n.NodeType != plan.Node_AGG {
		return -1
	}
	if len(n.GroupBy) == 0 {
		return -1
	}
	if n.Stats.HashmapSize < HashMapSizeForBucket {
		return -1
	}
	if n.Stats.Outcnt/n.Stats.Cost < 0.1 {
		return -1
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
		return -1
	}

	if GetHashColumnIdx(n.GroupBy[idx]) == -1 {
		return -1
	}
	//for now ,only support integer and string type
	switch types.T(n.GroupBy[idx].Typ.Id) {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		return idx
	}
	return -1
}

func GetShuffleDop() (dop int) {
	return MAXShuffleDOP
}
