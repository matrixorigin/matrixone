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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	HashMapSizeForBucket = 250000
	MAXShuffleDOP        = 16
)

func SimpleHashToRange(bytes []byte, upperLimit int) int {
	lenBytes := len(bytes)
	//sample five bytes
	return (int(bytes[0])*(int(bytes[lenBytes/4])+int(bytes[lenBytes/2])+int(bytes[lenBytes*3/4])) + int(bytes[lenBytes-1])) % upperLimit
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

func NeedShuffle(n *plan.Node) bool {
	if n.NodeType != plan.Node_AGG {
		return false
	}
	if len(n.GroupBy) != 1 {
		return false
	}
	if n.Stats.HashmapSize < HashMapSizeForBucket {
		return false
	}
	if n.Stats.Outcnt/n.Stats.Cost < 0.1 {
		return false
	}

	if GetHashColumnIdx(n.GroupBy[0]) == -1 {
		return false
	}
	//for now ,only support integer type
	return n.GroupBy[0].Typ.Id == int32(types.T_int64) || n.GroupBy[0].Typ.Id == int32(types.T_int32)
}

func GetShuffleDop(n *plan.Node) (dop int) {
	dop = int(n.Stats.HashmapSize/HashMapSizeForBucket) + 1
	if dop > MAXShuffleDOP {
		dop = MAXShuffleDOP
	}
	return
}
