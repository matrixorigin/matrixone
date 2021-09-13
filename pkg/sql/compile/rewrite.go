// Copyright 2021 Matrix Origin
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

package compile

import (
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/dedup"
	"matrixone/pkg/sql/op/group"
	"matrixone/pkg/sql/op/innerJoin"
	"matrixone/pkg/sql/op/limit"
	"matrixone/pkg/sql/op/naturalJoin"
	"matrixone/pkg/sql/op/offset"
	"matrixone/pkg/sql/op/order"
	"matrixone/pkg/sql/op/product"
	"matrixone/pkg/sql/op/projection"
	"matrixone/pkg/sql/op/relation"
	"matrixone/pkg/sql/op/restrict"
	"matrixone/pkg/sql/op/summarize"
	"matrixone/pkg/sql/op/top"
)

func rewrite(o op.OP, cnt int) op.OP {
	switch n := o.(type) {
	case *top.Top:
		cnt--
		if cnt == 0 {
			n.IsPD = true
		}
		n.Prev = rewrite(n.Prev, cnt)
		return n
	case *dedup.Dedup:
		cnt--
		if cnt == 0 {
			n.IsPD = true
		}
		n.Prev = rewrite(n.Prev, cnt)
		return n
	case *group.Group:
		cnt--
		if cnt == 0 {
			n.IsPD = true
		}
		n.Prev = rewrite(n.Prev, cnt)
		return n
	case *limit.Limit:
		cnt--
		if cnt == 0 {
			n.IsPD = true
		}
		if prev, ok := n.Prev.(*offset.Offset); ok {
			prev.Prev = rewrite(prev.Prev, cnt)
			n.Prev = prev
			return n
		}
		n.Prev = rewrite(n.Prev, cnt)
		return n
	case *order.Order:
		cnt--
		if cnt == 0 {
			n.IsPD = true
		}
		n.Prev = rewrite(n.Prev, cnt)
		return n
	case *product.Product:
		cnt--
		if cnt == 0 {
			n.IsPD = true
		}
		n.R = rewrite(n.R, cnt)
		n.S = rewrite(n.S, cnt)
		return n
	case *innerJoin.Join:
		cnt--
		if cnt == 0 {
			n.IsPD = true
		}
		n.R = rewrite(n.R, cnt)
		n.S = rewrite(n.S, cnt)
		return n
	case *naturalJoin.Join:
		cnt--
		if cnt == 0 {
			n.IsPD = true
		}
		n.R = rewrite(n.R, cnt)
		n.S = rewrite(n.S, cnt)
		return n
	case *relation.Relation:
		return n
	case *restrict.Restrict:
		if cnt == 0 {
			n.IsPD = true
		}
		rewrite(n.Prev, cnt)
	case *summarize.Summarize:
		cnt--
		if cnt == 0 {
			n.IsPD = true
		}
		n.Prev = rewrite(n.Prev, cnt)
		return n
	case *projection.Projection:
		if cnt == 0 {
			n.IsPD = true
		}
		n.Prev = rewrite(n.Prev, cnt)
		return n
	}
	return o
}

func mergeCount(o op.OP, cnt int) int {
	switch n := o.(type) {
	case *top.Top:
		return mergeCount(n.Prev, cnt+1)
	case *dedup.Dedup:
		return mergeCount(n.Prev, cnt+1)
	case *group.Group:
		return mergeCount(n.Prev, cnt+1)
	case *limit.Limit:
		if prev, ok := n.Prev.(*offset.Offset); ok {
			return mergeCount(prev.Prev, cnt+1)
		}
		return mergeCount(n.Prev, cnt+1)
	case *order.Order:
		return mergeCount(n.Prev, cnt+1)
	case *product.Product:
		return mergeCount(n.R, cnt) + mergeCount(n.S, cnt) + 1
	case *innerJoin.Join:
		return mergeCount(n.R, cnt) + mergeCount(n.S, cnt) + 1
	case *naturalJoin.Join:
		return mergeCount(n.R, cnt) + mergeCount(n.S, cnt) + 1
	case *relation.Relation:
		return cnt
	case *restrict.Restrict:
		return mergeCount(n.Prev, cnt)
	case *summarize.Summarize:
		return mergeCount(n.Prev, cnt+1)
	case *projection.Projection:
		return mergeCount(n.Prev, cnt)
	}
	return cnt
}
