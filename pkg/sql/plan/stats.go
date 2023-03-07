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
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"math"
	"sort"
)

func estimateFilterWeight(ctx context.Context, expr *plan.Expr, w float64) float64 {
	switch expr.Typ.Id {
	case int32(types.T_decimal64):
		w += 64
	case int32(types.T_decimal128):
		w += 128
	case int32(types.T_char), int32(types.T_varchar), int32(types.T_text), int32(types.T_json):
		w += 4
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funcImpl := exprImpl.F
		switch funcImpl.Func.GetObjName() {
		case "like":
			w += 10
		case "in":
			w += 5
		case "<", "<=":
			w += 1.1
		default:
			w += 1
		}
		for _, child := range exprImpl.F.Args {
			w += estimateFilterWeight(ctx, child, 0)
		}
	}
	if CheckExprIsMonotonic(ctx, expr) {
		//this is a monotonic filter
		//calc selectivity is too heavy now. will change this in the future
		w *= 0.1
	}
	return w
}

func SortFilterListByStats(ctx context.Context, nodeID int32, builder *QueryBuilder) {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			SortFilterListByStats(ctx, child, builder)
		}
	}
	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		if node.ObjRef != nil && len(node.FilterList) > 1 {
			bat := batch.NewWithSize(0)
			bat.Zs = []int64{1}
			for i := range node.FilterList {
				expr, _ := ConstantFold(bat, DeepCopyExpr(node.FilterList[i]), builder.compCtx.GetProcess())
				if expr != nil {
					node.FilterList[i] = expr
				}
			}
			sort.Slice(node.FilterList, func(i, j int) bool {
				return estimateFilterWeight(builder.GetContext(), node.FilterList[i], 0) <= estimateFilterWeight(builder.GetContext(), node.FilterList[j], 0)
			})
		}
	}
}

func ReCalcNodeStats(nodeID int32, builder *QueryBuilder, recursive bool) {
	node := builder.qry.Nodes[nodeID]
	if recursive {
		if len(node.Children) > 0 {
			for _, child := range node.Children {
				ReCalcNodeStats(child, builder, recursive)
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

	switch node.NodeType {
	case plan.Node_JOIN:
		ndv := math.Min(leftStats.Outcnt, rightStats.Outcnt)
		if ndv < 1 {
			ndv = 1
		}
		//assume all join is not cross join
		//will fix this in the future
		//isCrossJoin := (len(node.OnList) == 0)
		isCrossJoin := false
		selectivity := math.Pow(rightStats.Selectivity, math.Pow(leftStats.Selectivity, 0.5))

		switch node.JoinType {
		case plan.Node_INNER:
			outcnt := leftStats.Outcnt * rightStats.Outcnt / ndv
			if !isCrossJoin {
				outcnt *= selectivity
			}
			node.Stats = &plan.Stats{
				Outcnt:      outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
				Selectivity: selectivity,
			}

		case plan.Node_LEFT:
			outcnt := leftStats.Outcnt * rightStats.Outcnt / ndv
			if !isCrossJoin {
				outcnt *= selectivity
				outcnt += leftStats.Outcnt
			}
			node.Stats = &plan.Stats{
				Outcnt:      outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
				Selectivity: selectivity,
			}

		case plan.Node_RIGHT:
			outcnt := leftStats.Outcnt * rightStats.Outcnt / ndv
			if !isCrossJoin {
				outcnt *= selectivity
				outcnt += rightStats.Outcnt
			}
			node.Stats = &plan.Stats{
				Outcnt:      outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
				Selectivity: selectivity,
			}

		case plan.Node_OUTER:
			outcnt := leftStats.Outcnt * rightStats.Outcnt / ndv
			if !isCrossJoin {
				outcnt *= selectivity
				outcnt += leftStats.Outcnt + rightStats.Outcnt
			}
			node.Stats = &plan.Stats{
				Outcnt:      outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
				Selectivity: selectivity,
			}

		case plan.Node_SEMI, plan.Node_ANTI:
			node.Stats = &plan.Stats{
				Outcnt:      leftStats.Outcnt * selectivity,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
				Selectivity: selectivity,
			}

		case plan.Node_SINGLE, plan.Node_MARK:
			node.Stats = &plan.Stats{
				Outcnt:      leftStats.Outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
				Selectivity: selectivity,
			}
		}

	case plan.Node_AGG:
		if len(node.GroupBy) > 0 {
			node.Stats = &plan.Stats{
				Outcnt:      childStats.Outcnt * 0.2,
				Cost:        childStats.Outcnt,
				HashmapSize: childStats.Outcnt,
				Selectivity: 1,
			}
		} else {
			node.Stats = &plan.Stats{
				Outcnt:      1,
				Cost:        childStats.Cost,
				Selectivity: 1,
			}
		}

	case plan.Node_UNION:
		node.Stats = &plan.Stats{
			Outcnt:      (leftStats.Outcnt + rightStats.Outcnt) * 0.7,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
			Selectivity: 1,
		}
	case plan.Node_UNION_ALL:
		node.Stats = &plan.Stats{
			Outcnt:      leftStats.Outcnt + rightStats.Outcnt,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			Selectivity: 1,
		}
	case plan.Node_INTERSECT:
		node.Stats = &plan.Stats{
			Outcnt:      math.Min(leftStats.Outcnt, rightStats.Outcnt) * 0.5,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
			Selectivity: 1,
		}
	case plan.Node_INTERSECT_ALL:
		node.Stats = &plan.Stats{
			Outcnt:      math.Min(leftStats.Outcnt, rightStats.Outcnt) * 0.7,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
			Selectivity: 1,
		}
	case plan.Node_MINUS:
		minus := math.Max(leftStats.Outcnt, rightStats.Outcnt) - math.Min(leftStats.Outcnt, rightStats.Outcnt)
		node.Stats = &plan.Stats{
			Outcnt:      minus * 0.5,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
			Selectivity: 1,
		}
	case plan.Node_MINUS_ALL:
		minus := math.Max(leftStats.Outcnt, rightStats.Outcnt) - math.Min(leftStats.Outcnt, rightStats.Outcnt)
		node.Stats = &plan.Stats{
			Outcnt:      minus * 0.7,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
			Selectivity: 1,
		}

	case plan.Node_TABLE_SCAN:
		if node.ObjRef != nil {
			expr, num := HandleFiltersForZM(node.FilterList, builder.compCtx.GetProcess())
			node.Stats = builder.compCtx.Stats(node.ObjRef, expr)
			//if there is non monotonic filters
			if num > 0 {
				node.Stats.Selectivity *= 0.15
				node.Stats.Outcnt *= 0.15
			}
		}

	default:
		if len(node.Children) > 0 {
			node.Stats = &plan.Stats{
				Outcnt:      childStats.Outcnt,
				Cost:        childStats.Outcnt,
				Selectivity: childStats.Selectivity,
			}
		} else if node.Stats == nil {
			node.Stats = DefaultStats()
		}
	}
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
