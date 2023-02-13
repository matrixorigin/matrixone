package plan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"math"
)

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
		switch node.JoinType {
		case plan.Node_INNER:
			outcnt := leftStats.Outcnt * rightStats.Outcnt / ndv
			if len(node.OnList) > 0 {
				outcnt *= 0.1
			}
			node.Stats = &plan.Stats{
				Outcnt:      outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
			}

		case plan.Node_LEFT:
			outcnt := leftStats.Outcnt * rightStats.Outcnt / ndv
			if len(node.OnList) > 0 {
				outcnt *= 0.1
				outcnt += leftStats.Outcnt
			}
			node.Stats = &plan.Stats{
				Outcnt:      outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
			}

		case plan.Node_RIGHT:
			outcnt := leftStats.Outcnt * rightStats.Outcnt / ndv
			if len(node.OnList) > 0 {
				outcnt *= 0.1
				outcnt += rightStats.Outcnt
			}
			node.Stats = &plan.Stats{
				Outcnt:      outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
			}

		case plan.Node_OUTER:
			outcnt := leftStats.Outcnt * rightStats.Outcnt / ndv
			if len(node.OnList) > 0 {
				outcnt *= 0.1
				outcnt += leftStats.Outcnt + rightStats.Outcnt
			}
			node.Stats = &plan.Stats{
				Outcnt:      outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
			}

		case plan.Node_SEMI, plan.Node_ANTI:
			node.Stats = &plan.Stats{
				Outcnt:      leftStats.Outcnt * .7,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
			}

		case plan.Node_SINGLE, plan.Node_MARK:
			node.Stats = &plan.Stats{
				Outcnt:      leftStats.Outcnt,
				Cost:        leftStats.Cost + rightStats.Cost,
				HashmapSize: rightStats.Outcnt,
			}
		}

	case plan.Node_AGG:
		if len(node.GroupBy) > 0 {
			node.Stats = &plan.Stats{
				Outcnt:      childStats.Outcnt * 0.1,
				Cost:        childStats.Outcnt,
				HashmapSize: childStats.Outcnt,
			}
		} else {
			node.Stats = &plan.Stats{
				Outcnt: 1,
				Cost:   childStats.Cost,
			}
		}

	case plan.Node_UNION:
		node.Stats = &plan.Stats{
			Outcnt:      (leftStats.Outcnt + rightStats.Outcnt) * 0.7,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
		}
	case plan.Node_UNION_ALL:
		node.Stats = &plan.Stats{
			Outcnt: leftStats.Outcnt + rightStats.Outcnt,
			Cost:   leftStats.Outcnt + rightStats.Outcnt,
		}
	case plan.Node_INTERSECT:
		node.Stats = &plan.Stats{
			Outcnt:      math.Min(leftStats.Outcnt, rightStats.Outcnt) * 0.5,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
		}
	case plan.Node_INTERSECT_ALL:
		node.Stats = &plan.Stats{
			Outcnt:      math.Min(leftStats.Outcnt, rightStats.Outcnt) * 0.7,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
		}
	case plan.Node_MINUS:
		minus := math.Max(leftStats.Outcnt, rightStats.Outcnt) - math.Min(leftStats.Outcnt, rightStats.Outcnt)
		node.Stats = &plan.Stats{
			Outcnt:      minus * 0.5,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
		}
	case plan.Node_MINUS_ALL:
		minus := math.Max(leftStats.Outcnt, rightStats.Outcnt) - math.Min(leftStats.Outcnt, rightStats.Outcnt)
		node.Stats = &plan.Stats{
			Outcnt:      minus * 0.7,
			Cost:        leftStats.Outcnt + rightStats.Outcnt,
			HashmapSize: rightStats.Outcnt,
		}

	case plan.Node_TABLE_SCAN:
		if node.ObjRef != nil {
			node.Stats = builder.compCtx.Stats(node.ObjRef, HandleFiltersForZM(node.FilterList, builder.compCtx.GetProcess()))
			node.Stats.Selectivity = node.Stats.Outcnt / node.Stats.Cost
		}

	default:
		if len(node.Children) > 0 {
			node.Stats = &plan.Stats{
				Outcnt: childStats.Outcnt,
				Cost:   childStats.Outcnt,
			}
		} else if node.Stats == nil {
			node.Stats = &plan.Stats{
				Outcnt: 1000,
				Cost:   1000000,
			}
		}
	}
}

func DeduceSelectivity(expr *plan.Expr, sortKeyName string) float64 {
	if expr == nil {
		return 1
	}
	var sel float64
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funcName := exprImpl.F.Func.ObjName
		switch funcName {
		case "=":
			sortOrder := util.GetClusterByColumnOrder(sortKeyName, getColumnNameFromExpr(expr))
			if sortOrder == 0 {
				return 0.9
			} else if sortOrder == 1 {
				return 0.6
			} else if sortOrder == 2 {
				return 0.3
			} else {
				return 0.01
			}
		case "and":
			sel = math.Min(DeduceSelectivity(exprImpl.F.Args[0], sortKeyName), DeduceSelectivity(exprImpl.F.Args[1], sortKeyName))
			return sel
		case "or":
			sel1 := DeduceSelectivity(exprImpl.F.Args[0], sortKeyName)
			sel2 := DeduceSelectivity(exprImpl.F.Args[1], sortKeyName)
			sel = math.Max(sel1, sel2)
			if sel < 0.1 {
				return sel * 1.05
			} else {
				return 1 - (1-sel1)*(1-sel2)
			}
		default:
			return 0.33
		}
	}
	return 1
}

func CalcScanStats([][]disttae.BlockMeta) {
	
}
