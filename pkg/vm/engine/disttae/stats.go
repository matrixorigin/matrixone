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

package disttae

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
)

func getColumnNameFromExpr(expr *plan.Expr) string {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return exprImpl.Col.Name

	case *plan.Expr_F:
		return getColumnNameFromExpr(exprImpl.F.Args[0])
	}
	return ""
}

// deduce selectivity for expr
func deduceSelectivity(expr *plan.Expr, sortKeyName string) float64 {
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
			//if col is clusterby, we assume most of the rows in blocks we read is needed
			//otherwise, deduce selectivity according to ndv
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
			//get the smaller one of two children
			sel = math.Min(deduceSelectivity(exprImpl.F.Args[0], sortKeyName), deduceSelectivity(exprImpl.F.Args[1], sortKeyName))
			return sel
		case "or":
			//get the bigger one of two children
			//if the result is small, tune it up a little bit
			sel1 := deduceSelectivity(exprImpl.F.Args[0], sortKeyName)
			sel2 := deduceSelectivity(exprImpl.F.Args[1], sortKeyName)
			sel = math.Max(sel1, sel2)
			if sel < 0.1 {
				return sel * 1.05
			} else {
				return 1 - (1-sel1)*(1-sel2)
			}
		default:
			//for filters like a>1, no good way to estimate, just 1/3
			return 0.33
		}
	}
	return 1
}

// calculate the stats for scan node.
// we need to get the zonemap from cn, and eval the filters with zonemap
func CalcStats(ctx context.Context, blocks *[][]BlockMeta, expr *plan.Expr, tableDef *plan.TableDef, proc *process.Process, sortKeyName string) (*plan.Stats, error) {
	var blockNum int
	var tableCnt, cost int64
	exprMono := plan2.CheckExprIsMonotonic(ctx, expr)
	columnMap, columns, maxCol := plan2.GetColumnsByExpr(expr, tableDef)
	for i := range *blocks {
		for j := range (*blocks)[i] {
			tableCnt += (*blocks)[i][j].Rows
			if !exprMono || needRead(ctx, expr, (*blocks)[i][j], tableDef, columnMap, columns, maxCol, proc) {
				cost += (*blocks)[i][j].Rows
				blockNum++
			}
		}
	}
	stats := new(plan.Stats)
	stats.BlockNum = int32(blockNum)
	stats.TableCnt = float64(tableCnt)
	stats.Cost = float64(cost)
	stats.Selectivity = deduceSelectivity(expr, sortKeyName)
	stats.Outcnt = stats.Cost * stats.Selectivity
	return stats, nil
}
