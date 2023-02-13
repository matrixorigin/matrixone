package disttae

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func CalcStats(ctx context.Context, blocks *[][]BlockMeta, expr *plan.Expr, tableDef *plan.TableDef, proc *process.Process, sortKeyName string) (*plan.Stats, error) {
	var blockNum int
	var tableCnt, cost int64
	exprMono := plan.CheckExprIsMonotonic(ctx, expr)
	columnMap, columns, maxCol := plan.GetColumnsByExpr(expr, tableDef)
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
	stats.Outcnt = float64(cost) * plan.DeduceSelectivity(expr, sortKeyName)
	stats.Selectivity = stats.Outcnt / stats.TableCnt
	return stats, nil
}
