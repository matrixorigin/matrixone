// Copyright 2023 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// partition expression visiter function type
type partitionExprProcessor func(ctx context.Context, tbInfo *plan.TableDef, expr tree.Expr) error

type partitionExprChecker struct {
	processors []partitionExprProcessor
	ctx        context.Context
	tableInfo  *plan.TableDef
	err        error
	columns    []*ColDef // Columns used in partition expressions
}

func newPartitionExprChecker(ctx context.Context, tbInfo *plan.TableDef, processor ...partitionExprProcessor) *partitionExprChecker {
	p := &partitionExprChecker{
		processors: processor,
		ctx:        ctx,
		tableInfo:  tbInfo,
	}
	p.processors = append(p.processors, p.extractColumns)
	return p
}

func (p *partitionExprChecker) Enter(n tree.Expr) (node tree.Expr, skipChildren bool) {
	for _, processor := range p.processors {
		if err := processor(p.ctx, p.tableInfo, n); err != nil {
			p.err = err
			return n, true
		}
	}
	return n, false
}

func (p *partitionExprChecker) Exit(n tree.Expr) (node tree.Expr, ok bool) {
	return n, p.err == nil
}

func (p *partitionExprChecker) extractColumns(ctx context.Context, _ *plan.TableDef, expr tree.Expr) error {
	columnNameExpr, ok := expr.(*tree.UnresolvedName)
	if !ok {
		return nil
	}

	colInfo := findColumnByName(columnNameExpr.Parts[0], p.tableInfo)
	if colInfo == nil {
		return moerr.NewBadFieldError(ctx, columnNameExpr.Parts[0], "partition function")
	}

	p.columns = append(p.columns, colInfo)
	return nil
}

// checkPartitionFuncValid checks partition expression function validly.
func checkPartitionFuncValid(ctx context.Context, tblInfo *plan.TableDef, expr tree.Expr) error {
	if expr == nil {
		return nil
	}
	exprChecker := newPartitionExprChecker(ctx, tblInfo, checkPartitionExprArgs, checkPartitionExprAllowed)
	expr.Accept(exprChecker)
	if exprChecker.err != nil {
		return exprChecker.err
	}
	if len(exprChecker.columns) == 0 {
		return moerr.NewWrongExprInPartitionFunc(ctx)
	}
	return nil
}

// buildPartitionColumns enables the use of multiple columns in partitioning keys
func buildPartitionExpr(ctx context.Context, tblInfo *plan.TableDef, partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, pExpr tree.Expr) error {
	if err := checkPartitionFuncValid(ctx, tblInfo, pExpr); err != nil {
		return err
	}
	planExpr, err := partitionBinder.BindExpr(pExpr, 0, true)
	if err != nil {
		return err
	}
	// TODO: format partition expression
	//fmtCtx := tree.NewFmtCtx2(dialect.MYSQL, tree.RestoreNameBackQuotes)
	//pExpr.Format(fmtCtx)
	//exprStr := fmtCtx.ToString()

	// Temporary operation
	exprStr := tree.String(pExpr, dialect.MYSQL)
	partitionDef.PartitionExpr = &plan.PartitionExpr{
		Expr:    planExpr,
		ExprStr: exprStr,
	}
	return nil
}
