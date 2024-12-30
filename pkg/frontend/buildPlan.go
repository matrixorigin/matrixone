// Copyright 2024 Matrix Origin
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

package frontend

import (
	"context"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

func buildPlanV1(reqCtx context.Context, ses FeSession, ctx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
	var ret *plan2.Plan
	var err error

	txnOp := ctx.GetProcess().GetTxnOperator()
	start := time.Now()
	seq := uint64(0)
	if txnOp != nil {
		seq = txnOp.NextSequence()
		txnTrace.GetService(ses.GetService()).AddTxnDurationAction(
			txnOp,
			client.BuildPlanEvent,
			seq,
			0,
			0,
			err)
	}

	defer func() {
		cost := time.Since(start)
		if txnOp != nil {
			txnTrace.GetService(ses.GetService()).AddTxnDurationAction(
				txnOp,
				client.BuildPlanEvent,
				seq,
				0,
				cost,
				err)
		}
		v2.TxnStatementBuildPlanDurationHistogram.Observe(cost.Seconds())
	}()

	// NOTE: The context used by buildPlan comes from the CompilerContext object
	planContext := ctx.GetContext()
	stats := statistic.StatsInfoFromContext(planContext)
	stats.PlanStart()

	crs := new(perfcounter.CounterSet)
	planContext = perfcounter.AttachBuildPlanMarkKey(planContext, crs)
	ctx.SetContext(planContext)
	defer func() {
		stats.AddBuildPlanS3Request(statistic.S3Request{
			List:      crs.FileService.S3.List.Load(),
			Head:      crs.FileService.S3.Head.Load(),
			Put:       crs.FileService.S3.Put.Load(),
			Get:       crs.FileService.S3.Get.Load(),
			Delete:    crs.FileService.S3.Delete.Load(),
			DeleteMul: crs.FileService.S3.DeleteMulti.Load(),
		})
		stats.PlanEnd()
	}()

	isPrepareStmt := false
	if ses != nil {
		accId, err := defines.GetAccountId(reqCtx)
		if err != nil {
			return nil, err
		}
		ses.SetAccountId(accId)

		if len(ses.GetSql()) > 8 {
			prefix := strings.ToLower(ses.GetSql()[:8])
			isPrepareStmt = prefix == "execute " || prefix == "prepare "
		}
	}
	// Handle specific statement types
	if s, ok := stmt.(*tree.Insert); ok {
		if _, ok := s.Rows.Select.(*tree.ValuesClause); ok {
			ret, err = plan2.BuildPlan(ctx, stmt, isPrepareStmt)
			if err != nil {
				return nil, err
			}
		}
	}

	// After building the plan, handle other types of statements
	if ret != nil {
		ret.IsPrepare = isPrepareStmt
		return ret, err
	}

	// Default handling of various statements
	switch stmt := stmt.(type) {
	case *tree.Select, *tree.ParenSelect, *tree.ValuesStatement,
		*tree.Update, *tree.Delete, *tree.Insert,
		*tree.ShowDatabases, *tree.ShowTables, *tree.ShowSequences, *tree.ShowColumns, *tree.ShowColumnNumber,
		*tree.ShowTableNumber, *tree.ShowCreateDatabase, *tree.ShowCreateTable, *tree.ShowIndex,
		*tree.ExplainStmt, *tree.ExplainAnalyze, *tree.ExplainPhyPlan:
		opt := plan2.NewBaseOptimizer(ctx)
		optimized, err := opt.Optimize(stmt, isPrepareStmt) // isPrepareStmt = false
		if err != nil {
			return nil, err
		}

		ret = &plan2.Plan{
			Plan: &plan2.Plan_Query{
				Query: optimized,
			},
		}
	default:
		ret, err = plan2.BuildPlan(ctx, stmt, isPrepareStmt)
	}

	if ret != nil {
		ret.IsPrepare = isPrepareStmt
	}
	return ret, err
}

// buildPlanWithAuthorization wraps the buildPlan function to perform permission checks
// after the plan has been successfully built.
func buildPlanWithAuthorizationV1(reqCtx context.Context, ses FeSession, ctx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
	planContext := ctx.GetContext()
	stats := statistic.StatsInfoFromContext(planContext)

	// Step 1: Call buildPlan to construct the execution plan
	plan, err := buildPlanV1(reqCtx, ses, ctx, stmt)
	if err != nil {
		return nil, err
	}

	// Step 2: Perform permission check after the plan is built
	if ses != nil && ses.GetTenantInfo() != nil && !ses.IsBackgroundSession() {
		authStats, err := authenticateCanExecuteStatementAndPlan(reqCtx, ses.(*Session), stmt, plan)
		if err != nil {
			return nil, err
		}
		// record permission statistics.
		stats.PermissionAuth.Add(&authStats)
	}
	return plan, nil
}
