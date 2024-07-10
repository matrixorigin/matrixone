package tools

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func AssertPlan(ctx context.Context, config *AssertConfig, sql string, pattern *MatchPattern) error {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	mock := plan.NewMockOptimizer(false)
	one, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	if err != nil {
		return err
	}
	actual, err := plan.BuildPlan(mock.CurrentContext(), one, false)
	if err != nil {
		return err
	}
	query := actual.GetQuery()
	if query.GetStmtType() != plan2.Query_SELECT {
		return moerr.NewInternalError(ctx, "support select query plan only")
	}
	res, err := assertPlan(ctx, config, query, pattern)
	if err != nil {
		return err
	}
	if !res.IsMatch {
		return moerr.NewInternalError(ctx, "plan dismatch")
	}
	return nil
}

func assertPlan(ctx context.Context, config *AssertConfig, query *plan2.Query, pattern *MatchPattern) (*MatchResult, error) {
	return MatchSteps(ctx, query, pattern)
}
