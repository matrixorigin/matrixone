package tools

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func Setup(sql string, pattern *MatchPattern) (*plan.Plan, error) {
	mock := plan.NewMockOptimizer(false)
	one, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	if err != nil {
		return nil, err
	}
	buildPlan, err := plan.BuildPlan(mock.CurrentContext(), one, false)
	if err != nil {
		return nil, err
	}
	return buildPlan, nil
}
