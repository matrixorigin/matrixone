// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

func AssertPlan(ctx context.Context, sql string, pattern *MatchPattern) error {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
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
	res, err := MatchSteps(ctx, query, pattern)
	if err != nil {
		return err
	}
	if !res.IsMatch {
		return moerr.NewInternalError(ctx, "plan dismatch")
	}
	return nil
}
