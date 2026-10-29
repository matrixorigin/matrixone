// Copyright 2026 Matrix Origin
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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestBuildApproxCountRejectsUnsupportedArity(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	for _, sql := range []string{
		"select approx_count(a, b) from select_test.bind_select",
		"select approx_count(a, b, c) from select_test.bind_select",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			_, err = BuildPlan(ctx, stmt, false)
			require.ErrorContains(t, err, "approx_count requires exactly one argument")
		})
	}
}

func TestBuildApproxCountAcceptsOneArgument(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select approx_count(a) from select_test.bind_select", 1)
	require.NoError(t, err)

	_, err = BuildPlan(NewMockCompilerContext(true, newPlanTestProcess(t)), stmt, false)
	require.NoError(t, err)
}
