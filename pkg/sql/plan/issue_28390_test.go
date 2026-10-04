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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestIssue28390NumericPrefixBitwiseResults(t *testing.T) {
	tests := []struct {
		sql  string
		want uint64
	}{
		{sql: "select '7x' & 3", want: 3},
		{sql: "select '7x' | 0", want: 7},
		{sql: "select '7x' ^ 1", want: 6},
		{sql: "select '7x' << 1", want: 14},
		{sql: "select '7x' >> 1", want: 3},
		{sql: "select ~'7x'", want: ^uint64(7)},
		{sql: "select '-2tail' & 3", want: 2},
		{sql: "select '-2tail' | 0", want: math.MaxUint64 - 1},
		{sql: "select '-2tail' ^ 1", want: math.MaxUint64},
		{sql: "select 'abc' & 3", want: 0},
		{sql: "select 'abc' | 0", want: 0},
		{sql: "select 'abc' ^ 1", want: 1},
		{sql: "select 'abc' << 1", want: 0},
		{sql: "select 'abc' >> 1", want: 0},
		{sql: "select ~'abc'", want: math.MaxUint64},
		{sql: "select '+7' & 3", want: 3},
		{sql: "select '18446744073709551615tail' | 0", want: math.MaxUint64},
		{sql: "select NULL & 3", want: 0},
	}
	for _, test := range tests {
		t.Run(test.sql, func(t *testing.T) {
			pl, err := runOneExprStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			proc := testutil.NewProc(t)
			defer proc.Free()
			expr := pl.GetQuery().Nodes[1].ProjectList[0]
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, types.T_uint64, result.GetType().Oid)
			if test.sql == "select NULL & 3" {
				require.True(t, result.IsNull(0))
				return
			}
			require.False(t, result.IsNull(0))
			require.Equal(t, test.want, vector.GetFixedAtWithTypeCheck[uint64](result, 0))
		})
	}
}

func TestIssue28390BitwisePlannerCastsTextOnly(t *testing.T) {
	for _, sql := range []string{
		"select '7x' & 3",
		"select '7x' | 3",
		"select '7x' ^ 3",
		"select '7x' << 3",
		"select '7x' >> 3",
		"select ~'7x'",
		"select n_name & 3 from nation",
		"select n_name | 3 from nation",
		"select n_name ^ 3 from nation",
		"select n_name << 3 from nation",
		"select n_name >> 3 from nation",
		"select ~n_name from nation",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := runOneExprStmt(NewMockOptimizer(false), t, sql)
			require.NoError(t, err)
		})
	}

	ctx := context.Background()
	for _, operator := range []string{"&", "|", "^", "<<", ">>"} {
		get, err := function.GetFunctionByName(ctx, operator, []types.Type{
			types.T_varchar.ToType(), types.T_int64.ToType(),
		})
		require.NoError(t, err)
		targets, cast := get.ShouldDoImplicitTypeCast()
		require.True(t, cast)
		require.Equal(t, []types.Type{types.T_int64.ToType(), types.T_int64.ToType()}, targets)
	}
}
