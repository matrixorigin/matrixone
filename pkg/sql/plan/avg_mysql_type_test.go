// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestCTASAvgExactNumericMetadata(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{
			name: "ordinary aggregate",
			sql: `create table t_avg as select
avg(n_nationkey) as avg_i,
avg(cast(n_nationkey as decimal(20,6))) as avg_d,
avg(cast(n_nationkey as decimal(38,38))) as avg_high_scale,
avg(cast(n_nationkey as double)) as avg_f
from nation`,
		},
		{
			name: "window aggregate",
			sql: `create table t_avg_window as select
avg(n_nationkey) over () as avg_i,
avg(cast(n_nationkey as decimal(20,6))) over () as avg_d,
avg(cast(n_nationkey as decimal(38,38))) over () as avg_high_scale,
avg(cast(n_nationkey as double)) over () as avg_f
from nation`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, test.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			logicPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)

			var visible []*planpb.ColDef
			for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
				if !col.Hidden {
					visible = append(visible, col)
				}
			}
			require.Len(t, visible, 4)
			require.Equal(t, int32(types.T_decimal128), visible[0].Typ.Id)
			require.Equal(t, int32(14), visible[0].Typ.Width)
			require.Equal(t, int32(4), visible[0].Typ.Scale)
			require.Equal(t, int32(types.T_decimal128), visible[1].Typ.Id)
			require.Equal(t, int32(24), visible[1].Typ.Width)
			require.Equal(t, int32(10), visible[1].Typ.Scale)
			require.Equal(t, int32(types.T_decimal256), visible[2].Typ.Id)
			require.Equal(t, int32(42), visible[2].Typ.Width)
			require.Equal(t, int32(38), visible[2].Typ.Scale)
			require.Equal(t, int32(types.T_float64), visible[3].Typ.Id)
		})
	}
}

func TestCTASAvgUsesIntegerExpressionPrecision(t *testing.T) {
	stmt, err := parsers.ParseOne(
		context.Background(), dialect.MYSQL,
		`create table t_avg_literal as select
avg(2) as avg_literal,
avg(-2) as avg_negative_literal,
avg((2)) as avg_parenthesized_literal,
avg(2 + 3) as avg_add,
avg(2 * 3) as avg_mul,
avg(2 % 3) as avg_mod,
avg(n_nationkey) as avg_column,
avg(cast(2 as signed)) as avg_cast
from nation`,
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.NoError(t, err)

	var visible []*planpb.ColDef
	for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 8)
	for _, col := range visible {
		require.Equal(t, int32(types.T_decimal128), col.Typ.Id)
		require.Equal(t, int32(4), col.Typ.Scale)
	}
	// MySQL uses the literal's DECIMAL(1,0) precision, then adds four
	// fractional digits for AVG: DECIMAL(5,4), not BIGINT's DECIMAL(23,4).
	require.Equal(t, int32(5), visible[0].Typ.Width)
	require.Equal(t, int32(5), visible[1].Typ.Width)
	require.Equal(t, int32(5), visible[2].Typ.Width)
	require.Equal(t, int32(6), visible[3].Typ.Width)
	require.Equal(t, int32(6), visible[4].Typ.Width)
	require.Equal(t, int32(5), visible[5].Typ.Width)
	// A column and an explicit integer CAST retain their complete domains.
	require.Equal(t, int32(14), visible[6].Typ.Width)
	require.Equal(t, int32(23), visible[7].Typ.Width)
}

func TestCTASAvgBoundsWideIntegerExpressionPrecision(t *testing.T) {
	stmt, err := parsers.ParseOne(
		context.Background(), dialect.MYSQL,
		`create table t_avg_wide_literal as select
avg(0 * 100000000 * 100000000 * 100000000 * 100000) as avg_at_decimal128_limit,
avg(0 * 100000000 * 100000000 * 100000000 * 1000000) as avg_above_decimal128_limit,
avg(0 * 100000000 * 100000000 * 100000000 * 100000000) as avg_zero,
avg(0 * 100000000 * 100000000 * 100000000 * 100000000 * 100000000 * 100000000 * 100000000) as avg_capped
from nation`,
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.NoError(t, err)

	var visible []*planpb.ColDef
	for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 4)
	// The first expression has precision 34, so AVG reaches the Decimal128
	// boundary (38,4); the next two promote to valid Decimal256 metadata even
	// though their values are exactly zero.
	require.Equal(t, int32(types.T_decimal128), visible[0].Typ.Id)
	require.Equal(t, int32(38), visible[0].Typ.Width)
	require.Equal(t, int32(4), visible[0].Typ.Scale)
	require.Equal(t, int32(types.T_decimal256), visible[1].Typ.Id)
	require.Equal(t, int32(39), visible[1].Typ.Width)
	require.Equal(t, int32(4), visible[1].Typ.Scale)
	require.Equal(t, int32(types.T_decimal256), visible[2].Typ.Id)
	require.Equal(t, int32(41), visible[2].Typ.Width)
	require.Equal(t, int32(4), visible[2].Typ.Scale)
	require.Equal(t, int32(types.T_decimal256), visible[3].Typ.Id)
	require.Equal(t, int32(65), visible[3].Typ.Width)
	require.Equal(t, int32(4), visible[3].Typ.Scale)
}
