// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// MySQL negates the numeric value of DATE/TIME/DATETIME/TIMESTAMP operands
// (the same value as `0 - x`), and its parser drops unary plus entirely.
func TestUnarySignTemporalOperands(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name  string
		expr  string
		oid   types.T
		scale int32
		want  string
	}{
		{name: "minus date", expr: "-date'2024-06-18'", oid: types.T_int64, want: "-20240618"},
		{name: "minus time", expr: "-time'01:02:03'", oid: types.T_decimal64, want: "-10203"},
		{name: "minus negative fractional time", expr: "-cast('-01:02:03.50' as time(2))", oid: types.T_decimal64, scale: 2, want: "10203.50"},
		{name: "minus datetime", expr: "-cast('2024-06-18 01:02:03' as datetime)", oid: types.T_decimal128, want: "-20240618010203"},
		{name: "minus fractional datetime", expr: "-cast('2024-06-18 01:02:03.250' as datetime(3))", oid: types.T_decimal128, scale: 3, want: "-20240618010203.250"},
		{name: "minus timestamp", expr: "-cast('2024-06-18 01:02:03' as timestamp)", oid: types.T_decimal128, want: "-20240618010203"},
		{name: "minus integer control", expr: "-5", oid: types.T_int64, want: "-5"},
		{name: "plus date", expr: "+date'2024-06-18'", oid: types.T_date, want: "2024-06-18"},
		{name: "plus time", expr: "+time'01:02:03'", oid: types.T_time, want: "01:02:03"},
		{name: "plus datetime", expr: "+cast('2024-06-18 01:02:03' as datetime)", oid: types.T_datetime, want: "2024-06-18 01:02:03"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select "+tc.expr, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, bound, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			typ := result.GetType()
			require.Equal(t, tc.oid, typ.Oid)
			var got string
			switch typ.Oid {
			case types.T_int64:
				got = strconv.FormatInt(vector.GetFixedAtNoTypeCheck[int64](result, 0), 10)
			case types.T_decimal64:
				require.Equal(t, tc.scale, typ.Scale)
				got = vector.GetFixedAtNoTypeCheck[types.Decimal64](result, 0).Format(typ.Scale)
			case types.T_decimal128:
				require.Equal(t, tc.scale, typ.Scale)
				got = vector.GetFixedAtNoTypeCheck[types.Decimal128](result, 0).Format(typ.Scale)
			case types.T_date:
				got = vector.GetFixedAtNoTypeCheck[types.Date](result, 0).String()
			case types.T_time:
				got = vector.GetFixedAtNoTypeCheck[types.Time](result, 0).String2(typ.Scale)
			case types.T_datetime:
				got = vector.GetFixedAtNoTypeCheck[types.Datetime](result, 0).String2(typ.Scale)
			}
			require.Equal(t, tc.want, got)
		})
	}
}
