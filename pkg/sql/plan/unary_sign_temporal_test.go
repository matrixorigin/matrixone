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

// MySQL negates DATE/TIME/DATETIME/TIMESTAMP operands in the DOUBLE domain,
// while its parser drops unary plus entirely.
func TestUnarySignTemporalOperands(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name   string
		expr   string
		oid    types.T
		digits int
		want   string
		null   bool
	}{
		{name: "minus date", expr: "-date'2024-06-18'", oid: types.T_float64, digits: -1, want: "-20240618"},
		{name: "minus time", expr: "-time'01:02:03'", oid: types.T_float64, digits: -1, want: "-10203"},
		{name: "minus negative fractional time", expr: "-cast('-01:02:03.50' as time(2))", oid: types.T_float64, digits: 2, want: "10203.50"},
		{name: "minus datetime", expr: "-cast('2024-06-18 01:02:03' as datetime)", oid: types.T_float64, digits: -1, want: "-20240618010203"},
		{name: "minus fractional datetime rounds as double", expr: "-cast('2024-06-18 01:02:03.123456' as datetime(6))", oid: types.T_float64, digits: 6, want: "-20240618010203.125000"},
		{name: "minus fractional timestamp rounds as double", expr: "-cast('2024-06-18 01:02:03.123456' as timestamp(6))", oid: types.T_float64, digits: 6, want: "-20240618010203.125000"},
		{name: "minus null timestamp", expr: "-cast(null as timestamp)", oid: types.T_float64, null: true},
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
			require.Equal(t, tc.null, result.IsConstNull() || result.IsNull(0))
			if tc.null {
				return
			}
			var got string
			switch typ.Oid {
			case types.T_int64:
				got = strconv.FormatInt(vector.GetFixedAtNoTypeCheck[int64](result, 0), 10)
			case types.T_float64:
				got = strconv.FormatFloat(vector.GetFixedAtNoTypeCheck[float64](result, 0), 'f', tc.digits, 64)
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
