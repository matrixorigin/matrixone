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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestIssue28791PreparedBinaryBitwisePlanRebindsProtocolDomain(t *testing.T) {
	binaryParam := func(value string) ParamValue {
		return ParamValue{
			Value:            value,
			IsBinaryString:   true,
			IsBinaryProtocol: true,
			RuntimeType:      types.T_blob.ToType(),
			HasRuntimeType:   true,
		}
	}
	integerParam := func(value string) ParamValue {
		return ParamValue{
			Value:            value,
			IsBinaryProtocol: true,
			RuntimeType:      types.T_int64.ToType(),
			HasRuntimeType:   true,
		}
	}

	for _, test := range []struct {
		name       string
		query      string
		function   string
		params     []any
		argTypes   []types.T
		resultType types.T
	}{
		{
			name:       "and with two binary params",
			query:      "select hex(? & ?) from nation",
			function:   "&",
			params:     []any{binaryParam(string([]byte{0x12})), binaryParam(string([]byte{0x34}))},
			argTypes:   []types.T{types.T_blob, types.T_blob},
			resultType: types.T_blob,
		},
		{
			name:       "or with two binary params",
			query:      "select hex(? | ?) from nation",
			function:   "|",
			params:     []any{binaryParam(string([]byte{0x12})), binaryParam(string([]byte{0x34}))},
			argTypes:   []types.T{types.T_blob, types.T_blob},
			resultType: types.T_blob,
		},
		{
			name:       "xor with two binary params",
			query:      "select hex(? ^ ?) from nation",
			function:   "^",
			params:     []any{binaryParam(string([]byte{0x12})), binaryParam(string([]byte{0x34}))},
			argTypes:   []types.T{types.T_blob, types.T_blob},
			resultType: types.T_blob,
		},
		{
			name:       "complement binary param",
			query:      "select hex(~?) from nation",
			function:   "unary_tilde",
			params:     []any{binaryParam(string([]byte{0x12, 0x34}))},
			argTypes:   []types.T{types.T_blob},
			resultType: types.T_blob,
		},
		{
			name:       "right shift binary param by integer",
			query:      "select hex(? >> ?) from nation",
			function:   ">>",
			params:     []any{binaryParam(string([]byte{0x80})), integerParam("1")},
			argTypes:   []types.T{types.T_blob, types.T_int64},
			resultType: types.T_blob,
		},
		{
			name:       "left shift binary param by integer",
			query:      "select hex(? << ?) from nation",
			function:   "<<",
			params:     []any{binaryParam(string([]byte{0x12, 0x34})), integerParam("8")},
			argTypes:   []types.T{types.T_blob, types.T_int64},
			resultType: types.T_blob,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, test.query)
			require.True(t, PreparedPlanNeedsRuntimeSpecialization(prepare.Plan),
				"prepared bitwise expression must rebind when a COM_STMT parameter can change its domain")

			filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), prepare.Plan, test.params)
			require.NoError(t, err)
			require.True(t, specialized)

			operator := findPlanFunctionExpr(filled, test.function)
			require.NotNil(t, operator)
			require.Len(t, operator.GetF().Args, len(test.argTypes))
			for i, want := range test.argTypes {
				require.Equal(t, int32(want), operator.GetF().Args[i].Typ.Id,
					"argument %d", i)
			}
			require.Equal(t, int32(test.resultType), operator.Typ.Id)
			require.Equal(t, uint32(types.CharsetBinary), operator.Typ.Charset)
		})
	}

	t.Run("numeric parameters retain numeric bitwise overload", func(t *testing.T) {
		prepare := buildPreparedAggregatePlan(t, "select ? & ? from nation")
		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(
			context.Background(), prepare.Plan,
			[]any{integerParam("18"), integerParam("52")})
		require.NoError(t, err)

		operator := findPlanFunctionExpr(filled, "&")
		require.NotNil(t, operator)
		require.Len(t, operator.GetF().Args, 2)
		require.Equal(t, int32(types.T_int64), operator.GetF().Args[0].Typ.Id)
		require.Equal(t, int32(types.T_int64), operator.GetF().Args[1].Typ.Id)
		require.Equal(t, int32(types.T_uint64), operator.Typ.Id)
		require.Equal(t, types.StringDomainNone,
			types.StaticStringDomain(makeTypeByPlan2Expr(operator)))
	})

	t.Run("explicit casts remain authoritative", func(t *testing.T) {
		prepare := buildPreparedAggregatePlan(t,
			"select CAST(? AS SIGNED) & CAST(? AS SIGNED) from nation")
		preparedOperator := findPlanFunctionExpr(prepare.Plan, "&")
		require.NotNil(t, preparedOperator)
		for i, arg := range preparedOperator.GetF().Args {
			require.True(t, isExplicitPreparedCast(arg), "prepared argument %d: %s", i, arg.String())
		}
		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(
			context.Background(), prepare.Plan,
			[]any{binaryParam(string([]byte{0x12})), binaryParam(string([]byte{0x34}))})
		require.NoError(t, err)

		operator := findPlanFunctionExpr(filled, "&")
		require.NotNil(t, operator)
		require.Len(t, operator.GetF().Args, 2)
		for i, arg := range operator.GetF().Args {
			require.Equal(t, int32(types.T_int64), arg.Typ.Id, "argument %d", i)
		}
		require.Equal(t, int32(types.T_uint64), operator.Typ.Id)
	})
}
