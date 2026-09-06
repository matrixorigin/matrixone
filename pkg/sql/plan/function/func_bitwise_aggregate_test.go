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

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestBitwiseAggregateBinaryOperandWidth(t *testing.T) {
	ctx := context.Background()
	for _, functionName := range []string{"bit_and", "bit_or", "bit_xor"} {
		for _, oid := range []types.T{types.T_binary, types.T_varbinary} {
			for _, width := range []int32{510, 511} {
				resolved, err := GetFunctionByName(ctx, functionName,
					[]types.Type{types.New(oid, width, 0)})
				require.NoError(t, err, "%s(%s(%d))", functionName, oid, width)
				require.Equal(t, oid, resolved.GetReturnType().Oid)
				require.Equal(t, width, resolved.GetReturnType().Width)
			}

			for _, width := range []int32{512, 600} {
				_, err := GetFunctionByName(ctx, functionName,
					[]types.Type{types.New(oid, width, 0)})
				require.Error(t, err, "%s(%s(%d)) must be rejected", functionName, oid, width)
				moErr := moerr.DowncastError(err)
				require.Equal(t, moerr.ErrInvalidBitwiseAggregateOperandsSize, moErr.ErrorCode())
				require.Equal(t, uint16(moerr.ER_INVALID_BITWISE_AGGREGATE_OPERANDS_SIZE), moErr.MySQLCode())
				require.Equal(t,
					"Aggregate bitwise functions cannot accept arguments longer than 511 bytes; consider using the SUBSTRING() function",
					moErr.Error())
			}
		}
	}
}

func TestBitwiseAggregateAcceptsBoundedBinaryExpressions(t *testing.T) {
	ctx := context.Background()
	textInput := types.New(types.T_varchar, 64, 0)

	for _, producer := range []struct {
		name string
		args []types.Type
	}{
		{name: "uuid_to_bin", args: []types.Type{textInput}},
		{name: "inet6_aton", args: []types.Type{textInput}},
	} {
		t.Run(producer.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(ctx, producer.name, producer.args)
			require.NoError(t, err)
			resultType := resolved.GetReturnType()
			require.Equal(t, types.T_varbinary, resultType.Oid)
			require.Equal(t, int32(16), resultType.Width)

			for _, aggregateName := range []string{"bit_and", "bit_or", "bit_xor"} {
				aggregate, err := GetFunctionByName(ctx, aggregateName, []types.Type{resultType})
				require.NoError(t, err, "%s(%s(...))", aggregateName, producer.name)
				require.Equal(t, resultType, aggregate.GetReturnType())
			}
		})
	}

	operand := types.NewWithCharset(types.T_varbinary, 16, 0, types.CharsetBinary)
	for _, operatorName := range []string{"&", "|", "^"} {
		t.Run("binary operator "+operatorName, func(t *testing.T) {
			resolved, err := GetFunctionByName(ctx, operatorName, []types.Type{operand, operand})
			require.NoError(t, err)
			require.Equal(t, types.T_varbinary, resolved.GetReturnType().Oid)
			require.Equal(t, int32(16), resolved.GetReturnType().Width)
		})
	}

	for _, test := range []struct {
		name      string
		operator  string
		left      types.Type
		right     types.Type
		wantWidth int32
		tooWide   bool
	}{
		{
			name:      "bounded result",
			operator:  "|",
			left:      types.NewWithCharset(types.T_varbinary, 16, 0, types.CharsetBinary),
			right:     types.NewWithCharset(types.T_varbinary, 24, 0, types.CharsetBinary),
			wantWidth: 24,
		},
		{
			name:      "oversized result",
			operator:  "^",
			left:      types.NewWithCharset(types.T_varbinary, 16, 0, types.CharsetBinary),
			right:     types.NewWithCharset(types.T_varbinary, 512, 0, types.CharsetBinary),
			wantWidth: 512,
			tooWide:   true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(ctx, test.operator, []types.Type{test.left, test.right})
			require.NoError(t, err)
			require.Equal(t, types.T_varbinary, resolved.GetReturnType().Oid)
			require.Equal(t, test.wantWidth, resolved.GetReturnType().Width)

			_, err = GetFunctionByName(ctx, "bit_or", []types.Type{resolved.GetReturnType()})
			if test.tooWide {
				require.Error(t, err)
				moErr := moerr.DowncastError(err)
				require.Equal(t, moerr.ErrInvalidBitwiseAggregateOperandsSize, moErr.ErrorCode())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
