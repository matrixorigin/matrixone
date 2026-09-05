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
