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

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestJSONObjectAggNumericKeyResolution(t *testing.T) {
	ctx := context.Background()
	valueType := types.T_varchar.ToType()

	for _, keyType := range []types.Type{
		types.T_int8.ToType(),
		types.T_int16.ToType(),
		types.T_int32.ToType(),
		types.T_int64.ToType(),
		types.T_uint8.ToType(),
		types.T_uint16.ToType(),
		types.T_uint32.ToType(),
		types.T_uint64.ToType(),
		types.T_float32.ToType(),
		types.T_float64.ToType(),
		types.New(types.T_decimal64, 18, 4),
		types.New(types.T_decimal128, 38, 6),
		types.New(types.T_decimal256, 76, 8),
	} {
		t.Run(keyType.Oid.String(), func(t *testing.T) {
			resolved, err := GetFunctionByName(ctx, "json_objectagg", []types.Type{keyType, valueType})
			require.NoError(t, err)
			targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast)
			require.Equal(t, []types.Type{types.T_varchar.ToType(), valueType}, targets)
		})
	}
}

func TestJSONObjectAggKeyTypeBoundaries(t *testing.T) {
	ctx := context.Background()
	valueType := types.T_varchar.ToType()

	t.Run("any becomes varchar", func(t *testing.T) {
		resolved, err := GetFunctionByName(ctx, "json_objectagg", []types.Type{types.T_any.ToType(), valueType})
		require.NoError(t, err)
		targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
		require.True(t, shouldCast)
		require.Equal(t, []types.Type{valueType, valueType}, targets)
	})

	for _, keyType := range []types.Type{
		types.T_char.ToType(),
		types.T_varchar.ToType(),
		types.T_text.ToType(),
		types.T_binary.ToType(),
		types.T_varbinary.ToType(),
		types.T_blob.ToType(),
	} {
		t.Run("string/"+keyType.Oid.String(), func(t *testing.T) {
			resolved, err := GetFunctionByName(ctx, "json_objectagg", []types.Type{keyType, valueType})
			require.NoError(t, err)
			targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast)
			require.Equal(t, keyType, targets[0])
			require.Equal(t, valueType, targets[1])
		})
	}

	for _, keyType := range []types.Type{
		types.T_bit.ToType(),
		types.T_year.ToType(),
		types.T_datetime.ToType(),
		types.T_json.ToType(),
		types.T_uuid.ToType(),
	} {
		t.Run("rejected/"+keyType.Oid.String(), func(t *testing.T) {
			_, err := GetFunctionByName(ctx, "json_objectagg", []types.Type{keyType, valueType})
			require.Error(t, err)
		})
	}

	for _, valueType := range []types.Type{
		types.T_binary.ToType(),
		types.T_varbinary.ToType(),
		types.T_blob.ToType(),
	} {
		_, err := GetFunctionByName(ctx, "json_objectagg", []types.Type{types.T_int64.ToType(), valueType})
		require.Error(t, err)
	}

	_, err := GetFunctionByName(ctx, "json_objectagg", []types.Type{types.T_int64.ToType()})
	require.Error(t, err)
}
