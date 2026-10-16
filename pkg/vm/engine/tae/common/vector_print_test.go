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

package common

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestMoVectorToStringScalarTypes(t *testing.T) {
	// These are diagnostic contracts, independent of the PK search matrix.
	checkFixedVectorPrint(t, types.T_bool, true, "true")
	checkFixedVectorPrint(t, types.T_int8, int8(-7), "-7")
	checkFixedVectorPrint(t, types.T_int16, int16(-7), "-7")
	checkFixedVectorPrint(t, types.T_int32, int32(-7), "-7")
	checkFixedVectorPrint(t, types.T_int64, int64(-7), "-7")
	checkFixedVectorPrint(t, types.T_uint8, uint8(7), "7")
	checkFixedVectorPrint(t, types.T_uint16, uint16(7), "7")
	checkFixedVectorPrint(t, types.T_uint32, uint32(7), "7")
	checkFixedVectorPrint(t, types.T_uint64, uint64(7), "7")
	checkFixedVectorPrint(t, types.T_float32, float32(0.5), "0.5")
	checkFixedVectorPrint(t, types.T_float64, float64(0.5), "0.5")
	checkFixedVectorPrint(t, types.T_date, types.Date(2), "0001-01-03")
	ts, err := types.ParseTimestamp(time.Local, "2020-01-02 03:04:05", 6)
	require.NoError(t, err)
	checkFixedVectorPrint(t, types.T_timestamp, ts, "2020-01-02 03:04:05.000000")
	checkFixedVectorPrint(t, types.T_decimal128, types.Decimal128{B0_63: 7}, "7")
	checkFixedVectorPrint(t, types.T_uuid, types.Uuid{}, "00000000-0000-0000-0000-000000000000")
}

func checkFixedVectorPrint[T types.FixedSizeT](t *testing.T, oid types.T, value T, want string) {
	t.Helper()
	t.Run(oid.String(), func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		vec := vector.NewVec(oid.ToType())
		defer vec.Free(mp)
		require.NoError(t, vector.AppendFixed(vec, value, false, mp))
		require.NoError(t, vector.AppendFixed(vec, value, true, mp))
		require.Equal(t, "[2]: "+want+",null", MoVectorToString(vec, 10))
		require.Equal(t, "[2]: "+want, MoVectorToString(vec, 1))
		require.Equal(t, "[2]: ", MoVectorToString(vec, 0))
	})
}

func TestMoVectorToStringVariableAndEmpty(t *testing.T) {
	require.Equal(t, "empty vector", MoVectorToString(nil, 10))
	for _, oid := range []types.T{types.T_char, types.T_varchar, types.T_binary} {
		t.Run(oid.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			vec := vector.NewVec(oid.ToType())
			defer vec.Free(mp)
			require.Equal(t, "empty vector", MoVectorToString(vec, 10))
			require.NoError(t, vector.AppendBytes(vec, []byte("ab"), false, mp))
			require.Equal(t, "[1]: ab", MoVectorToString(vec, 10))
			require.NoError(t, vector.AppendBytes(vec, []byte{0, 1}, false, mp))
			require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
			require.Equal(t, "[3]: ab,0001,null", MoVectorToString(vec, 10))
			require.Equal(t, "[3]: ab,binary[2],null", MoVectorToString(vec, 10, WithDoNotPrintBin{}))
		})
	}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	unsupported := vector.NewVec(types.T_decimal256.ToType())
	defer unsupported.Free(mp)
	require.NoError(t, vector.AppendFixed(unsupported, types.Decimal256{B0_63: 7}, false, mp))
	require.Equal(t, "unkown type vec... DECIMAL256", MoVectorToString(unsupported, 10))
}
