// Copyright 2021 Matrix Origin
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

package index

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestScanFollowupZoneMap(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
	bools, ints := make([]bool, 33), make([]int64, 33)
	for i := range ints {
		bools[i] = true
		ints[i] = int64(i) - 16
	}
	bools[0], bools[32] = false, false
	ints[0], ints[31], ints[32] = math.MinInt64, math.MaxInt64, math.MinInt64
	bv, iv := vector.NewVec(types.T_bool.ToType()), vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { bv.Free(mp); iv.Free(mp) })
	require.NoError(t, vector.AppendFixedList(bv, bools, nil, mp))
	require.NoError(t, vector.AppendFixedList(iv, ints, nil, mp))
	for _, v := range []*vector.Vector{bv, iv} {
		for _, nullable := range []bool{false, true} {
			if nullable {
				v.SetNull(0)
			}
			zm := NewZM(v.GetType().Oid, 0)
			require.NoError(t, BatchUpdateZM(zm, v))
			var wantLo, wantHi []byte
			if v.GetType().Oid == types.T_bool {
				lo, hi := false, true
				wantLo, wantHi = types.EncodeBool(&lo), types.EncodeBool(&hi)
			} else {
				lo, hi := int64(math.MinInt64), int64(math.MaxInt64)
				wantLo, wantHi = types.EncodeInt64(&lo), types.EncodeInt64(&hi)
			}
			require.True(t, zm.IsInited())
			require.Equal(t, wantLo, zm.GetMinBuf())
			require.Equal(t, wantHi, zm.GetMaxBuf())
			wire, err := zm.Marshal()
			require.NoError(t, err)
			restored := DecodeZM(wire)
			require.Equal(t, zm, restored)
		}
	}
}
