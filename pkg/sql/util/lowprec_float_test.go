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

package util

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestSetBytesToAnyVectorLowPrecisionFloat checks that a bf16, float16, float8 or float4
// value given as text (a user variable or parameter) is parsed with the range and
// finiteness checks of a cast and stores -0 as +0.
func TestSetBytesToAnyVectorLowPrecisionFloat(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	ctx := context.Background()
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		vec := vector.NewVec(oid.ToType())
		require.NoError(t, vec.PreExtend(1, mp))
		vec.SetLength(1)
		require.NoError(t, SetBytesToAnyVector(ctx, "1.5", 0, false, vec, proc))
		f, _ := vector.GetLowPrecisionFloatAt(vec, 0)
		require.Equal(t, float32(1.5), f, oid.String())
		require.NoError(t, SetBytesToAnyVector(ctx, "-0", 0, false, vec, proc))
		require.Equal(t, []byte{0}, vec.GetRawBytesAt(0)[:1], oid.String())
		require.Error(t, SetBytesToAnyVector(ctx, "abc", 0, false, vec, proc))
		require.Error(t, SetBytesToAnyVector(ctx, "1e40", 0, false, vec, proc))
		require.Error(t, SetBytesToAnyVector(ctx, "nan", 0, false, vec, proc))
		vec.Free(mp)
	}
}
