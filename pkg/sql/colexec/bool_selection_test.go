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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestInitBoolSelection(t *testing.T) {
	mp := mpool.MustNewZero()
	boolType := types.T_bool.ToType()
	var sel bitmap.Bitmap
	defer sel.Free()

	flat := vector.NewVec(boolType, mp)
	defer flat.Free(mp)
	require.NoError(t, vector.AppendFixedList(flat, []bool{true, false, true, true, false}, nil, mp))
	flat.GetNulls().Add(3)
	require.NoError(t, InitBoolSelection(&sel, flat, 5, mp))
	require.Equal(t, []uint64{0, 2}, sel.ToArray(), "NULL is not TRUE")
	require.Equal(t, int64(5), sel.Len())
	// Fewer rows than the vector holds.
	require.NoError(t, InitBoolSelection(&sel, flat, 1, mp))
	require.Equal(t, []uint64{0}, sel.ToArray())
	require.Error(t, InitBoolSelection(&sel, flat, 6, mp))

	yes, err := vector.NewConstFixed(boolType, true, 1, mp)
	require.NoError(t, err)
	defer yes.Free(mp)
	require.NoError(t, InitBoolSelection(&sel, yes, 70, mp))
	require.Equal(t, 70, sel.Count(), "a constant TRUE selects every row")

	no, err := vector.NewConstFixed(boolType, false, 1, mp)
	require.NoError(t, err)
	defer no.Free(mp)
	require.NoError(t, InitBoolSelection(&sel, no, 70, mp))
	require.True(t, sel.IsEmpty())

	null := vector.NewConstNull(boolType, 1, mp)
	defer null.Free(mp)
	require.NoError(t, InitBoolSelection(&sel, null, 70, mp))
	require.True(t, sel.IsEmpty())

	ints := vector.NewVec(types.T_int32.ToType(), mp)
	defer ints.Free(mp)
	require.Error(t, InitBoolSelection(&sel, ints, 0, mp))
}
