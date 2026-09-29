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

package vector

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TestUnionSelectionPreservesOwnedDisjointVarlenaLayout(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
	})

	source := NewVec(types.T_varchar.ToType())
	defer source.Free(mp)
	values := [][]byte{
		bytes.Repeat([]byte{'a'}, 49),
		bytes.Repeat([]byte{'b'}, 53),
		[]byte("inline"),
		bytes.Repeat([]byte{'c'}, 57),
	}
	for row, value := range values {
		require.NoError(t, AppendBytes(source, value, row == 1, mp))
	}

	selected := NewVec(types.T_varchar.ToType())
	defer selected.Free(mp)
	sels := []int32{3, 0, 1, 3, 2}
	require.NoError(t, selected.PreExtend(len(sels), mp))
	require.NoError(t, selected.UnionInt32(source, sels, mp))
	require.True(t, selected.VarlenaAreaIsDisjoint())
	plan, err := selected.PrepareMarshalBinary()
	require.NoError(t, err)
	require.False(t, plan.canonicalVarlen)

	descriptors := MustFixedColNoTypeCheck[types.Varlena](selected)
	firstOffset, _ := descriptors[0].OffsetLen()
	repeatedOffset, _ := descriptors[3].OffsetLen()
	require.NotEqual(t, firstOffset, repeatedOffset)

	wire, err := selected.MarshalBinary()
	require.NoError(t, err)
	decoded := NewVecFromReuse()
	defer decoded.Free(nil)
	require.NoError(t, decoded.UnmarshalBinary(wire))
	require.Equal(t, values[3], decoded.GetBytesAt(0))
	require.Equal(t, values[0], decoded.GetBytesAt(1))
	require.True(t, decoded.IsNull(2))
	require.Equal(t, values[3], decoded.GetBytesAt(3))
	require.Equal(t, values[2], decoded.GetBytesAt(4))
}

func TestUnionSelectionKeepsConstVarlenaAliased(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
	})

	constant, err := NewConstBytes(
		types.T_varchar.ToType(),
		bytes.Repeat([]byte{'x'}, 49),
		2,
		mp,
	)
	require.NoError(t, err)
	defer constant.Free(mp)
	selected := NewVec(types.T_varchar.ToType())
	defer selected.Free(mp)
	require.NoError(t, selected.PreExtend(2, mp))
	require.NoError(t, selected.UnionInt32(constant, []int32{0, 0}, mp))
	require.False(t, selected.VarlenaAreaIsDisjoint())
	plan, err := selected.PrepareMarshalBinary()
	require.NoError(t, err)
	require.True(t, plan.canonicalVarlen)
}
