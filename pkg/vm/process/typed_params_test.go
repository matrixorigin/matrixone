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

package process

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestTypedPrepareParamsCodec(t *testing.T) {
	mp := mpool.MustNewZero()

	t.Run("absent", func(t *testing.T) {
		encoded, err := encodeTypedPrepareParams(nil, 2)
		require.NoError(t, err)
		require.Nil(t, encoded)
		decoded, err := decodeTypedPrepareParams(nil, 2, mp)
		require.NoError(t, err)
		require.Nil(t, decoded)
	})

	t.Run("round trip keeps types and TEXT-only gaps", func(t *testing.T) {
		typed := batch.NewWithSize(3)
		var err error
		typed.Vecs[0], err = vector.NewConstFixed(types.T_int64.ToType(), int64(-9), 1, mp)
		require.NoError(t, err)
		dec, err := types.ParseDecimal128("12.50", 4, 2)
		require.NoError(t, err)
		typed.Vecs[2], err = vector.NewConstFixed(types.New(types.T_decimal128, 4, 2), dec, 1, mp)
		require.NoError(t, err)
		typed.SetRowCount(1)
		defer typed.Clean(mp)

		encoded, err := encodeTypedPrepareParams(typed, 3)
		require.NoError(t, err)
		require.Len(t, encoded, 3)
		require.Empty(t, encoded[1])

		decoded, err := decodeTypedPrepareParams(encoded, 3, mp)
		require.NoError(t, err)
		defer decoded.Clean(mp)
		require.Nil(t, decoded.Vecs[1])
		require.Equal(t, int64(-9), vector.GetFixedAtNoTypeCheck[int64](decoded.Vecs[0], 0))
		require.Equal(t, types.New(types.T_decimal128, 4, 2).Oid, decoded.Vecs[2].GetType().Oid)
		require.Equal(t, int32(2), decoded.Vecs[2].GetType().Scale)
		require.Equal(t, dec, vector.GetFixedAtNoTypeCheck[types.Decimal128](decoded.Vecs[2], 0))
	})

	t.Run("count mismatch is rejected", func(t *testing.T) {
		typed := batch.NewWithSize(1)
		defer typed.Clean(mp)
		_, err := encodeTypedPrepareParams(typed, 2)
		require.Error(t, err)
		_, err = decodeTypedPrepareParams([][]byte{nil}, 2, mp)
		require.Error(t, err)
	})

	t.Run("multi-row entry is rejected", func(t *testing.T) {
		vec := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(vec, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(vec, int64(2), false, mp))
		data, err := vec.MarshalBinary()
		require.NoError(t, err)
		vec.Free(mp)
		_, err = decodeTypedPrepareParams([][]byte{data}, 1, mp)
		require.Error(t, err)
		require.Zero(t, mp.CurrNB())
	})
}

func TestTypedPrepareParamsLifecycle(t *testing.T) {
	proc := NewTopProcess(t.Context(), mpool.MustNewZero(), nil, nil, nil, nil, nil, nil, nil, nil, nil)
	newTyped := func(v int64) *batch.Batch {
		bat := batch.NewWithSize(1)
		var err error
		bat.Vecs[0], err = vector.NewConstFixed(types.T_int64.ToType(), v, 1, proc.Mp())
		require.NoError(t, err)
		bat.SetRowCount(1)
		return bat
	}
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("1"), false, proc.Mp()))

	proc.SetPrepareParams(params)
	proc.SetTypedPrepareParams(newTyped(1))
	require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](proc.GetTypedPrepareParam(0), 0))
	require.Nil(t, proc.GetTypedPrepareParam(1))
	require.Nil(t, proc.GetTypedPrepareParam(-1))

	// Detach/restore moves typed values together with the TEXT transport.
	state := proc.DetachPrepareParams()
	require.Nil(t, proc.GetTypedPrepareParams())
	proc.BorrowPrepareParams(state)
	require.NotNil(t, proc.GetTypedPrepareParam(0))
	proc.RestorePrepareParams(state)
	require.NotNil(t, proc.GetTypedPrepareParam(0))

	// A new TEXT transport is a new execution: the old typed values go.
	proc.SetPrepareParams(params)
	require.Nil(t, proc.GetTypedPrepareParams())

	proc.SetTypedPrepareParams(newTyped(2))
	proc.SetPrepareParams(nil)
	require.Nil(t, proc.GetTypedPrepareParams())
	params.Free(proc.Mp())
	require.Zero(t, proc.Mp().CurrNB())
}
