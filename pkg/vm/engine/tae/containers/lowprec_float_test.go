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

package containers

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestLowPrecisionFloatBatchWALRoundTrip writes a batch with bf16, float16, float8 and
// float4 columns as an append command does and reads it back as WAL replay does.
func TestLowPrecisionFloatBatchWALRoundTrip(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := NewBatch()
	defer bat.Close()
	id := MakeVector(types.T_int32.ToType(), mp)
	id.Append(int32(1), false)
	id.Append(int32(2), false)
	bat.AddVector("id", id)
	cols := map[string]types.T{"a": types.T_bf16, "b": types.T_float16, "c": types.T_float8, "d": types.T_float4}
	for _, name := range []string{"a", "b", "c", "d"} {
		vec := MakeVector(cols[name].ToType(), mp)
		switch cols[name] {
		case types.T_bf16:
			vec.Append(types.BF16FromFloat32(1.5), false)
		case types.T_float16:
			vec.Append(types.Float16FromFloat32(1.5), false)
		case types.T_float8:
			vec.Append(types.Float8FromFloat32(1.5), false)
		case types.T_float4:
			vec.Append(types.Float4FromFloat32(1.5), false)
		}
		vec.Append(nil, true)
		bat.AddVector(name, vec)
	}
	var buf bytes.Buffer
	_, err := bat.WriteTo(&buf)
	require.NoError(t, err)
	got := NewBatch()
	defer got.Close()
	_, err = got.ReadFrom(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, bat.Attrs, got.Attrs)
	require.Len(t, got.Vecs, len(bat.Vecs))
	w := got.CloneWindow(1, 1)
	defer w.Close()
	for i, name := range got.Attrs {
		require.Equal(t, bat.Vecs[i].GetType().Oid, got.Vecs[i].GetType().Oid, name)
		require.Equal(t, 2, got.Vecs[i].Length(), name)
		require.Equal(t, bat.Vecs[i].Get(0), got.Vecs[i].Get(0), name)
		if name != "id" {
			require.True(t, w.Vecs[i].IsNull(0), name)
		}
	}
}

// TestLowPrecisionFloatVectorGetUpdate reads and updates non-null bf16, float16, float8 and
// float4 values through the vector wrapper.
func TestLowPrecisionFloatVectorGetUpdate(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, tc := range []struct {
		oid      types.T
		one, two any
	}{
		{types.T_bf16, types.BF16FromFloat32(1.5), types.BF16FromFloat32(2)},
		{types.T_float16, types.Float16FromFloat32(1.5), types.Float16FromFloat32(2)},
		{types.T_float8, types.Float8FromFloat32(1.5), types.Float8FromFloat32(2)},
		{types.T_float4, types.Float4FromFloat32(1.5), types.Float4FromFloat32(2)},
	} {
		vec := MakeVector(tc.oid.ToType(), mp)
		vec.Append(tc.one, false)
		vec.Append(nil, true)
		require.Equal(t, tc.one, vec.Get(0), tc.oid.String())
		v, isNull := vec.ShallowGet(0), vec.IsNull(0)
		require.Equal(t, tc.one, v, tc.oid.String())
		require.False(t, isNull, tc.oid.String())
		vec.Update(0, tc.two, false)
		require.Equal(t, tc.two, vec.Get(0), tc.oid.String())
		vec.Update(1, tc.one, false)
		require.False(t, vec.IsNull(1), tc.oid.String())
		require.Equal(t, tc.one, vec.Get(1), tc.oid.String())
		vec.Update(0, nil, true)
		require.True(t, vec.IsNull(0), tc.oid.String())
		vec.Close()
	}
	require.Zero(t, mp.CurrNB())
}
