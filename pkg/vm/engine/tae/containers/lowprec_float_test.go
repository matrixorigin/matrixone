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
		if name != "id" {
			require.True(t, w.Vecs[i].IsNull(0), name)
		}
	}
}
