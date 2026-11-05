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

package externalwrite

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

var lowPrecTypes = []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4}

// lowPrecVector holds 1.5 and -0.5, exact in every low-precision type, then a NULL.
func lowPrecVector(t *testing.T, oid types.T, mp *mpool.MPool) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(oid.ToType())
	for i, v := range []float32{1.5, -0.5, 0} {
		null := i == 2
		var err error
		switch oid {
		case types.T_bf16:
			err = vector.AppendFixed(vec, types.BF16FromFloat32(v), null, mp)
		case types.T_float16:
			err = vector.AppendFixed(vec, types.Float16FromFloat32(v), null, mp)
		case types.T_float8:
			err = vector.AppendFixed(vec, types.Float8FromFloat32(v), null, mp)
		case types.T_float4:
			err = vector.AppendFixed(vec, types.Float4FromFloat32(v), null, mp)
		}
		require.NoError(t, err)
	}
	return vec
}

func TestEncodeLowPrecisionFloat(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, oid := range lowPrecTypes {
		vec := lowPrecVector(t, oid, mp)
		bat := batch.New([]string{"f"})
		bat.Vecs[0] = vec
		bat.SetRowCount(3)

		w := NewExternalWriter(nil, WriterConfig{Format: FormatCSV, Attrs: []string{"f"}}).(*externalWriter)
		out, err := w.encodeCSV(bat)
		require.NoError(t, err)
		require.Contains(t, string(out), "1.5", oid.String())
		require.Contains(t, string(out), "-0.5", oid.String())

		w2 := NewExternalWriter(nil, WriterConfig{Format: FormatJSONLine, Attrs: []string{"f"}}).(*externalWriter)
		out, err = w2.encodeJSONLine(bat)
		require.NoError(t, err)
		require.Contains(t, string(out), `"f":1.5`, oid.String())
		require.Contains(t, string(out), `"f":-0.5`, oid.String())
		vec.Free(mp)
	}
}
