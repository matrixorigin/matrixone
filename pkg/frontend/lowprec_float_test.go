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

package frontend

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

// TestLowPrecisionFloatExportAndBranch covers JSON export, data branch value comparison
// and data branch SQL literals for bf16, float16, float8 and float4 columns.
func TestLowPrecisionFloatExportAndBranch(t *testing.T) {
	mp := mpool.MustNewZero()
	ses := &Session{}
	for _, oid := range lowPrecTypes {
		vec := lowPrecVector(t, oid, mp)
		v, err := vectorValueToJSON(vec, 0, nil, nil)
		require.NoError(t, err)
		require.Equal(t, float32(1.5), v, oid.String())

		c, err := compareSingleValInVector(context.Background(), ses, 0, 1, vec, vec)
		require.NoError(t, err)
		require.Equal(t, 1, c, oid.String())
		c, err = compareSingleValInVector(context.Background(), ses, 1, 1, vec, vec)
		require.NoError(t, err)
		require.Zero(t, c, oid.String())

		var buf bytes.Buffer
		require.NoError(t, formatValIntoString(ses, float32(-0.5), *vec.GetType(), &buf))
		require.Equal(t, "-0.5", buf.String(), oid.String())
		vec.Free(mp)
	}
}
