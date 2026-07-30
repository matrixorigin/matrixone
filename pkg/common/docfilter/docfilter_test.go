// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package docfilter

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestSupportsBitset(t *testing.T) {
	for _, oid := range []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	} {
		require.True(t, SupportsBitset(oid.ToType()), oid.String())
	}
	for _, oid := range []types.T{types.T_varchar, types.T_char, types.T_decimal64, types.T_float64} {
		require.False(t, SupportsBitset(oid.ToType()), oid.String())
	}
}

// buildIntVec builds a fixed integer vector; rows whose value is in nullRows are null.
func buildIntVec[T int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64](
	t *testing.T, mp *mpool.MPool, typ types.Type, vals []T, nullRows map[int]bool,
) *vector.Vector {
	v := vector.NewVec(typ)
	for i, val := range vals {
		require.NoError(t, vector.AppendFixed(v, val, nullRows[i], mp))
	}
	return v
}

func must(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}
