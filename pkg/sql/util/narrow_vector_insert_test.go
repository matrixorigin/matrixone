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

package util

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// INSERT of a string literal into a vecbf16/vecf16/vecint8/vecuint8 column.
// SetInsertValueString parses the literal per element type; the narrow arms were
// missing, so `insert into t values ('[1,2,3]')` failed on a narrow column while
// the identical statement on vecf32 succeeded.
//
// Each case also pins the DIMENSION check, because that is the arm most easily
// dropped when adding a type: a literal of the wrong length must be rejected
// rather than silently truncated or zero-padded into a corrupt vector.
func TestSetInsertValueStringNarrowVectors(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, tc := range []struct {
		name    string
		typ     types.Type
		literal string
		want    []byte
	}{
		{
			name:    "bf16",
			typ:     types.New(types.T_array_bf16, 3, 0),
			literal: "[1,2,3]",
			want: types.ArrayToBytes[types.BF16]([]types.BF16{
				types.BF16FromFloat32(1), types.BF16FromFloat32(2), types.BF16FromFloat32(3),
			}),
		},
		{
			name:    "f16",
			typ:     types.New(types.T_array_float16, 3, 0),
			literal: "[1,2,3]",
			want: types.ArrayToBytes[types.Float16]([]types.Float16{
				types.Float16FromFloat32(1), types.Float16FromFloat32(2), types.Float16FromFloat32(3),
			}),
		},
		{
			name:    "int8 keeps the sign",
			typ:     types.New(types.T_array_int8, 3, 0),
			literal: "[-128,0,127]",
			want:    types.ArrayToBytes[int8]([]int8{-128, 0, 127}),
		},
		{
			name:    "uint8 does not wrap at 128",
			typ:     types.New(types.T_array_uint8, 3, 0),
			literal: "[0,128,255]",
			want:    types.ArrayToBytes[uint8]([]uint8{0, 128, 255}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			numVal := tree.NewNumVal(tc.literal, tc.literal, false, tree.P_char)
			canInsert, got, err := SetInsertValueString(proc, numVal, &tc.typ)
			require.NoError(t, err)
			require.True(t, canInsert)
			require.Equal(t, tc.want, got)
		})

		t.Run(tc.name+"/unparseable literal rejected", func(t *testing.T) {
			// The element parse error must PROPAGATE, not be swallowed into a
			// zero vector: silently inserting [0,0,0] for a typo'd literal is
			// far worse than failing the statement.
			numVal := tree.NewNumVal("[a,b,c]", "[a,b,c]", false, tree.P_char)
			_, _, err := SetInsertValueString(proc, numVal, &tc.typ)
			require.Error(t, err)
		})

		t.Run(tc.name+"/dimension mismatch rejected", func(t *testing.T) {
			// Declared width is 3; a 2-element literal must error, not pad.
			numVal := tree.NewNumVal("[1,2]", "[1,2]", false, tree.P_char)
			_, _, err := SetInsertValueString(proc, numVal, &tc.typ)
			require.Error(t, err)
		})
	}
}
