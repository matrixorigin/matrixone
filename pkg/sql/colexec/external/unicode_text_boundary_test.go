// Copyright 2026 Matrix Origin
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

package external

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// File input becomes a typed value here, so a native Unicode column admits
// only valid text in its charset, for both CSV/JSONLINE text fields and
// Parquet strings. Legacy text and binary columns stay unrestricted.
func TestFileInputValidatesUnicodeText(t *testing.T) {
	proc := testutil.NewProcess(t)
	mb4 := types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8MB4UnicodeCI)
	mb3 := types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8MB3UnicodeCI)
	mb4Text := types.NewWithCharset(types.T_text, 0, 0, types.CharsetUTF8MB4UnicodeCI)
	mb3Text := types.NewWithCharset(types.T_text, 0, 0, types.CharsetUTF8MB3UnicodeCI)
	binary := types.NewWithCharset(types.T_varbinary, 64, 0, types.CharsetUTF8MB4UnicodeCI)
	appenders := map[string]func(*vector.Vector, []byte) error{
		"text field": func(vec *vector.Vector, value []byte) error {
			return appendFieldText(vec, string(value), proc.Mp())
		},
		"parquet": func(vec *vector.Vector, value []byte) error {
			return appendParquetVarlenValue(proc.Ctx, vec, value, false, proc.Mp(), *vec.GetType())
		},
	}
	for name, appendValue := range appenders {
		for _, tc := range []struct {
			name  string
			typ   types.Type
			value []byte
			bad   bool
		}{
			{"mb4_valid", mb4, []byte("😀"), false},
			{"mb3_bmp", mb3, []byte("中文"), false},
			{"mb3_non_bmp", mb3, []byte("😀"), true},
			{"mb3_text_non_bmp", mb3Text, []byte("😀"), true},
			{"mb4_text_invalid_long", mb4Text, bytes.Repeat([]byte{0xff}, 49), true},
			{"legacy_invalid", types.T_text.ToType(), []byte{0xff}, false},
			{"binary_with_unicode_metadata", binary, []byte{0xff}, false},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				vec := vector.NewVec(tc.typ, proc.Mp())
				t.Cleanup(func() { vec.Free(proc.Mp()) })
				err := appendValue(vec, tc.value)
				if tc.bad {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "got %v", err)
					require.Zero(t, vec.Length())
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.value, vec.GetBytesAt(0))
			})
		}
	}
}
