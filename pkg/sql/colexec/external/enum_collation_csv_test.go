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

package external

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCSVEnumCollationPreservesInputWhitespace(t *testing.T) {
	cases := append(collatedEnumInputs(),
		collatedEnumInput{name: "unicode NBSP invalid", domain: "a", input: "a\u00a0", identity: types.EnumCollationUTF8MB4UnicodeCI, invalid: true},
		collatedEnumInput{name: "unicode zero weight empty", domain: "\u0301", input: "", identity: types.EnumCollationUTF8MB4UnicodeCI, want: 1},
	)
	for _, tc := range cases {
		for _, mode := range []string{"strict", "parallel", "local-nonstrict"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				defer proc.Free()
				typ := plan.Type{Id: int32(types.T_enum), Enumvalues: tc.domain, EnumCollation: tc.identity}
				param := enumSetLoadParam(typ, mode)
				field := csvparser.Field{Val: tc.input, HasStringQuote: true}
				line := []csvparser.Field{field}
				require.Equal(t, !tc.invalid, isLegalLine(param.Extern, param.Cols, line))
				require.Equal(t, field, line[0], "validation must not mutate parser input")
				vectorType := types.T_enum.ToType()
				if mode == "parallel" {
					vectorType = types.T_varchar.ToType()
				}
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(vectorType)
				defer bat.Clean(proc.Mp())
				err := getColData(bat, line, 0, param, proc.Mp(), param.Attrs[0], proc)
				if mode == "parallel" {
					require.NoError(t, err)
					require.Equal(t, tc.input, bat.Vecs[0].GetStringAt(0), "parallel cast must receive the original bytes")
					return
				}
				if tc.invalid {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.False(t, bat.Vecs[0].GetNulls().Contains(0))
				require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[types.Enum](bat.Vecs[0], 0))
			})
		}
	}
}

func TestCSVEnumCollationNullAndInvalidIdentity(t *testing.T) {
	for _, tc := range []struct {
		name        string
		identity    uint32
		field       csvparser.Field
		wantNull    bool
		wantInvalid bool
	}{
		{name: "explicit null", identity: types.EnumCollationUTF8Bin, field: csvparser.Field{Val: "invalid", IsNull: true}, wantNull: true},
		{name: "empty without member", identity: types.EnumCollationUTF8Bin, field: csvparser.Field{Val: ""}, wantNull: true},
		{name: "unknown identity", identity: 999, field: csvparser.Field{Val: ""}, wantInvalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			typ := plan.Type{Id: int32(types.T_enum), Enumvalues: "a", EnumCollation: tc.identity}
			param := enumSetLoadParam(typ, "strict")
			line := []csvparser.Field{tc.field}
			require.Equal(t, !tc.wantInvalid, isLegalLine(param.Extern, param.Cols, line))
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(types.T_enum.ToType())
			defer bat.Clean(proc.Mp())
			err := getColData(bat, line, 0, param, proc.Mp(), param.Attrs[0], proc)
			if tc.wantInvalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantNull, bat.Vecs[0].GetNulls().Contains(0))
		})
	}
}
