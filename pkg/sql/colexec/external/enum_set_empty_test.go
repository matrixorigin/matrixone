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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func enumSetLoadParam(typ plan.Type, mode string) *ExternalParam {
	param := decimal256ExternalParam(tree.CSV, "", typ)
	param.Extern.ExternType = int32(plan.ExternType_LOAD)
	switch mode {
	case "parallel":
		param.ParallelLoad = true
	case "local-nonstrict":
		param.Extern.Local = true
		param.StrictSqlMode = false
	}
	return param
}

// An empty ENUM member and an empty SET are valid values that must not be
// loaded as NULL (issue #29840); explicit NULL markers still load as NULL.
// SET is stored as uint64 but must not be zero-filled or numeric-prefixed, and
// a declared whitespace ENUM member must not be trimmed into another member.
func TestGetColDataKeepsEmptyEnumAndSetDistinctFromNull(t *testing.T) {
	proc := testutil.NewProcess(t)

	enumTyp := plan.Type{Id: int32(types.T_enum), Enumvalues: " ,red,"}
	setTyp := plan.Type{Id: int32(types.T_uint64), Enumvalues: "0,a"}
	for _, mode := range []string{"strict", "parallel", "local-nonstrict"} {
		for _, tc := range []struct {
			typ  plan.Type
			val  string
			want any // Enum / uint64, or the raw text under parallel load
		}{
			{enumTyp, " ", types.Enum(1)},
			{enumTyp, "", types.Enum(3)},
			{enumTyp, " red ", types.Enum(2)},
			{setTyp, "", uint64(0)},
			{setTyp, " ", uint64(0)},
			{setTyp, "a", uint64(2)},
			{setTyp, "0", uint64(1)},
		} {
			param := enumSetLoadParam(tc.typ, mode)
			vecTyp := types.T(tc.typ.Id).ToType()
			if mode == "parallel" {
				vecTyp = types.T_varchar.ToType()
			}
			for _, input := range []struct {
				field  csvparser.Field
				mapped bool
			}{{field: csvparser.Field{Val: tc.val}}, {field: csvparser.Field{IsNull: true}}, {field: csvparser.Field{Val: tc.val}, mapped: true}} {
				func() {
					field := input.field
					param.Extern.NullMap = nil
					if input.mapped {
						param.Extern.NullMap = map[string][]string{param.Attrs[0].ColName: {tc.val, strings.TrimSpace(tc.val)}}
					}
					bat := batch.NewWithSize(1)
					bat.Vecs[0] = vector.NewVec(vecTyp)
					defer bat.Clean(proc.Mp())
					require.NoError(t, getColData(bat, []csvparser.Field{field}, 0, param, proc.Mp(), param.Attrs[0], proc))
					vec := bat.Vecs[0]
					name := mode + "/" + tc.typ.Enumvalues + "/" + tc.val
					if field.IsNull || input.mapped {
						require.True(t, vec.GetNulls().Contains(0), name)
					} else {
						require.False(t, vec.GetNulls().Contains(0), name)
						switch {
						case mode == "parallel":
							// the text is cast by the type parser later; it must
							// reach it unchanged except for non-member trimming
							got := vec.GetStringAt(0)
							_, isSet := tc.want.(uint64)
							if isSet {
								require.Equal(t, strings.TrimSpace(tc.val), got, name)
							} else if tc.val == " red " {
								require.Equal(t, "red", got, name)
							} else {
								require.Equal(t, tc.val, got, name)
							}
						case tc.typ.Id == int32(types.T_enum):
							require.Equal(t, tc.want, vector.MustFixedColWithTypeCheck[types.Enum](vec)[0], name)
						default:
							require.Equal(t, tc.want, vector.MustFixedColWithTypeCheck[uint64](vec)[0], name)
						}
					}
				}()
			}
		}
	}

	// ENUM without a declared '' member keeps the previous behavior (NULL).
	param := enumSetLoadParam(plan.Type{Id: int32(types.T_enum), Enumvalues: "red,blue"}, "strict")
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_enum.ToType())
	defer bat.Clean(proc.Mp())
	require.NoError(t, getColData(bat, []csvparser.Field{{Val: ""}}, 0, param, proc.Mp(), param.Attrs[0], proc))
	require.True(t, bat.Vecs[0].GetNulls().Contains(0))
}

func TestIsEnumMember(t *testing.T) {
	for _, tc := range []struct {
		def, name string
		want      bool
	}{
		{" ,red,", " ", true}, {" ,red,", "", true}, {" ,red,", "RED", true},
		{"red,blue", "", false}, {"red,blue", "blue", true}, {"red,blue", " red", false},
		{"", "", false}, {",", "", true},
	} {
		require.Equal(t, tc.want, isEnumMember(tc.def, tc.name), "%q in %q", tc.name, tc.def)
	}
	// the empty-to-NULL decision runs per row and must not allocate
	require.Zero(t, testing.AllocsPerRun(100, func() { isEnumMember("red,green,blue", "") }))
}
