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

// An empty ENUM member and an empty SET are valid values that must not be
// loaded as NULL (issue #29840); explicit NULL markers still load as NULL.
func TestGetColDataKeepsEmptyEnumAndSetDistinctFromNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	for _, tc := range []struct {
		name   string
		typ    plan.Type
		vecTyp types.Type
		want   func(*vector.Vector) any
		empty  any
	}{
		{
			name:   "enum",
			typ:    plan.Type{Id: int32(types.T_enum), Enumvalues: "red,中文,"},
			vecTyp: types.T_enum.ToType(),
			want:   func(v *vector.Vector) any { return vector.MustFixedColWithTypeCheck[types.Enum](v)[0] },
			empty:  types.Enum(3),
		},
		{
			name:   "set",
			typ:    plan.Type{Id: int32(types.T_uint64), Enumvalues: "a,中文"},
			vecTyp: types.T_uint64.ToType(),
			want:   func(v *vector.Vector) any { return vector.MustFixedColWithTypeCheck[uint64](v)[0] },
			empty:  uint64(0),
		},
	} {
		for _, format := range []string{tree.CSV, tree.JSONLINE} {
			t.Run(tc.name+"/"+format, func(t *testing.T) {
				param := decimal256ExternalParam(format, tree.OBJECT, tc.typ)
				for _, field := range []csvparser.Field{{Val: ""}, {Val: " "}} {
					bat := batch.NewWithSize(1)
					bat.Vecs[0] = vector.NewVec(tc.vecTyp)
					require.NoError(t, getColData(bat, []csvparser.Field{field}, 0, param, proc.Mp(), param.Attrs[0], proc))
					require.False(t, bat.Vecs[0].GetNulls().Contains(0), "empty value %q loaded as NULL", field.Val)
					require.Equal(t, tc.empty, tc.want(bat.Vecs[0]))
					bat.Clean(proc.Mp())
				}

				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(tc.vecTyp)
				require.NoError(t, getColData(bat, []csvparser.Field{{IsNull: true}}, 0, param, proc.Mp(), param.Attrs[0], proc))
				require.True(t, bat.Vecs[0].GetNulls().Contains(0))
				bat.Clean(proc.Mp())
			})
		}
	}

	// ENUM without a declared '' member keeps the previous behavior (NULL).
	param := decimal256ExternalParam(tree.CSV, "", plan.Type{Id: int32(types.T_enum), Enumvalues: "red,blue"})
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_enum.ToType())
	defer bat.Clean(proc.Mp())
	require.NoError(t, getColData(bat, []csvparser.Field{{Val: ""}}, 0, param, proc.Mp(), param.Attrs[0], proc))
	require.True(t, bat.Vecs[0].GetNulls().Contains(0))
}
