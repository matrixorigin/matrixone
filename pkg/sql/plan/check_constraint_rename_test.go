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

package plan

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestValidateCheckConstraintNames(t *testing.T) {
	require.NoError(t, ValidateCheckConstraintNames(t.Context(), nil))
	require.NoError(t, ValidateCheckConstraintNames(t.Context(), []*plan.CheckDef{nil, {Name: strings.Repeat("c", 64)}}))
	err := ValidateCheckConstraintNames(t.Context(), []*plan.CheckDef{{Name: "ck"}, {Name: "CK"}})
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateCheckConstraintName))
	for _, length := range []int{58, 59, 64} {
		checks := RenameGeneratedCheckConstraintNames([]*plan.CheckDef{{Name: "t_chk_1"}}, "t", strings.Repeat("u", length))
		err = ValidateCheckConstraintNames(t.Context(), checks)
		if length == 58 {
			require.NoError(t, err)
		} else {
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTooLongIdent))
		}
	}
}

func TestRenameGeneratedCheckConstraintNames(t *testing.T) {
	check := func(name string) *plan.CheckDef {
		return &plan.CheckDef{Name: name, OriginSql: "`a` > 0"}
	}

	t.Run("generated names follow the new table name", func(t *testing.T) {
		in := []*plan.CheckDef{check("t_chk_1"), check("named_chk"), nil, check("t_chk_3")}
		out := RenameGeneratedCheckConstraintNames(in, "t", "u")
		require.Equal(t, "u_chk_1", out[0].Name)
		require.Equal(t, "named_chk", out[1].Name)
		require.Nil(t, out[2])
		require.Equal(t, "u_chk_3", out[3].Name)
		// The input slice and entries stay untouched.
		require.Equal(t, "t_chk_1", in[0].Name)
		require.Equal(t, "t_chk_3", in[3].Name)
	})

	t.Run("matching is case insensitive", func(t *testing.T) {
		in := []*plan.CheckDef{check("T_CHK_1"), check("t_Chk_2")}
		out := RenameGeneratedCheckConstraintNames(in, "t", "U")
		require.Equal(t, "U_chk_1", out[0].Name)
		require.Equal(t, "U_chk_2", out[1].Name)
	})

	t.Run("non generated lookalikes are preserved", func(t *testing.T) {
		in := []*plan.CheckDef{
			check("t_chk_"),        // no ordinal
			check("t_chk_x"),       // not numeric
			check("t_chk_01"),      // non canonical ordinal
			check("t_chk_0"),       // ordinal must be >= 1
			check("other_chk_1"),   // different table prefix
			check("t_chk_1_extra"), // trailing text
		}
		out := RenameGeneratedCheckConstraintNames(in, "t", "u")
		for i, name := range []string{"t_chk_", "t_chk_x", "t_chk_01", "t_chk_0", "other_chk_1", "t_chk_1_extra"} {
			require.Equal(t, name, out[i].Name)
		}
	})

	t.Run("no match returns the input", func(t *testing.T) {
		require.Empty(t, RenameGeneratedCheckConstraintNames(nil, "t", "u"))
		in := []*plan.CheckDef{check("named")}
		out := RenameGeneratedCheckConstraintNames(in, "t", "u")
		require.Equal(t, 1, len(out))
		require.Equal(t, "named", out[0].Name)
		require.Same(t, in[0], out[0])
	})
}
