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

package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPickEnumLiteralRespectsDomain(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_enum.ToType()
	typ.Charset = uint8(types.EnumCollationUTF8MB4UnicodeCI)
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(mp)
	require.NoError(t, vector.AppendBytes(params, []byte("2"), false, mp))
	require.NoError(t, params.SetStringSource(types.StringSourceCOMStmt))
	proc := testutil.NewProcess(t)
	proc.SetPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{vector.PrepareParamNone})
	for _, tc := range []struct {
		literal string
		want    types.Enum
		bad     bool
	}{
		{"'2'", 1, false}, {"2", 2, false}, {"'x'", 2, false},
		{"0", 0, false}, {"?", 1, false}, {"'missing'", 0, true},
	} {
		t.Run(tc.literal, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "data branch pick s into d keys("+tc.literal+")", 1)
			require.NoError(t, err)
			vec := vector.NewVec(typ)
			defer vec.Free(mp)
			err = appendPickKeyExpr(vec, stmt.(*tree.DataBranchPick).Keys.KeyExprs[0], typ, "２,x", time.UTC, mp, params, proc)
			if tc.bad {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[types.Enum](vec, 0))
		})
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "data branch pick s into d keys(?)", 1)
	require.NoError(t, err)
	proc.SetPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{vector.PrepareParamInteger})
	numeric := vector.NewVec(typ)
	defer numeric.Free(mp)
	require.NoError(t, appendPickKeyExpr(numeric, stmt.(*tree.DataBranchPick).Keys.KeyExprs[0], typ, "２,x", time.UTC, mp, params, proc))
	require.Equal(t, types.Enum(2), vector.GetFixedAtNoTypeCheck[types.Enum](numeric, 0))
}

func TestPickEnumRejectsAmbiguousDisplaySubquery(t *testing.T) {
	for _, tc := range []struct {
		name, definition string
		identity         uint32
		wantError        bool
	}{
		{"empty binary member", "", types.EnumCollationUTF8Bin, true},
		{"zero-weight unicode member", "\u0301,x", types.EnumCollationUTF8MB4UnicodeCI, true},
		{"no empty member", "A,a", types.EnumCollationUTF8Bin, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := types.T_enum.ToType()
			typ.Charset = uint8(tc.identity)
			var tbl tableStuff
			tbl.def.colTypes = []types.Type{typ}
			tbl.def.enumValues = []string{tc.definition}
			tbl.def.pkColIdxes = []int{0}
			display := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: int64(function.CAST_INDEX_TO_VALUE) << 32},
				Args: []*plan.Expr{{}, {Typ: plan.Type{Id: int32(types.T_enum)}}},
			}}}
			query := &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{display}}}}
			err := validatePickEnumSubquery(query, tbl)
			if tc.wantError {
				require.ErrorContains(t, err, "cannot distinguish ordinal 0")
			} else {
				require.NoError(t, err)
			}
			// An explicit cast is an intentional text/ordinal boundary, rather
			// than a bare display projection.
			query.Nodes[0].ProjectList[0] = &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: int64(function.CAST) << 32}, Args: []*plan.Expr{display},
			}}}
			require.NoError(t, validatePickEnumSubquery(query, tbl))
			query.Steps = []int32{0}
			query.Nodes[0].ProjectList[0] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_enum)}}
			err = validatePickEnumSubquery(query, tbl)
			if tc.wantError {
				require.ErrorContains(t, err, "ENUM domain is unavailable")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPickEnumSubqueryRemapsDomainAndPreservesZero(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_enum.ToType()
	typ.Charset = uint8(types.EnumCollationUTF8Bin)
	src := vector.NewVec(typ)
	defer src.Free(mp)
	for _, v := range []types.Enum{0, 1, 2} {
		require.NoError(t, vector.AppendFixed(src, v, false, mp))
	}
	source := &plan.Type{Id: int32(types.T_enum), Enumvalues: "A,a", EnumCollation: types.EnumCollationUTF8Bin}
	unchanged, owned, err := coercePickEnumKeyVector(nil, src, typ, "A,a", source, mp)
	require.NoError(t, err)
	require.False(t, owned)
	require.Same(t, src, unchanged)
	remapped, owned, err := coercePickEnumKeyVector(nil, src, typ, "a,A", source, mp)
	require.NoError(t, err)
	require.True(t, owned)
	defer remapped.Free(mp)
	require.Equal(t, []types.Enum{0, 2, 1}, vector.MustFixedColNoTypeCheck[types.Enum](remapped))
	_, _, err = coercePickEnumKeyVector(nil, src, typ, "a,A", nil, mp)
	require.ErrorContains(t, err, "domain is unavailable")

	text := vector.NewVec(types.T_varchar.ToType())
	defer text.Free(mp)
	require.NoError(t, vector.AppendBytes(text, []byte("a"), false, mp))
	converted, owned, err := coercePickEnumKeyVector(nil, text, typ, "A,a", nil, mp)
	require.NoError(t, err)
	require.True(t, owned)
	defer converted.Free(mp)
	require.Equal(t, types.Enum(2), vector.GetFixedAtNoTypeCheck[types.Enum](converted, 0))

	empty := vector.NewVec(typ)
	defer empty.Free(mp)
	require.NoError(t, vector.AppendFixed(empty, types.Enum(0), false, mp))
	require.NoError(t, vector.AppendFixed(empty, types.Enum(1), false, mp))
	emptyType := &plan.Type{Id: int32(types.T_enum), Enumvalues: "", EnumCollation: types.EnumCollationUTF8Bin}
	_, _, err = coercePickEnumKeyVector(nil, empty, typ, "x,", emptyType, mp)
	require.ErrorContains(t, err, "domain is unavailable")
}
