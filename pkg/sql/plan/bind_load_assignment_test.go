// Copyright 2021 Matrix Origin
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestLoadAssignmentIgnorePolicy(t *testing.T) {
	require.False(t, loadAssignmentIgnore(nil))
	require.False(t, loadAssignmentIgnore(&tree.Load{DuplicateHandling: &tree.DuplicateKeyError{}}))
	require.True(t, loadAssignmentIgnore(&tree.Load{DuplicateHandling: &tree.DuplicateKeyIgnore{}}))
	require.True(t, loadAssignmentIgnore(&tree.Load{Local: true, DuplicateHandling: &tree.DuplicateKeyError{}}))
	require.False(t, loadAssignmentIgnore(&tree.Load{Local: true, DuplicateHandling: &tree.DuplicateKeyReplace{}}))
	require.False(t, loadAssignmentIgnore(&tree.Load{
		DuplicateHandling: &tree.DuplicateKeyError{},
		Param: &tree.ExternParam{ExParamConst: tree.ExParamConst{
			Tail: &tree.TailParameter{IgnoredLines: 1},
		}},
	}))
}

func TestApplyLoadAssignmentCasts(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	tinyText := planpb.Type{Id: int32(types.T_text), Width: types.MaxTinyTextLen}
	tinyBlob := planpb.Type{Id: int32(types.T_blob), Width: types.MaxTinyTextLen}
	intType := planpb.Type{Id: int32(types.T_int32)}
	tableDef := &planpb.TableDef{Cols: []*planpb.ColDef{
		{Name: "txt", Typ: tinyText},
		{Name: "blob", Typ: tinyBlob},
		{Name: "n", Typ: intType},
	}}
	makeCol := func(typ planpb.Type, pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos}}}
	}
	exprs := map[string]*planpb.Expr{
		"txt":  makeCol(tinyText, 2),
		"blob": makeCol(tinyBlob, 0),
		"n":    makeCol(intType, 1),
	}

	require.NoError(t, builder.applyLoadAssignmentCasts(tableDef, exprs, false))
	require.Equal(t, "cast_assign", exprs["txt"].GetF().GetFunc().GetObjName())
	require.Equal(t, int32(2), exprs["txt"].GetF().GetArgs()[0].GetCol().GetColPos())
	require.Equal(t, "cast_assign", exprs["blob"].GetF().GetFunc().GetObjName())
	require.Equal(t, int32(0), exprs["blob"].GetF().GetArgs()[0].GetCol().GetColPos())
	require.Nil(t, exprs["n"].GetF())

	ignored := map[string]*planpb.Expr{"txt": makeCol(tinyText, 4)}
	require.NoError(t, builder.applyLoadAssignmentCasts(tableDef, ignored, true))
	require.Equal(t, "cast_ignore", ignored["txt"].GetF().GetFunc().GetObjName())
}
