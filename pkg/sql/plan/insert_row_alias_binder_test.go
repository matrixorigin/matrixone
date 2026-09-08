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

package plan

import (
	"context"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func testInsertAliasTable() *planpb.TableDef {
	return &planpb.TableDef{
		Name: "t",
		Cols: []*planpb.ColDef{
			{Name: "id"},
			{Name: "a"},
			{Name: "b"},
		},
		Name2ColIndex: map[string]int32{"id": 0, "a": 1, "b": 2},
	}
}

func testInsertAliasName(parts ...string) *tree.UnresolvedName {
	cstrs := make([]*tree.CStr, len(parts))
	for i, part := range parts {
		cstrs[i] = tree.NewCStr(part, 1)
	}
	return tree.NewUnresolvedName(cstrs...)
}

func TestInsertRowAliasBindingMapsTargetIdentity(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"x", "y"}},
		[]string{"b", "a"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	require.Equal(t, insertRowAliasColumn{targetIdx: 2, incomingPos: 2}, binding.cols["x"])
	require.Equal(t, insertRowAliasColumn{targetIdx: 1, incomingPos: 1}, binding.cols["y"])

	require.NoError(t, binding.remapIncomingPositions(context.Background(), tableDef, map[string]int32{
		"t.a": 4,
		"t.b": 3,
	}))
	require.Equal(t, 3, binding.cols["x"].incomingPos)
	require.Equal(t, 4, binding.cols["y"].incomingPos)
}

func TestInsertRowAliasBindingRemapsGeneratedColumnByIdentity(t *testing.T) {
	tableDef := testInsertAliasTable()
	tableDef.Cols = append(tableDef.Cols, &planpb.ColDef{
		Name:         "g",
		GeneratedCol: &planpb.GeneratedCol{},
	})
	tableDef.Name2ColIndex["g"] = 3
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"id", "g"}},
		[]string{"id", "g"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	require.NoError(t, binding.remapIncomingPositions(context.Background(), tableDef, map[string]int32{
		"t.id": 5,
		"t.g":  2,
	}))
	require.Equal(t, 5, binding.cols["id"].incomingPos)
	require.Equal(t, 2, binding.cols["g"].incomingPos)
}

func TestInsertRowAliasBinderResolvesIncomingAndTargetRows(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"x", "y"}},
		[]string{"b", "a"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	binder := NewOndupUpdateBinder(context.Background(), nil, nil, 11, 7, tableDef, "db", "t", 1, binding)

	expr, err := binder.BindColRef(testInsertAliasName("n", "x"), 0, true)
	require.NoError(t, err)
	require.Equal(t, int32(7), expr.GetCol().RelPos)
	require.Equal(t, int32(2), expr.GetCol().ColPos)

	expr, err = binder.BindColRef(testInsertAliasName("t", "a"), 0, true)
	require.NoError(t, err)
	require.Equal(t, int32(11), expr.GetCol().RelPos)
	require.Equal(t, int32(1), expr.GetCol().ColPos)

	expr, err = binder.BindColRef(testInsertAliasName("n", "x"), 2, true)
	require.NoError(t, err)
	require.Equal(t, int32(2), expr.GetCorr().Depth)
	require.Equal(t, int32(7), expr.GetCorr().RelPos)
	require.Equal(t, int32(2), expr.GetCorr().ColPos)
}

func TestInsertRowAliasBinderRejectsAmbiguousAndInvalidNames(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n"}, []string{"a", "b"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	binder := NewOndupUpdateBinder(context.Background(), nil, nil, 11, 7, tableDef, "db", "t", 1, binding)
	_, err = binder.BindColRef(testInsertAliasName("a"), 0, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ambiguous")

	for _, alias := range []*tree.AliasClause{
		{Alias: "t"},
		{Alias: "n", Cols: tree.IdentifierList{"x", "x"}},
		{Alias: "n", Cols: tree.IdentifierList{"x"}},
	} {
		_, err = validateInsertRowAlias(context.Background(), alias, []string{"a", "b"}, tableDef, "db", "t", 1)
		require.Error(t, err)
	}
}

func TestInsertRowAliasFallbackCollectsNestedParameters(t *testing.T) {
	stmt, err := parsers.ParseOne(
		context.Background(), dialect.MYSQL,
		"insert into t values (1) as n on duplicate key update a = case when ? then n.a + ? else (select ?)", 1,
	)
	require.NoError(t, err)
	insert := stmt.(*tree.Insert)
	require.Equal(t, []int{0, 1, 2}, collectParamExprOffsets(insert.OnDuplicateUpdate[0].Expr))
}
