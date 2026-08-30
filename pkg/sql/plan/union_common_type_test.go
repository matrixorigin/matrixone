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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestUnionDecimalLiteralCommonType(t *testing.T) {
	for _, test := range []struct {
		name     string
		sql      string
		oid      types.T
		width    int32
		scale    int32
		nullable bool
	}{
		{
			name:  "small positive integer literal",
			sql:   "select 1 as x union all select 2.5 as x",
			oid:   types.T_decimal64,
			width: 2,
			scale: 1,
		},
		{
			name:  "negative integer literal",
			sql:   "select -123 as x union select 2.50 as x",
			oid:   types.T_decimal64,
			width: 5,
			scale: 2,
		},
		{
			name:  "bigint boundary literal",
			sql:   "select 9223372036854775807 as x union all select 0.1 as x",
			oid:   types.T_decimal128,
			width: 20,
			scale: 1,
		},
		{
			name:  "explicit signed domain is not narrowed",
			sql:   "select cast(1 as signed) as x union all select 2.5 as x",
			oid:   types.T_decimal128,
			width: 20,
			scale: 1,
		},
		{
			name:     "pure null remains neutral and nullable",
			sql:      "select 1 as x union all select null union all select 2.5",
			oid:      types.T_decimal64,
			width:    2,
			scale:    1,
			nullable: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			typ := buildFirstQueryResultType(t, test.sql)
			require.Equal(t, int32(test.oid), typ.Id)
			require.Equal(t, test.width, typ.Width)
			require.Equal(t, test.scale, typ.Scale)
			require.Equal(t, !test.nullable, typ.NotNullable)
		})
	}
}

func TestCTASUnionDecimalLiteralMetadata(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"create table t_ctas_union_decimal as select 1 as x union all select 2.5 as x", 1)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.NoError(t, err)

	var visible []*planpb.ColDef
	for _, col := range logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 1)
	require.Equal(t, "x", visible[0].Name)
	require.Equal(t, int32(types.T_decimal64), visible[0].Typ.Id)
	require.Equal(t, int32(2), visible[0].Typ.Width)
	require.Equal(t, int32(1), visible[0].Typ.Scale)
	require.True(t, visible[0].Typ.NotNullable)
	require.NotNil(t, visible[0].Default)
	require.False(t, visible[0].Default.NullAbility)
}

func buildFirstQueryResultType(t *testing.T, sql string) planpb.Type {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotEmpty(t, query.Steps)
	root := query.Nodes[query.Steps[len(query.Steps)-1]]
	require.NotEmpty(t, root.ProjectList)
	return root.ProjectList[0].Typ
}
