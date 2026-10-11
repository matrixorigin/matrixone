// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree_test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestCaseExprsUsesStableLexicalOwners(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select case when ? then case when ? then null else ? end else ? end v order by case when ? then ? else ? end", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	first, second := tree.CaseExprs(stmt), tree.CaseExprs(stmt)
	require.Len(t, first, 3)
	require.Len(t, second, len(first))
	for index := range first {
		require.Same(t, first[index], second[index])
	}
	require.Same(t, first[1], first[0].Whens[0].Val)
	require.Equal(t, 7, tree.ParameterCount(stmt))
}

func TestFunctionExprsUsesStableLexicalOwners(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "with s as (select field(?,?) v) select field(coalesce(v,?),?),field((select max(v) from s),?) from s order by field(v,?)", 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	first := tree.FunctionExprs(stmt)
	names := make([]string, 0, len(first))
	for _, expr := range first {
		names = append(names, expr.Func.FunctionReference.(*tree.UnresolvedName).ColName())
	}
	require.Equal(t, []string{"field", "field", "coalesce", "field", "max", "field"}, names)
	second := tree.FunctionExprs(stmt)
	require.Len(t, second, len(first))
	for index := range first {
		require.Same(t, first[index], second[index])
	}
	require.Equal(t, 6, tree.ParameterCount(stmt), "consumer traversal neither rewrites nor compacts marker ordinals")
}
