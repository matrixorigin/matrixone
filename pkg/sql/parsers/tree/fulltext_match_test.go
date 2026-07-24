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

package tree

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

func TestFullTextMatchFuncExpressionFormat(t *testing.T) {
	cols := []*KeyPart{{ColName: NewUnresolvedColName("body")}}

	// classic MATCH deparses as MATCH and carries its mode. The pattern is an Expr
	// (a string literal here; #24796 made it an expression so prepared '?' works).
	ft, err := NewFullTextMatchFuncExpression(cols, NewNumVal("apple", "apple", false, P_char), FULLTEXT_BOOLEAN)
	require.NoError(t, err)

	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	ft.Format(ctx)
	require.Equal(t, `MATCH (body) AGAINST ("apple" IN BOOLEAN MODE)`, ctx.String())

	// default mode omits the mode clause.
	def, _ := NewFullTextMatchFuncExpression(cols, NewNumVal("apple", "apple", false, P_char), FULLTEXT_DEFAULT)
	ctx2 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	def.Format(ctx2)
	require.Equal(t, `MATCH (body) AGAINST ("apple")`, ctx2.String())
}

func TestFullTextMatchExprValid(t *testing.T) {
	cols := []*KeyPart{{ColName: NewUnresolvedColName("body")}}

	// empty column list is rejected
	_, err := NewFullTextMatchFuncExpression(nil, NewNumVal("apple", "apple", false, P_char), FULLTEXT_DEFAULT)
	require.Error(t, err)

	// empty pattern is rejected
	_, err = NewFullTextMatchFuncExpression(cols, NewNumVal("", "", false, P_char), FULLTEXT_DEFAULT)
	require.Error(t, err)
}
