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

func TestBm25MatchFuncExpression(t *testing.T) {
	cols := []*KeyPart{{ColName: NewUnresolvedColName("body")}}

	// BM25(): IsBm25 set, mode fixed to DEFAULT, deparses as BM25 with no mode.
	bm, err := NewBm25MatchFuncExpression(cols, "apple banana")
	require.NoError(t, err)
	require.True(t, bm.IsBm25)
	require.Equal(t, FULLTEXT_DEFAULT, bm.Mode)

	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	bm.Format(ctx)
	require.Equal(t, "BM25 (body) AGAINST (apple banana)", ctx.String())
}

func TestFullTextMatchFuncExpressionFormat(t *testing.T) {
	cols := []*KeyPart{{ColName: NewUnresolvedColName("body")}}

	// classic MATCH: IsBm25 false, deparses as MATCH and carries its mode.
	ft, err := NewFullTextMatchFuncExpression(cols, "apple", FULLTEXT_BOOLEAN)
	require.NoError(t, err)
	require.False(t, ft.IsBm25)

	ctx := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	ft.Format(ctx)
	require.Equal(t, "MATCH (body) AGAINST (apple IN BOOLEAN MODE)", ctx.String())

	// default mode omits the mode clause.
	def, _ := NewFullTextMatchFuncExpression(cols, "apple", FULLTEXT_DEFAULT)
	ctx2 := NewFmtCtx(dialect.MYSQL, WithQuoteString(true))
	def.Format(ctx2)
	require.Equal(t, "MATCH (body) AGAINST (apple)", ctx2.String())
}

func TestFullTextMatchExprValid(t *testing.T) {
	cols := []*KeyPart{{ColName: NewUnresolvedColName("body")}}

	// empty column list is rejected
	_, err := NewBm25MatchFuncExpression(nil, "apple")
	require.Error(t, err)

	// empty pattern is rejected
	_, err = NewBm25MatchFuncExpression(cols, "")
	require.Error(t, err)

	// same for the classic constructor
	_, err = NewFullTextMatchFuncExpression(nil, "apple", FULLTEXT_DEFAULT)
	require.Error(t, err)
}
