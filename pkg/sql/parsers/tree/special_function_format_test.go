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

package tree_test

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSpecialFunctionFormatRoundTripForCTAS(t *testing.T) {
	for _, tc := range []struct {
		name         string
		sql          string
		wantCTAS     string
		wantOrdinary string
	}{
		{"trim default", "select trim(b) as t from s", "trim(`b`)", "trim(`b`)"},
		{"trim removal", "select trim(a from b) as t from s", "trim(`a` from `b`)", "trim(`a` from `b`)"},
		{"trim both", "select trim(both a from b) as t from s", "trim(both `a` from `b`)", "trim(both `a` from `b`)"},
		{"trim leading", "select trim(leading a from b) as t from s", "trim(leading `a` from `b`)", "trim(leading `a` from `b`)"},
		{"trim trailing", "select trim(trailing a from b) as t from s", "trim(trailing `a` from `b`)", "trim(trailing `a` from `b`)"},
		{"position column", "select position(a in b) as p from s", "position(`a` in `b`)", "position(`a` in `b`)"},
		{"position literal", "select position('ab' in b) as p from s", "position(\"ab\" in `b`)", "position('ab' in `b`)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			t.Cleanup(original.Free)
			for _, format := range []struct {
				name string
				opts []tree.FmtCtxOption
				want string
			}{
				{"ctas", []tree.FmtCtxOption{tree.WithQuoteString(true), tree.WithQuoteIdentifier()}, tc.wantCTAS},
				{"ordinary", []tree.FmtCtxOption{tree.WithSingleQuoteString(), tree.WithQuoteIdentifier()}, tc.wantOrdinary},
			} {
				t.Run(format.name, func(t *testing.T) {
					formatted := tree.NewFmtCtx(dialect.MYSQL, format.opts...)
					original.Format(formatted)
					require.Contains(t, formatted.String(), format.want)
					reparsed, err := parsers.ParseOne(context.Background(), dialect.MYSQL, formatted.String(), 1)
					require.NoError(t, err, formatted.String())
					t.Cleanup(reparsed.Free)
					originalFn := original.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.FuncExpr)
					reparsedFn := reparsed.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.FuncExpr)
					require.True(t, strings.EqualFold(originalFn.FuncName.Origin(), reparsedFn.FuncName.Origin()))
					require.Len(t, reparsedFn.Exprs, len(originalFn.Exprs))
					for i := range originalFn.Exprs {
						require.Equal(t, tree.String(originalFn.Exprs[i], dialect.MYSQL), tree.String(reparsedFn.Exprs[i], dialect.MYSQL))
					}
				})
			}
		})
	}
}
