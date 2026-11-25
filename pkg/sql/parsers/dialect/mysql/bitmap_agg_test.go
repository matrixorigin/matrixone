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

package mysql

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBitmapAggregate(t *testing.T) {
	const frame = "over (partition by p order by k rows between 1 preceding and current row)"
	for _, name := range []string{"Bitmap_Construct_Agg", "Bitmap_Or_Agg"} {
		for _, tc := range []struct {
			modifier string
			typ      tree.FuncType
			window   string
		}{
			{"", tree.FUNC_TYPE_DEFAULT, ""},
			{"all ", tree.FUNC_TYPE_ALL, ""},
			{"distinct ", tree.FUNC_TYPE_DISTINCT, ""},
			{"", tree.FUNC_TYPE_DEFAULT, "over ()"},
			{"", tree.FUNC_TYPE_DEFAULT, frame},
		} {
			sql := "select " + name + "(" + tc.modifier + "v) " + tc.window + " from t"
			t.Run(sql, func(t *testing.T) {
				stmt, err := ParseOne(t.Context(), sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				fn := firstSelectExpr(t, stmt).(*tree.FuncExpr)
				require.Equal(t, strings.ToLower(name), fn.FuncName.Compare())
				require.Equal(t, tc.typ, fn.Type)
				require.Len(t, fn.Exprs, 1)
				require.Equal(t, "v", tree.String(fn.Exprs[0], dialect.MYSQL))
				if tc.window == "" {
					require.Nil(t, fn.WindowSpec)
				} else {
					require.NotNil(t, fn.WindowSpec)
					require.Equal(t, tc.window, tree.String(fn.WindowSpec, dialect.MYSQL))
				}
			})
		}
		t.Run(name+" identifiers", func(t *testing.T) {
			stmt, err := ParseOne(t.Context(), "select "+name+" as "+name+" from "+name, 1)
			require.NoError(t, err)
			defer stmt.Free()
		})
		for _, args := range []string{"()", "(a,b)"} {
			_, err := ParseOne(t.Context(), "select "+name+args, 1)
			require.ErrorContains(t, err, "syntax error")
		}
	}
	_, err := ParseOne(t.Context(), "select unrelated_fn(v) over () from t", 1)
	require.ErrorContains(t, err, "syntax error")
}
