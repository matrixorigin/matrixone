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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestGroupByFormatRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name, by string
		expanded bool
	}{
		{"normal", "group by k, r", false},
		{"sets", "group by grouping sets ((k), (r), ())", false},
		{"duplicate sets", "group by grouping sets ((k), (k))", false},
		{"empty set", "group by grouping sets (())", false},
		{"rollup", "group by k, r with rollup", false},
		{"expanded rollup", "group by k, r with rollup", true},
		{"cube", "group by cube (k, r)", false},
		{"expanded cube", "group by cube (k, r)", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), "select k, count(*) from t "+tc.by, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			group := stmt.(*tree.Select).Select.(*tree.SelectClause).GroupBy
			original := append([]tree.Exprs(nil), group.GroupByExprsList...)
			if tc.expanded {
				full := original[0]
				if group.Cube {
					group.GroupByExprsList = []tree.Exprs{nil, full[:1], full, full[1:]}
				} else {
					group.GroupByExprsList = []tree.Exprs{full, full[:1], nil}
				}
			}
			formatted := tree.String(group, dialect.MYSQL)
			require.Equal(t, tc.by, formatted)
			again, err := mysql.ParseOne(context.Background(), "select k, count(*) from t "+formatted, 1)
			require.NoError(t, err)
			t.Cleanup(again.Free)
			parsed := again.(*tree.Select).Select.(*tree.SelectClause).GroupBy
			require.Equal(t, group.Rollup, parsed.Rollup)
			require.Equal(t, group.Cube, parsed.Cube)
			require.Equal(t, original, parsed.GroupByExprsList, "formatting must preserve empty/duplicate sets and the unexpanded key list")
		})
	}
}
