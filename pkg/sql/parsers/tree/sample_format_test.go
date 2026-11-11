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

func TestSampleFormatRoundTrip(t *testing.T) {
	for _, query := range []string{
		"select sample(v,100 percent) from src",
		"select sample(v,0 percent) from src",
		"select sample(v,12.34 percent) from src",
		"select sample(src.v+1,h,2 rows) from src",
		"select sample(v,2 rows,'row') from src",
		"select sample(v,2 rows,'block') from src",
		"select sample(*,100 percent) from src",
		"select sample(*,2 rows,'row') from src",
	} {
		t.Run(query, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), query, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			formatted := tree.String(stmt, dialect.MYSQL)
			again, err := mysql.ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err, "SAMPLE must survive CTAS source serialization: %s", formatted)
			t.Cleanup(again.Free)
			require.Equal(t, formatted, tree.String(again, dialect.MYSQL))
			before := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.SampleExpr)
			after := again.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.SampleExpr)
			require.Equal(t, before, after, "preserve columns/star, precision, row count and sampling unit")
		})
	}
}
