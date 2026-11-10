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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// SHOW CREATE VIEW must return the stored view text unchanged (#31070).
func TestShowCreateViewSQLKeepsStoredText(t *testing.T) {
	for _, tc := range []struct{ name, stmt string }{
		{"v1", `create view v1 as select 'C:\\new' as p`},
		{"v2", `create view v2 as select 'it''s' as c, "dq\"x" as d, 'a\\b\\\\c' as e`},
		{`v"3'`, "create view `v\"3'` as select '\n' as nl"},
	} {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, showCreateViewSQL(tc.name, tc.stmt), 1)
		require.NoError(t, err, tc.stmt)
		exprs := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs
		require.Equal(t, tc.name, exprs[0].Expr.(*tree.NumVal).String())
		require.Equal(t, tc.stmt, exprs[1].Expr.(*tree.NumVal).String())
		stmt.Free()
	}
}
