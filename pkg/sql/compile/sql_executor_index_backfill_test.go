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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestMarkIndexBackfillSource(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"select", "select serial(e) from t", false},
		{"insert select", "insert into idx select serial(e) from t", false},
		{"union", "select e from t union all select e from t", true},
		{"update", "update t set e='a'", true},
		{"multiple statements", "select e from t; select e from t", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stmts, err := parsers.Parse(ctx, dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer func() {
				for _, stmt := range stmts {
					stmt.Free()
				}
			}()
			err = markIndexBackfillSource(ctx, stmts, 42)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			var selectStmt *tree.Select
			switch stmt := stmts[0].(type) {
			case *tree.Select:
				selectStmt = stmt
			case *tree.Insert:
				selectStmt = stmt.Rows
			}
			require.NotNil(t, selectStmt)
			clause, ok := selectStmt.Select.(*tree.SelectClause)
			require.True(t, ok)
			require.EqualValues(t, 42, clause.InternalIndexBackfillSourceID)
		})
	}
}
