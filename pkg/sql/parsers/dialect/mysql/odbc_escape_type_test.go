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

package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// ODBC escapes {d '...'}, {t '...'}, {ts '...'} must cast to DATE, TIME and
// TIMESTAMP respectively; {d} used to be cast to DATETIME.
func TestODBCEscapeCastType(t *testing.T) {
	cases := []struct {
		sql    string
		oid    defines.MysqlType
		family tree.Family
		format string
	}{
		{"select {d '2020-01-01'}", defines.MYSQL_TYPE_DATE, tree.DateFamily, "select cast(2020-01-01 as date)"},
		{"select {D '2020-01-01'}", defines.MYSQL_TYPE_DATE, tree.DateFamily, "select cast(2020-01-01 as date)"},
		{"select {t '10:20:30'}", defines.MYSQL_TYPE_TIME, tree.TimeFamily, "select cast(10:20:30 as time)"},
		{"select {ts '2020-01-01 10:20:30'}", defines.MYSQL_TYPE_TIMESTAMP, tree.TimestampFamily, "select cast(2020-01-01 10:20:30 as timestamp)"},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), c.sql, 1)
			require.NoError(t, err)
			sel := stmt.(*tree.Select).Select.(*tree.SelectClause)
			cast, ok := sel.Exprs[0].Expr.(*tree.CastExpr)
			require.True(t, ok, "expected CastExpr, got %T", sel.Exprs[0].Expr)
			typ, ok := cast.Type.(*tree.T)
			require.True(t, ok, "expected *tree.T, got %T", cast.Type)
			require.Equal(t, uint32(c.oid), typ.InternalType.Oid)
			require.Equal(t, c.family, typ.InternalType.Family)
			require.Equal(t, c.format, tree.String(stmt, dialect.MYSQL))
		})
	}
}
