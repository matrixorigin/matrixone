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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestCTASConflictModifiersParseAndFormat(t *testing.T) {
	cases := []struct{ sql, modifier string }{
		{"CREATE TABLE t IGNORE AS SELECT 1 AS a", "ignore"},
		{"CREATE TABLE t REPLACE AS SELECT 1 AS a", "replace"},
		{"CREATE TABLE t (a INT) IGNORE AS SELECT 1", "ignore"},
		{"CREATE TABLE t (a INT) REPLACE AS SELECT 1", "replace"},
		{"CREATE TABLE t AS SELECT 1 AS a", ""},
		{"CREATE TABLE t SELECT 1 AS a", ""},
	}
	for _, tc := range cases {
		stmt, err := ParseOne(context.Background(), tc.sql, 1)
		require.NoError(t, err, tc.sql)
		create, ok := stmt.(*tree.CreateTable)
		require.True(t, ok, tc.sql)
		require.True(t, create.IsAsSelect, tc.sql)
		require.Equal(t, tc.modifier, create.CTASConflict, tc.sql)
		formatted := tree.String(create, dialect.MYSQL)
		_, err = ParseOne(context.Background(), formatted, 1)
		require.NoError(t, err, formatted)
	}
}
