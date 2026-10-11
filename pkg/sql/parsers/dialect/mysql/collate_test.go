// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
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

func TestExpressionCollationRoundTrip(t *testing.T) {
	for _, sql := range []string{
		"select 'a' collate utf8mb4_bin",
		"select (concat('a','b')) collate utf8mb4_bin = 'ab'",
		"select ? collate utf8mb4_bin",
		"select ('a' collate utf8mb4_general_ci) collate utf8mb4_bin",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(t.Context(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			formatted := tree.String(stmt, dialect.MYSQL)
			require.Contains(t, formatted, "collate utf8mb4_bin")
			require.Equal(t, strings.Count(sql, "collate "), strings.Count(formatted, "collate "))
			require.Equal(t, strings.Count(sql, "?"), tree.ParameterCount(stmt))
			reparsed, err := ParseOne(t.Context(), formatted, 1)
			require.NoError(t, err)
			defer reparsed.Free()
			require.Equal(t, formatted, tree.String(reparsed, dialect.MYSQL))
		})
	}
}
