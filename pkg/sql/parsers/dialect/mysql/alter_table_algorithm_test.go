// Copyright 2021 Matrix Origin
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestAlterTableAlgorithmLock(t *testing.T) {
	cases := []struct {
		sql       string
		expectAlg string
		expectLck string
	}{
		{
			"ALTER TABLE t ALGORITHM=INPLACE, LOCK=NONE, ADD COLUMN c INT",
			"INPLACE", "NONE",
		},
		{
			"ALTER TABLE t ALGORITHM=COPY, ADD COLUMN c INT",
			"COPY", "",
		},
		{
			"ALTER TABLE t LOCK=SHARED, ADD COLUMN c INT",
			"", "SHARED",
		},
		{
			"ALTER TABLE t ALGORITHM=INSTANT, LOCK=EXCLUSIVE, DROP COLUMN c",
			"INSTANT", "EXCLUSIVE",
		},
		{
			"ALTER TABLE t ALGORITHM=DEFAULT, LOCK=DEFAULT, ADD INDEX idx(a)",
			"DEFAULT", "DEFAULT",
		},
	}

	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := ParseOne(context.TODO(), tc.sql, 1)
			require.NoError(t, err)

			alter, ok := stmt.(*tree.AlterTable)
			require.True(t, ok, "expected AlterTable, got %T", stmt)

			var foundAlg, foundLck string
			for _, opt := range alter.Options {
				switch o := opt.(type) {
				case *tree.AlterOptionAlgorithm:
					foundAlg = o.Type
				case *tree.AlterOptionLock:
					foundLck = o.Type
				}
			}

			if tc.expectAlg != "" {
				assert.Equal(t, tc.expectAlg, foundAlg)
			}
			if tc.expectLck != "" {
				assert.Equal(t, tc.expectLck, foundLck)
			}
		})
	}
}

func TestAlterTableAlgorithmFormatter(t *testing.T) {
	sql := "ALTER TABLE t ALGORITHM=INPLACE, LOCK=NONE, ADD COLUMN c INT"
	stmt, err := ParseOne(context.TODO(), sql, 1)
	require.NoError(t, err)

	formatted := tree.StringWithOpts(stmt, dialect.MYSQL,
		tree.WithQuoteIdentifier(), tree.WithSingleQuoteString())
	// The formatted output should contain "algorithm = INPLACE" and "lock = NONE"
	// and NOT contain "alter algorithm not enforce" or "charset = lock"
	assert.Contains(t, formatted, "algorithm = INPLACE")
	assert.Contains(t, formatted, "lock = NONE")
	assert.NotContains(t, formatted, "not enforce")
	assert.NotContains(t, formatted, "charset = LOCK")
}
