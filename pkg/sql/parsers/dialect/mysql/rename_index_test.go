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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestRenameIndex(t *testing.T) {
	for _, keyword := range []string{"INDEX", "KEY"} {
		t.Run(keyword, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), "ALTER TABLE t RENAME "+keyword+" `Old``Name` TO `New Name`", 1)
			require.NoError(t, err)
			defer stmt.Free()
			option := stmt.(*tree.AlterTable).Options[0].(*tree.AlterTableRenameIndexClause)
			require.Equal(t, "Old`Name", option.OldName)
			require.Equal(t, "New Name", option.NewName)
			again, err := ParseOne(context.Background(), tree.String(stmt, dialect.MYSQL), 1)
			require.NoError(t, err)
			defer again.Free()
			require.Equal(t, option, again.(*tree.AlterTable).Options[0])
		})
	}
	stmt, err := ParseOne(context.Background(), "ALTER TABLE t RENAME TO u", 1)
	require.NoError(t, err)
	defer stmt.Free()
	require.IsType(t, &tree.AlterOptionTableName{}, stmt.(*tree.AlterTable).Options[0])
	for _, sql := range []string{"ALTER TABLE t RENAME INDEX TO x", "ALTER TABLE t RENAME KEY x TO"} {
		_, err := ParseOne(context.Background(), sql, 1)
		require.Error(t, err)
	}
}
