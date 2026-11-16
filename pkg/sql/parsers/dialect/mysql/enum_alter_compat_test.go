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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestAlterCharsetDistinguishesLegacyNoops(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		kind tree.LegacyAlterNoopKind
	}{
		{"alter table t disable keys", tree.LegacyAlterKeys},
		{"alter table t enable keys", tree.LegacyAlterKeys},
		{"alter table t force", tree.LegacyAlterForce},
		{"alter table t discard tablespace", tree.LegacyAlterTablespace},
		{"alter table t import tablespace", tree.LegacyAlterTablespace},
		{"alter table t with validation", tree.LegacyAlterValidation},
		{"alter table t without validation", tree.LegacyAlterValidation},
		{"alter table t character set utf8mb4", tree.LegacyAlterNotNoop},
		{"alter table t character set utf8mb4 collate utf8mb4_bin", tree.LegacyAlterNotNoop},
		{"alter table t convert to character set utf8mb4", tree.LegacyAlterNotNoop},
		{"alter table t convert to character set utf8mb4 collate utf8mb4_bin", tree.LegacyAlterNotNoop},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			option, ok := stmt.(*tree.AlterTable).Options[0].(*tree.TableOptionCharset)
			require.True(t, ok)
			require.Equal(t, tc.kind, option.LegacyAlterKind)
			if tc.kind != tree.LegacyAlterNotNoop {
				formatted := tree.String(stmt, dialect.MYSQL)
				reparsed, err := ParseOne(t.Context(), formatted, 1)
				require.NoError(t, err, formatted)
				replayed := reparsed.(*tree.AlterTable).Options[0].(*tree.TableOptionCharset)
				require.Equal(t, option.LegacyAlterKind, replayed.LegacyAlterKind)
				require.Equal(t, option.Charset, replayed.Charset)
				reparsed.Free()
			}
			stmt.Free()
		})
	}
	// Pooled placeholders must not turn a later real charset into a no-op.
	for i := 0; i < 20; i++ {
		stmt, err := ParseOne(t.Context(), "alter table t disable keys", 1)
		require.NoError(t, err)
		stmt.Free()
		stmt, err = ParseOne(t.Context(), "alter table t character set utf8mb4", 1)
		require.NoError(t, err)
		require.Equal(t, tree.LegacyAlterNotNoop, stmt.(*tree.AlterTable).Options[0].(*tree.TableOptionCharset).LegacyAlterKind)
		stmt.Free()
	}
}
