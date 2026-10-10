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

package mysql

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestAlterCharsetConversionIntentRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		sql     string
		convert bool
	}{
		{"alter table t default character set=utf8mb4 collate=utf8mb4_bin", false},
		{"alter table t convert to character set utf8mb4", true},
		{"alter table t convert to character set utf8mb4 collate=utf8mb4_bin", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			statement, err := ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			defer statement.Free()
			option := statement.(*tree.AlterTable).Options[0].(*tree.TableOptionCharset)
			require.Equal(t, tc.convert, option.Convert)
			formatted := tree.String(statement, dialect.MYSQL)
			replayed, err := ParseOne(t.Context(), formatted, 1)
			require.NoError(t, err, formatted)
			defer replayed.Free()
			require.Equal(t, tc.convert, replayed.(*tree.AlterTable).Options[0].(*tree.TableOptionCharset).Convert)
			require.Equal(t, option.Collate, replayed.(*tree.AlterTable).Options[0].(*tree.TableOptionCharset).Collate)
		})
	}
}
