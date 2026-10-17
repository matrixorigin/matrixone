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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAlterBinaryConvertRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		sql     string
		convert bool
	}{
		{"ALTER TABLE t CONVERT TO CHARACTER SET binary", true},
		{"ALTER TABLE t CONVERT TO CHARACTER SET binary COLLATE binary", true},
		{"ALTER TABLE t DEFAULT CHARACTER SET binary", false},
		{"ALTER TABLE t DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), tc.sql, 1)
			require.NoError(t, err)
			option := stmt.(*tree.AlterTable).Options[0].(*tree.TableOptionCharset)
			require.Equal(t, tc.convert, option.Convert)
			formatted := tree.String(stmt, dialect.MYSQL)
			again, err := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err)
			reparsed := again.(*tree.AlterTable).Options[0].(*tree.TableOptionCharset)
			require.Equal(t, option.Convert, reparsed.Convert)
			require.Equal(t, option.Charset, reparsed.Charset)
			require.Equal(t, option.Collate, reparsed.Collate)
		})
	}
}

func TestAlterBinaryConvertParenthesizedAddOrder(t *testing.T) {
	for _, tc := range []struct {
		sql       string
		convertAt int
	}{
		{"ALTER TABLE t ADD COLUMN (a VARCHAR(2), b CHAR(2)), CONVERT TO CHARACTER SET binary", 2},
		{"ALTER TABLE t CONVERT TO CHARACTER SET binary, ADD COLUMN (a VARCHAR(2), b CHAR(2))", 0},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			formatted := tree.String(stmt, dialect.MYSQL)
			again, err := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err)
			defer again.Free()
			for _, parsed := range []tree.Statement{stmt, again} {
				options := parsed.(*tree.AlterTable).Options
				require.Len(t, options, 3)
				conversion, ok := options[tc.convertAt].(*tree.TableOptionCharset)
				require.True(t, ok)
				require.True(t, conversion.Convert)
				require.Equal(t, "binary", conversion.Charset)
				names := make([]string, 0, 2)
				for i, option := range options {
					if i == tc.convertAt {
						continue
					}
					add, ok := option.(*tree.AlterAddCol)
					require.True(t, ok)
					names = append(names, add.Column.Name.ColName())
				}
				require.Equal(t, []string{"a", "b"}, names)
			}
		})
	}
}
