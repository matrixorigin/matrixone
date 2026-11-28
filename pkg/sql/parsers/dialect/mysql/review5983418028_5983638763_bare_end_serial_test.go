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

// REVIEW 5983418028 / 5983638763 on PR 32906: the serial column-definition
// alternative must accept the same declaration-name owner as the ordinary
// column_type alternative, so `end SERIAL` parses without quoting exactly
// like `"end" SERIAL`, `other SERIAL`, and the bare `end` with the explicit
// SERIAL expansion (BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE).
//
// The typed oracle compares the fully formatted definitions of the four
// declarations: identical formatting proves identical parsed type and
// attribute lists without coupling the assertions to internal field拼写.
func TestBareEndSerialMatchesExplicitExpansionTwins(t *testing.T) {
	formatted := map[string]string{}
	for name, sql := range map[string]string{
		"bare_end_serial":     "CREATE TABLE t (end SERIAL)",
		"quoted_end_serial":   "CREATE TABLE t (`end` SERIAL)",
		"other_name_serial":   "CREATE TABLE t (id SERIAL)",
		"bare_end_expanded":   "CREATE TABLE t (end BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE)",
		"quoted_end_expanded": "CREATE TABLE t (`end` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE)",
		"other_name_expanded": "CREATE TABLE t (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE)",
	} {
		t.Run(name, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err, sql)
			create, ok := stmt.(*tree.CreateTable)
			require.True(t, ok, sql)
			require.Equal(t, 1, len(create.Defs), sql)
			formatted[name] = tree.String(stmt, dialect.MYSQL)
		})
	}
	bare := formatted["bare_end_serial"]
	require.Contains(t, bare, "end", bare)
	require.Contains(t, bare, "bigint unsigned", bare)
	require.Contains(t, bare, "not null", bare)
	require.Contains(t, bare, "auto_increment", bare)
	require.Contains(t, bare, "unique", bare)
	// same name ⇒ exactly identical definitions between SERIAL and its expansion
	require.Equal(t, formatted["bare_end_expanded"], bare, "bare end SERIAL must equal its explicit expansion")
	require.Equal(t, formatted["quoted_end_serial"], formatted["quoted_end_expanded"], "quoted end SERIAL must equal its explicit expansion")
	require.Equal(t, formatted["other_name_serial"], formatted["other_name_expanded"], "other-name SERIAL must equal its explicit expansion")
}

// The ALTER ADD column owner shares the same declaration rule.
func TestBareEndSerialInAlterAddColumn(t *testing.T) {
	sql := "ALTER TABLE t ADD COLUMN end SERIAL"
	stmt, err := ParseOne(context.Background(), sql, 1)
	require.NoError(t, err, sql)
	formatted := tree.String(stmt, dialect.MYSQL)
	require.Contains(t, formatted, "end", formatted)
	require.Contains(t, formatted, "auto_increment", formatted)
	require.Contains(t, formatted, "unique", formatted)
}
