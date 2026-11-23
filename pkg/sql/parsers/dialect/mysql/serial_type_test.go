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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// firstCreateTableColumn parses sql as a single-column CREATE TABLE and
// returns the parsed statement and its first column definition.
func firstCreateTableColumn(t *testing.T, sql string) (*tree.CreateTable, *tree.ColumnTableDef) {
	t.Helper()
	stmt, err := ParseOne(context.Background(), sql, 1)
	require.NoError(t, err, sql)
	ct, ok := stmt.(*tree.CreateTable)
	require.True(t, ok, sql)
	require.Len(t, ct.Defs, 1, sql)
	col, ok := ct.Defs[0].(*tree.ColumnTableDef)
	require.True(t, ok, sql)
	return ct, col
}

// requireSerialMatchesCanonical runs the #31748 oracle: the tree parsed from
// the SERIAL spelling (with any user suffix) must be deeply equal to the tree
// parsed from the canonical MySQL expansion
// `bigint unsigned not null auto_increment unique` (with the same suffix),
// must carry exactly the canonical type body and attribute prefix, and its
// formatted text must reparse to an identical tree.
func requireSerialMatchesCanonical(t *testing.T, suffix string) {
	t.Helper()
	serialSQL := "create table t (id serial " + suffix + ")"
	canonicalSQL := "create table t (id bigint unsigned not null auto_increment unique " + suffix + ")"

	serialStmt, serialCol := firstCreateTableColumn(t, serialSQL)
	canonicalStmt, canonicalCol := firstCreateTableColumn(t, canonicalSQL)

	require.Equal(t, canonicalStmt, serialStmt, serialSQL)
	require.Equal(t, canonicalCol, serialCol, serialSQL)

	typ, ok := serialCol.Type.(*tree.T)
	require.True(t, ok, serialSQL)
	require.Equal(t, tree.IntFamily, typ.InternalType.Family, serialSQL)
	require.EqualValues(t, 64, typ.InternalType.Width, serialSQL)
	require.Equal(t, uint32(defines.MYSQL_TYPE_LONGLONG), typ.InternalType.Oid, serialSQL)
	require.True(t, typ.InternalType.Unsigned, serialSQL)

	// Canonical attribute prefix: NOT NULL, AUTO_INCREMENT, UNIQUE, then any
	// user-written attributes; the count and full order already match the
	// canonical expansion via the deep equality above.
	require.GreaterOrEqual(t, len(serialCol.Attributes), 3, serialSQL)
	nullAttr, ok := serialCol.Attributes[0].(*tree.AttributeNull)
	require.True(t, ok, serialSQL)
	require.False(t, nullAttr.Is, serialSQL)
	_, ok = serialCol.Attributes[1].(*tree.AttributeAutoIncrement)
	require.True(t, ok, serialSQL)
	_, ok = serialCol.Attributes[2].(*tree.AttributeUnique)
	require.True(t, ok, serialSQL)

	// Canonical generation provenance: the formatted SERIAL statement equals
	// the formatted canonical statement and replays to the same tree.
	// The replay oracle is asserted for the bare suffix case; a comment
	// suffix is not replayable because the pre-existing tree formatter does
	// not re-quote the comment string value (formatter behavior outside this
	// batch's scope, identical for the canonical spelling).
	serialFormatted := tree.String(serialStmt, dialect.MYSQL)
	require.Equal(t, tree.String(canonicalStmt, dialect.MYSQL), serialFormatted, serialSQL)
	if suffix == "" {
		reparsed, err := ParseOne(context.Background(), serialFormatted, 1)
		require.NoError(t, err, serialFormatted)
		require.Equal(t, serialStmt, reparsed, serialFormatted)
	}
}

func TestSerialTypeAliasMatchesCanonical(t *testing.T) {
	requireSerialMatchesCanonical(t, "")
}

func TestSerialWithUserAttributesMatchesCanonical(t *testing.T) {
	requireSerialMatchesCanonical(t, "comment 'c1'")
}

func TestSerialWithExplicitNotNullConflictMatchesCanonical(t *testing.T) {
	// A redundant explicit NOT NULL after SERIAL must behave exactly like the
	// same suffix after the canonical expansion, including attribute ordering.
	requireSerialMatchesCanonical(t, "not null")
}

// SERIAL is a column-definition alias only. It must not leak into the
// CAST/CONVERT type slot or accept a numeric display-width modifier, matching
// primary MySQL, where the serial type is separate from cast_type.
func TestSerialBoundaryNegativeControls(t *testing.T) {
	for _, sql := range []string{
		"select cast(1 as serial)",
		"select convert(1, serial)",
		"create table t (id serial(11))",
		"create sequence s as serial",
	} {
		_, err := ParseOne(context.Background(), sql, 1)
		require.Error(t, err, sql)
	}
}

func TestSerialCoexistsInColumnList(t *testing.T) {
	_, err := ParseOne(context.Background(), "create table t (id serial, note varchar(8))", 1)
	require.NoError(t, err)
}

func TestSerialInAlterTableAddColumnMatchesCanonical(t *testing.T) {
	serialSQL := "alter table t add column id serial"
	canonicalSQL := "alter table t add column id bigint unsigned not null auto_increment unique"

	serialStmt, err := ParseOne(context.Background(), serialSQL, 1)
	require.NoError(t, err, serialSQL)
	canonicalStmt, err := ParseOne(context.Background(), canonicalSQL, 1)
	require.NoError(t, err, canonicalSQL)
	require.Equal(t, canonicalStmt, serialStmt, serialSQL)
}

func TestSerialRemainsUsableAsIdentifier(t *testing.T) {
	for _, sql := range []string{
		"create table serial (a int)",
		"create table t (serial int)",
		"select serial from t2",
		"select * from serial",
		"create table serial (serial serial)",
	} {
		_, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
	}
}

func TestNonMySQLSerialVariantsRejected(t *testing.T) {
	for _, sql := range []string{
		"create table t (id bigserial)",
		"create table t (id smallserial)",
	} {
		_, err := ParseOne(context.Background(), sql, 1)
		require.Error(t, err, sql)
	}
}
