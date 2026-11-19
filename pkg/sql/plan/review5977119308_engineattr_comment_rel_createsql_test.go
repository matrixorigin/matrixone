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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// rel_createsql is persisted to mo_catalog.mo_tables and reparsed whenever the
// DDL has to be rebuilt, so every table option must reserialize as SQL the
// parser accepts. Review 5977119308 on PR 32906: ENGINE_ATTRIBUTE and
// SECONDARY_ENGINE_ATTRIBUTE serialized their decoded value without the
// required STRING quotes, and COMMENT left a decoded trailing backslash
// unescaped so it escaped the closing quote; both malformed the stored
// CTAS statement. These tests drive the real rel_createsql producer and the
// real catalog reparse consumer with the reviewer's exact values, for both the
// CTAS and the plain CREATE owners of the shared option formatter.
func relCreateSQLRoundtrip(t *testing.T, sql string) (*tree.CreateTable, string) {
	t.Helper()
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, sql)
	create, ok := stmt.(*tree.CreateTable)
	require.True(t, ok, sql)

	// The multi-statement request fallback serializes this statement's
	// canonical AST, which is the exact rel_createsql owner consumed by the
	// catalog (mirrors the review 5967618815 PROPERTIES closure).
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	createSQL := createTableSQLForCatalog(ctx, create)
	require.NotEmpty(t, createSQL, sql)

	reStmt, reErr := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
	require.NoError(t, reErr, createSQL)
	reCreate, ok := reStmt.(*tree.CreateTable)
	require.True(t, ok, createSQL)
	return reCreate, createSQL
}

func engineAttrOf(stmt *tree.CreateTable) (string, bool) {
	for _, opt := range stmt.Options {
		if attr, ok := opt.(*tree.TableOptionEngineAttr); ok {
			return attr.Engine, true
		}
	}
	return "", false
}

func secondaryEngineAttrOf(stmt *tree.CreateTable) (string, bool) {
	for _, opt := range stmt.Options {
		if attr, ok := opt.(*tree.TableOptionSecondaryEngineAttr); ok {
			return attr.Attr, true
		}
	}
	return "", false
}

func commentOf(stmt *tree.CreateTable) (string, bool) {
	for _, opt := range stmt.Options {
		if comment, ok := opt.(*tree.TableOptionComment); ok {
			return comment.Comment, true
		}
	}
	return "", false
}

func propertiesOf(stmt *tree.CreateTable) map[string]string {
	props := map[string]string{}
	for _, opt := range stmt.Options {
		if p, ok := opt.(*tree.TableOptionProperties); ok {
			for _, kv := range p.Preperties {
				props[kv.Key] = kv.Value
			}
		}
	}
	return props
}

func TestCTASEngineAttributeAndCommentRelCreateSQLReparses(t *testing.T) {
	sql := "CREATE TABLE tq ENGINE_ATTRIBUTE 'example' COMMENT 'ends with backslash\\\\' AS SELECT 1 AS a"
	_, createSQL := relCreateSQLRoundtrip(t, sql)

	require.Contains(t, createSQL, "ENGINE_ATTRIBUTE = 'example'", createSQL)
	require.Contains(t, createSQL, "comment = 'ends with backslash\\\\'", createSQL)
}

func TestCTASEngineAttributeQuoteAndEmptyValueControls(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE tq ENGINE_ATTRIBUTE 'va l''ue' AS SELECT 1 AS a",
		"CREATE TABLE tq ENGINE_ATTRIBUTE '' AS SELECT 1 AS a",
	} {
		t.Run(sql, func(t *testing.T) {
			reCreate, createSQL := relCreateSQLRoundtrip(t, sql)
			got, ok := engineAttrOf(reCreate)
			require.True(t, ok, createSQL)
			original, _ := engineAttrOf(mustCreateTable(t, sql))
			require.Equal(t, original, got, createSQL)
			if original == "va l'ue" {
				require.Contains(t, createSQL, "ENGINE_ATTRIBUTE = 'va l''ue'", createSQL)
			}
		})
	}
}

func TestCTASSecondaryEngineAttributeRelCreateSQLReparses(t *testing.T) {
	sql := "CREATE TABLE tq SECONDARY_ENGINE_ATTRIBUTE 'secondary attr backslash\\\\' AS SELECT 1 AS a"
	reCreate, createSQL := relCreateSQLRoundtrip(t, sql)
	got, ok := secondaryEngineAttrOf(reCreate)
	require.True(t, ok, createSQL)
	require.Equal(t, "secondary attr backslash\\", got, createSQL)
	require.Contains(t, createSQL, "SECONDARY_ENGINE_ATTRIBUTE = 'secondary attr backslash\\\\'", createSQL)
}

func TestCTASCommentControlsRelCreateSQLReparses(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE tq COMMENT 'plain comment' AS SELECT 1 AS a",
		"CREATE TABLE tq COMMENT 'it''s quoted' AS SELECT 1 AS a",
	} {
		t.Run(sql, func(t *testing.T) {
			reCreate, createSQL := relCreateSQLRoundtrip(t, sql)
			got, ok := commentOf(reCreate)
			require.True(t, ok, createSQL)
			original, _ := commentOf(mustCreateTable(t, sql))
			require.Equal(t, original, got, createSQL)
		})
	}
}

// The already-passing escaped-PROPERTIES closure must keep its decoded
// key/value bytes while ENGINE_ATTRIBUTE and COMMENT share the same statement.
func TestCTASPropertiesCoexistsWithEngineAttributeAndComment(t *testing.T) {
	sql := "CREATE TABLE tq PROPERTIES ('k'='va l''ue') ENGINE_ATTRIBUTE 'example' COMMENT 'ends with backslash\\\\' AS SELECT 1 AS a"
	reCreate, createSQL := relCreateSQLRoundtrip(t, sql)
	require.Equal(t, map[string]string{"k": "va l'ue"}, propertiesOf(reCreate), createSQL)
	engine, ok := engineAttrOf(reCreate)
	require.True(t, ok, createSQL)
	require.Equal(t, "example", engine, createSQL)
	comment, ok := commentOf(reCreate)
	require.True(t, ok, createSQL)
	require.Equal(t, "ends with backslash\\", comment, createSQL)
}

// The shared formatter is the same owner for plain CREATE TABLE: the stored
// statement must reparse with identical decoded option values.
func TestPlainCreateEngineAttributeAndCommentRelCreateSQLReparses(t *testing.T) {
	sql := "CREATE TABLE pl (a INT) ENGINE_ATTRIBUTE 'example' COMMENT 'ends with backslash\\\\'"
	reCreate, createSQL := relCreateSQLRoundtrip(t, sql)
	engine, ok := engineAttrOf(reCreate)
	require.True(t, ok, createSQL)
	require.Equal(t, "example", engine, createSQL)
	comment, ok := commentOf(reCreate)
	require.True(t, ok, createSQL)
	require.Equal(t, "ends with backslash\\", comment, createSQL)
}

func mustCreateTable(t *testing.T, sql string) *tree.CreateTable {
	t.Helper()
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, sql)
	create, ok := stmt.(*tree.CreateTable)
	require.True(t, ok, sql)
	return create
}
