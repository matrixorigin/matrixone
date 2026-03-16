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

package publication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsIndexTable(t *testing.T) {
	assert.True(t, isIndexTable("__mo_index_secondary_abc"))
	assert.True(t, isIndexTable("__mo_index_unique_xyz"))
	assert.False(t, isIndexTable("my_table"))
	assert.False(t, isIndexTable(""))
}

func TestEscapeSQLIdentifierForDDL(t *testing.T) {
	assert.Equal(t, "hello", escapeSQLIdentifierForDDL("hello"))
	assert.Equal(t, "he``llo", escapeSQLIdentifierForDDL("he`llo"))
	assert.Equal(t, "````", escapeSQLIdentifierForDDL("``"))
	assert.Equal(t, "", escapeSQLIdentifierForDDL(""))
}

func TestParseCreateTableSQL_Valid(t *testing.T) {
	ctx := context.Background()
	sql := "CREATE TABLE `t1` (`id` INT PRIMARY KEY, `name` VARCHAR(100) NOT NULL)"
	stmt, err := parseCreateTableSQL(ctx, sql)
	require.NoError(t, err)
	require.NotNil(t, stmt)
}

func TestParseCreateTableSQL_Invalid(t *testing.T) {
	ctx := context.Background()
	_, err := parseCreateTableSQL(ctx, "NOT A SQL")
	assert.Error(t, err)
}

func TestParseCreateTableSQL_NotCreateTable(t *testing.T) {
	ctx := context.Background()
	_, err := parseCreateTableSQL(ctx, "SELECT 1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not CREATE TABLE")
}

func TestBuildColumnMap(t *testing.T) {
	ctx := context.Background()
	sql := "CREATE TABLE `t1` (`id` INT, `name` VARCHAR(100))"
	stmt, err := parseCreateTableSQL(ctx, sql)
	require.NoError(t, err)

	cols := buildColumnMap(stmt)
	assert.Len(t, cols, 2)

	idCol := cols["id"]
	require.NotNil(t, idCol)
	assert.Equal(t, "id", idCol.name)
	assert.Equal(t, 0, idCol.position)

	nameCol := cols["name"]
	require.NotNil(t, nameCol)
	assert.Equal(t, "name", nameCol.name)
	assert.Equal(t, 1, nameCol.position)
}

func TestBuildIndexMap_Regular(t *testing.T) {
	ctx := context.Background()
	sql := "CREATE TABLE `t1` (`id` INT, `name` VARCHAR(100), INDEX `idx_name` (`name`))"
	stmt, err := parseCreateTableSQL(ctx, sql)
	require.NoError(t, err)

	indexes := buildIndexMap(stmt)
	assert.Len(t, indexes, 1)

	idx := indexes["idx_name"]
	require.NotNil(t, idx)
	assert.Equal(t, "idx_name", idx.name)
	assert.False(t, idx.unique)
	assert.Equal(t, []string{"name"}, idx.columns)
}

func TestBuildIndexMap_Unique(t *testing.T) {
	ctx := context.Background()
	sql := "CREATE TABLE `t1` (`id` INT, `email` VARCHAR(200), UNIQUE INDEX `idx_email` (`email`))"
	stmt, err := parseCreateTableSQL(ctx, sql)
	require.NoError(t, err)

	indexes := buildIndexMap(stmt)
	idx := indexes["idx_email"]
	require.NotNil(t, idx)
	assert.True(t, idx.unique)
}

func TestBuildIndexMap_Fulltext(t *testing.T) {
	ctx := context.Background()
	sql := "CREATE TABLE `t1` (`id` INT, `content` TEXT, FULLTEXT INDEX `ft_content` (`content`))"
	stmt, err := parseCreateTableSQL(ctx, sql)
	require.NoError(t, err)

	indexes := buildIndexMap(stmt)
	idx := indexes["ft_content"]
	require.NotNil(t, idx)
	assert.Equal(t, "FULLTEXT", idx.indexType)
}

func TestBuildForeignKeyMap_Empty(t *testing.T) {
	ctx := context.Background()
	sql := "CREATE TABLE `t1` (`id` INT, `name` VARCHAR(100))"
	stmt, err := parseCreateTableSQL(ctx, sql)
	require.NoError(t, err)

	fks := buildForeignKeyMap(stmt)
	assert.Len(t, fks, 0)
}

func TestGetTableComment(t *testing.T) {
	ctx := context.Background()
	sql := "CREATE TABLE `t1` (`id` INT) COMMENT 'my table'"
	stmt, err := parseCreateTableSQL(ctx, sql)
	require.NoError(t, err)
	assert.Equal(t, "my table", getTableComment(stmt))
}

func TestGetTableComment_NoComment(t *testing.T) {
	ctx := context.Background()
	sql := "CREATE TABLE `t1` (`id` INT)"
	stmt, err := parseCreateTableSQL(ctx, sql)
	require.NoError(t, err)
	assert.Equal(t, "", getTableComment(stmt))
}

func TestCanDoColumnChangesInplace_Same(t *testing.T) {
	old := map[string]*columnInfo{
		"id":   {name: "id", typ: "INT", position: 0, nullable: false},
		"name": {name: "name", typ: "VARCHAR(100)", position: 1, nullable: true},
	}
	new := map[string]*columnInfo{
		"id":   {name: "id", typ: "INT", position: 0, nullable: false},
		"name": {name: "name", typ: "VARCHAR(100)", position: 1, nullable: true},
	}
	assert.True(t, canDoColumnChangesInplace(old, new))
}

func TestCanDoColumnChangesInplace_Rename(t *testing.T) {
	old := map[string]*columnInfo{
		"name": {name: "name", typ: "VARCHAR(100)", position: 1, nullable: true},
		"id":   {name: "id", typ: "INT", position: 0, nullable: false},
	}
	new := map[string]*columnInfo{
		"username": {name: "username", typ: "VARCHAR(100)", position: 1, nullable: true},
		"id":       {name: "id", typ: "INT", position: 0, nullable: false},
	}
	assert.True(t, canDoColumnChangesInplace(old, new))
}

func TestCanDoColumnChangesInplace_DifferentCount(t *testing.T) {
	old := map[string]*columnInfo{
		"id": {name: "id", typ: "INT", position: 0},
	}
	new := map[string]*columnInfo{
		"id":   {name: "id", typ: "INT", position: 0},
		"name": {name: "name", typ: "VARCHAR(100)", position: 1},
	}
	assert.False(t, canDoColumnChangesInplace(old, new))
}

func TestCanDoColumnChangesInplace_TypeChanged(t *testing.T) {
	old := map[string]*columnInfo{
		"id": {name: "id", typ: "INT", position: 0, nullable: false},
	}
	new := map[string]*columnInfo{
		"id": {name: "id", typ: "BIGINT", position: 0, nullable: false},
	}
	assert.False(t, canDoColumnChangesInplace(old, new))
}

func TestCanDoColumnChangesInplace_SameNameDifferentPosition(t *testing.T) {
	old := map[string]*columnInfo{
		"a": {name: "a", typ: "INT", position: 0},
		"b": {name: "b", typ: "INT", position: 1},
	}
	new := map[string]*columnInfo{
		"a": {name: "a", typ: "INT", position: 1},
		"b": {name: "b", typ: "INT", position: 0},
	}
	assert.False(t, canDoColumnChangesInplace(old, new))
}

func TestGenerateAddIndexStatement_Regular(t *testing.T) {
	idx := &indexInfo{name: "idx1", columns: []string{"col1", "col2"}}
	stmt := generateAddIndexStatement("`db`.`t1`", "idx1", idx)
	assert.Contains(t, stmt, "ADD INDEX")
	assert.Contains(t, stmt, "`col1`")
	assert.Contains(t, stmt, "`col2`")
}

func TestGenerateAddIndexStatement_Unique(t *testing.T) {
	idx := &indexInfo{name: "idx1", unique: true, columns: []string{"col1"}}
	stmt := generateAddIndexStatement("`db`.`t1`", "idx1", idx)
	assert.Contains(t, stmt, "ADD UNIQUE INDEX")
}

func TestGenerateAddIndexStatement_Fulltext(t *testing.T) {
	idx := &indexInfo{name: "ft1", columns: []string{"content"}, indexType: "FULLTEXT"}
	stmt := generateAddIndexStatement("`db`.`t1`", "ft1", idx)
	assert.Contains(t, stmt, "ADD FULLTEXT INDEX")
}

func TestGenerateAddIndexStatement_IVFFlat(t *testing.T) {
	idx := &indexInfo{
		name:                  "vec_idx",
		columns:               []string{"embedding"},
		indexType:             "ivfflat",
		algoParamList:         100,
		algoParamVectorOpType: "vector_l2_ops",
	}
	stmt := generateAddIndexStatement("`db`.`t1`", "vec_idx", idx)
	assert.Contains(t, stmt, "USING ivfflat")
	assert.Contains(t, stmt, "LISTS = 100")
	assert.Contains(t, stmt, "OP_TYPE 'vector_l2_ops'")
}

func TestGenerateAddIndexStatement_HNSW(t *testing.T) {
	idx := &indexInfo{
		name:                  "hnsw_idx",
		columns:               []string{"embedding"},
		indexType:             "hnsw",
		hnswM:                 16,
		hnswEfConstruction:    200,
		algoParamVectorOpType: "vector_l2_ops",
	}
	stmt := generateAddIndexStatement("`db`.`t1`", "hnsw_idx", idx)
	assert.Contains(t, stmt, "USING hnsw")
	assert.Contains(t, stmt, "M = 16")
	assert.Contains(t, stmt, "EF_CONSTRUCTION = 200")
	assert.Contains(t, stmt, "OP_TYPE 'vector_l2_ops'")
}

func TestGenerateAddForeignKeyStatement(t *testing.T) {
	fk := &foreignKeyInfo{
		name:       "fk_user",
		columns:    []string{"user_id"},
		refTable:   "`users`",
		refColumns: []string{"id"},
		onDelete:   "CASCADE",
		onUpdate:   "RESTRICT",
	}
	stmt := generateAddForeignKeyStatement("`db`.`orders`", "fk_user", fk)
	assert.Contains(t, stmt, "ADD CONSTRAINT `fk_user`")
	assert.Contains(t, stmt, "FOREIGN KEY (`user_id`)")
	assert.Contains(t, stmt, "REFERENCES `users` (`id`)")
	assert.Contains(t, stmt, "ON DELETE CASCADE")
	assert.NotContains(t, stmt, "ON UPDATE") // RESTRICT is skipped
}

func TestGenerateColumnRenameStatements(t *testing.T) {
	ctx := context.Background()
	old := map[string]*columnInfo{
		"name": {name: "name", typ: "VARCHAR(100)", position: 1},
		"id":   {name: "id", typ: "INT", position: 0},
	}
	new := map[string]*columnInfo{
		"username": {name: "username", typ: "VARCHAR(100)", position: 1},
		"id":       {name: "id", typ: "INT", position: 0},
	}
	stmts := generateColumnRenameStatements(ctx, "`db`.`t1`", old, new)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "RENAME COLUMN `name` TO `username`")
}

func TestGenerateColumnRenameStatements_NoRename(t *testing.T) {
	ctx := context.Background()
	old := map[string]*columnInfo{
		"id": {name: "id", typ: "INT", position: 0},
	}
	stmts := generateColumnRenameStatements(ctx, "`db`.`t1`", old, old)
	assert.Empty(t, stmts)
}

func TestCompareTableDefsAndGenerateAlterStatements_NoChange(t *testing.T) {
	ctx := context.Background()
	sql := "CREATE TABLE `t1` (`id` INT, `name` VARCHAR(100))"
	stmts, ok, err := compareTableDefsAndGenerateAlterStatements(ctx, "db", "t1", sql, sql)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Empty(t, stmts)
}

func TestCompareTableDefsAndGenerateAlterStatements_AddIndex(t *testing.T) {
	ctx := context.Background()
	oldSQL := "CREATE TABLE `t1` (`id` INT, `name` VARCHAR(100))"
	newSQL := "CREATE TABLE `t1` (`id` INT, `name` VARCHAR(100), INDEX `idx_name` (`name`))"
	stmts, ok, err := compareTableDefsAndGenerateAlterStatements(ctx, "db", "t1", oldSQL, newSQL)
	require.NoError(t, err)
	assert.True(t, ok)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "ADD INDEX `idx_name`")
}

func TestCompareTableDefsAndGenerateAlterStatements_DropIndex(t *testing.T) {
	ctx := context.Background()
	oldSQL := "CREATE TABLE `t1` (`id` INT, `name` VARCHAR(100), INDEX `idx_name` (`name`))"
	newSQL := "CREATE TABLE `t1` (`id` INT, `name` VARCHAR(100))"
	stmts, ok, err := compareTableDefsAndGenerateAlterStatements(ctx, "db", "t1", oldSQL, newSQL)
	require.NoError(t, err)
	assert.True(t, ok)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "DROP INDEX `idx_name`")
}

func TestCompareTableDefsAndGenerateAlterStatements_CommentChange(t *testing.T) {
	ctx := context.Background()
	oldSQL := "CREATE TABLE `t1` (`id` INT) COMMENT 'old'"
	newSQL := "CREATE TABLE `t1` (`id` INT) COMMENT 'new'"
	stmts, ok, err := compareTableDefsAndGenerateAlterStatements(ctx, "db", "t1", oldSQL, newSQL)
	require.NoError(t, err)
	assert.True(t, ok)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "COMMENT 'new'")
}

func TestCompareTableDefsAndGenerateAlterStatements_ColumnAdded(t *testing.T) {
	ctx := context.Background()
	oldSQL := "CREATE TABLE `t1` (`id` INT)"
	newSQL := "CREATE TABLE `t1` (`id` INT, `name` VARCHAR(100))"
	_, _, err := compareTableDefsAndGenerateAlterStatements(ctx, "db", "t1", oldSQL, newSQL)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be done inplace")
}

func TestCompareTableDefsAndGenerateAlterStatements_InvalidOldSQL(t *testing.T) {
	ctx := context.Background()
	_, _, err := compareTableDefsAndGenerateAlterStatements(ctx, "db", "t1", "INVALID", "CREATE TABLE `t1` (`id` INT)")
	assert.Error(t, err)
}

func TestCompareTableDefsAndGenerateAlterStatements_InvalidNewSQL(t *testing.T) {
	ctx := context.Background()
	_, _, err := compareTableDefsAndGenerateAlterStatements(ctx, "db", "t1", "CREATE TABLE `t1` (`id` INT)", "INVALID")
	assert.Error(t, err)
}

func TestCompareTableDefsAndGenerateAlterStatements_ColumnRename(t *testing.T) {
	ctx := context.Background()
	oldSQL := "CREATE TABLE `t1` (`id` INT NOT NULL, `name` VARCHAR(100))"
	newSQL := "CREATE TABLE `t1` (`id` INT NOT NULL, `username` VARCHAR(100))"
	stmts, ok, err := compareTableDefsAndGenerateAlterStatements(ctx, "db", "t1", oldSQL, newSQL)
	require.NoError(t, err)
	assert.True(t, ok)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "RENAME COLUMN")
}

func TestFormatTypeReference_Nil(t *testing.T) {
	assert.Equal(t, "", formatTypeReference(nil))
}

func TestCompareTableDefs_IndexVisibilityChange(t *testing.T) {
	ctx := context.Background()
	oldSQL := "CREATE TABLE t1 (id INT, name VARCHAR(50), INDEX idx_name (name))"
	newSQL := "CREATE TABLE t1 (id INT, name VARCHAR(50), INDEX idx_name (name) INVISIBLE)"
	stmts, _, err := compareTableDefsAndGenerateAlterStatements(ctx, "db", "t1", oldSQL, newSQL)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0], "INVISIBLE")
}

func TestGenerateAddIndexStatement_IVFFlatWithParams(t *testing.T) {
	idx := &indexInfo{
		columns:               []string{"embedding"},
		indexType:             "ivfflat",
		algoParamList:         100,
		algoParamVectorOpType: "vector_l2_ops",
	}
	stmt := generateAddIndexStatement("`db`.`t1`", "idx_emb", idx)
	assert.Contains(t, stmt, "USING ivfflat")
	assert.Contains(t, stmt, "LISTS = 100")
	assert.Contains(t, stmt, "OP_TYPE 'vector_l2_ops'")
}

func TestGenerateAddIndexStatement_HNSWWithParams(t *testing.T) {
	idx := &indexInfo{
		columns:               []string{"embedding"},
		indexType:             "hnsw",
		hnswM:                 16,
		hnswEfConstruction:    200,
		algoParamVectorOpType: "vector_l2_ops",
	}
	stmt := generateAddIndexStatement("`db`.`t1`", "idx_emb", idx)
	assert.Contains(t, stmt, "USING hnsw")
	assert.Contains(t, stmt, "M = 16")
	assert.Contains(t, stmt, "EF_CONSTRUCTION = 200")
	assert.Contains(t, stmt, "OP_TYPE 'vector_l2_ops'")
}

func TestCanDoColumnChangesInplace_NullableDiff(t *testing.T) {
	oldCols := map[string]*columnInfo{
		"id": {typ: "INT", position: 0, nullable: false},
	}
	newCols := map[string]*columnInfo{
		"id": {typ: "INT", position: 0, nullable: true},
	}
	assert.False(t, canDoColumnChangesInplace(oldCols, newCols))
}
