// Copyright 2024 Matrix Origin
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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_parseCreateTableSQL_GoodPath(t *testing.T) {
	ctx := context.Background()

	t.Run("simple table", func(t *testing.T) {
		sql := "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))"
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)
		assert.NotNil(t, stmt)
		assert.Len(t, stmt.Defs, 2)
	})

	t.Run("table with index", func(t *testing.T) {
		sql := "CREATE TABLE orders (id INT, customer_id INT, INDEX idx_customer (customer_id))"
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)
		assert.NotNil(t, stmt)
		assert.Len(t, stmt.Defs, 3)
	})

	t.Run("table with foreign key", func(t *testing.T) {
		sql := "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, FOREIGN KEY fk_customer (customer_id) REFERENCES customers(id))"
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)
		assert.NotNil(t, stmt)
	})

	t.Run("table with comment", func(t *testing.T) {
		sql := "CREATE TABLE test (id INT) COMMENT 'test table'"
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)
		assert.NotNil(t, stmt)
	})
}

func Test_buildColumnMap_GoodPath(t *testing.T) {
	ctx := context.Background()

	t.Run("columns with various attributes", func(t *testing.T) {
		sql := `CREATE TABLE users (
			id INT NOT NULL,
			name VARCHAR(100) DEFAULT 'unknown',
			email VARCHAR(255) COMMENT 'user email',
			age INT
		)`
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)

		colMap := buildColumnMap(stmt)
		assert.Len(t, colMap, 4)

		// Check id column
		idCol := colMap["id"]
		assert.NotNil(t, idCol)
		assert.Equal(t, "id", idCol.name)
		assert.Equal(t, 0, idCol.position)
		// Note: nullable field behavior depends on AttributeNull.Is logic in source code

		// Check name column with default
		nameCol := colMap["name"]
		assert.NotNil(t, nameCol)
		assert.Equal(t, "name", nameCol.name)
		assert.Equal(t, 1, nameCol.position)
		assert.Contains(t, nameCol.defaultVal, "unknown")

		// Check email column with comment
		emailCol := colMap["email"]
		assert.NotNil(t, emailCol)
		assert.Equal(t, "email", emailCol.name)
		assert.Equal(t, 2, emailCol.position)

		// Check age column position
		ageCol := colMap["age"]
		assert.NotNil(t, ageCol)
		assert.Equal(t, 3, ageCol.position)
	})
}

func Test_buildIndexMap_GoodPath(t *testing.T) {
	ctx := context.Background()

	t.Run("various index types", func(t *testing.T) {
		sql := `CREATE TABLE test (
			id INT,
			name VARCHAR(100),
			description TEXT,
			INDEX idx_name (name),
			UNIQUE INDEX idx_id (id),
			FULLTEXT INDEX idx_desc (description)
		)`
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)

		idxMap := buildIndexMap(stmt)
		assert.Len(t, idxMap, 3)

		// Check regular index
		nameIdx := idxMap["idx_name"]
		assert.NotNil(t, nameIdx)
		assert.False(t, nameIdx.unique)
		assert.Equal(t, []string{"name"}, nameIdx.columns)
		assert.True(t, nameIdx.visible)

		// Check unique index
		idIdx := idxMap["idx_id"]
		assert.NotNil(t, idIdx)
		assert.True(t, idIdx.unique)
		assert.Equal(t, []string{"id"}, idIdx.columns)

		// Check fulltext index
		descIdx := idxMap["idx_desc"]
		assert.NotNil(t, descIdx)
		assert.Equal(t, "FULLTEXT", descIdx.indexType)
		assert.Equal(t, []string{"description"}, descIdx.columns)
	})

	t.Run("composite index", func(t *testing.T) {
		sql := `CREATE TABLE test (
			id INT,
			first_name VARCHAR(50),
			last_name VARCHAR(50),
			INDEX idx_fullname (first_name, last_name)
		)`
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)

		idxMap := buildIndexMap(stmt)
		idx := idxMap["idx_fullname"]
		assert.NotNil(t, idx)
		assert.Equal(t, []string{"first_name", "last_name"}, idx.columns)
	})
}

func Test_buildForeignKeyMap_GoodPath(t *testing.T) {
	ctx := context.Background()

	t.Run("foreign key with on delete cascade", func(t *testing.T) {
		// Use FOREIGN KEY name (...) syntax, as buildForeignKeyMap uses fk.Name field
		sql := `CREATE TABLE orders (
			id INT PRIMARY KEY,
			customer_id INT,
			FOREIGN KEY fk_customer (customer_id) REFERENCES customers(id) ON DELETE CASCADE
		)`
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)

		fkMap := buildForeignKeyMap(stmt)
		assert.Len(t, fkMap, 1)

		fk := fkMap["fk_customer"]
		assert.NotNil(t, fk)
		assert.Equal(t, "fk_customer", fk.name)
		assert.Equal(t, []string{"customer_id"}, fk.columns)
		assert.Contains(t, fk.refTable, "customers")
		assert.Equal(t, []string{"id"}, fk.refColumns)
		assert.EqualValues(t, "cascade", fk.onDelete)
	})

	t.Run("multiple foreign keys", func(t *testing.T) {
		sql := `CREATE TABLE order_items (
			id INT PRIMARY KEY,
			order_id INT,
			product_id INT,
			FOREIGN KEY fk_order (order_id) REFERENCES orders(id),
			FOREIGN KEY fk_product (product_id) REFERENCES products(id)
		)`
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)

		fkMap := buildForeignKeyMap(stmt)
		assert.Len(t, fkMap, 2)
		assert.Contains(t, fkMap, "fk_order")
		assert.Contains(t, fkMap, "fk_product")
	})
}

func Test_getTableComment_GoodPath(t *testing.T) {
	ctx := context.Background()

	t.Run("table with comment", func(t *testing.T) {
		sql := "CREATE TABLE test (id INT) COMMENT 'This is a test table'"
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)

		comment := getTableComment(stmt)
		assert.Equal(t, "This is a test table", comment)
	})

	t.Run("table without comment", func(t *testing.T) {
		sql := "CREATE TABLE test (id INT)"
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)

		comment := getTableComment(stmt)
		assert.Empty(t, comment)
	})
}

func Test_canDoColumnChangesInplace_GoodPath(t *testing.T) {
	ctx := context.Background()

	t.Run("identical columns", func(t *testing.T) {
		sql := "CREATE TABLE test (id INT, name VARCHAR(100))"
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)

		oldCols := buildColumnMap(stmt)
		newCols := buildColumnMap(stmt)

		result := canDoColumnChangesInplace(oldCols, newCols)
		assert.True(t, result)
	})

	t.Run("column rename - same position", func(t *testing.T) {
		oldSQL := "CREATE TABLE test (id INT, old_name VARCHAR(100))"
		newSQL := "CREATE TABLE test (id INT, new_name VARCHAR(100))"

		oldStmt, err := parseCreateTableSQL(ctx, oldSQL)
		require.NoError(t, err)
		newStmt, err := parseCreateTableSQL(ctx, newSQL)
		require.NoError(t, err)

		oldCols := buildColumnMap(oldStmt)
		newCols := buildColumnMap(newStmt)

		result := canDoColumnChangesInplace(oldCols, newCols)
		assert.True(t, result)
	})

	t.Run("column added - not inplace", func(t *testing.T) {
		oldSQL := "CREATE TABLE test (id INT)"
		newSQL := "CREATE TABLE test (id INT, name VARCHAR(100))"

		oldStmt, err := parseCreateTableSQL(ctx, oldSQL)
		require.NoError(t, err)
		newStmt, err := parseCreateTableSQL(ctx, newSQL)
		require.NoError(t, err)

		oldCols := buildColumnMap(oldStmt)
		newCols := buildColumnMap(newStmt)

		result := canDoColumnChangesInplace(oldCols, newCols)
		assert.False(t, result)
	})
}

func Test_generateColumnRenameStatements_GoodPath(t *testing.T) {
	ctx := context.Background()

	t.Run("column rename", func(t *testing.T) {
		oldSQL := "CREATE TABLE test (id INT, old_name VARCHAR(100))"
		newSQL := "CREATE TABLE test (id INT, new_name VARCHAR(100))"

		oldStmt, err := parseCreateTableSQL(ctx, oldSQL)
		require.NoError(t, err)
		newStmt, err := parseCreateTableSQL(ctx, newSQL)
		require.NoError(t, err)

		oldCols := buildColumnMap(oldStmt)
		newCols := buildColumnMap(newStmt)

		stmts := generateColumnRenameStatements(ctx, "`db`.`test`", oldCols, newCols)
		assert.Len(t, stmts, 1)
		assert.Contains(t, stmts[0], "RENAME COLUMN")
		assert.Contains(t, stmts[0], "old_name")
		assert.Contains(t, stmts[0], "new_name")
	})

	t.Run("no rename needed", func(t *testing.T) {
		sql := "CREATE TABLE test (id INT, name VARCHAR(100))"
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)

		cols := buildColumnMap(stmt)

		stmts := generateColumnRenameStatements(ctx, "`db`.`test`", cols, cols)
		assert.Len(t, stmts, 0)
	})
}

func Test_generateAddForeignKeyStatement_GoodPath(t *testing.T) {
	t.Run("simple foreign key", func(t *testing.T) {
		fk := &foreignKeyInfo{
			name:       "fk_customer",
			columns:    []string{"customer_id"},
			refTable:   "customers",
			refColumns: []string{"id"},
			onDelete:   "RESTRICT",
			onUpdate:   "RESTRICT",
		}

		stmt := generateAddForeignKeyStatement("`db`.`orders`", "fk_customer", fk)
		assert.Contains(t, stmt, "ADD CONSTRAINT `fk_customer`")
		assert.Contains(t, stmt, "FOREIGN KEY (`customer_id`)")
		assert.Contains(t, stmt, "REFERENCES customers (`id`)")
		// RESTRICT should not be in the output
		assert.NotContains(t, stmt, "ON DELETE")
		assert.NotContains(t, stmt, "ON UPDATE")
	})

	t.Run("foreign key with cascade", func(t *testing.T) {
		fk := &foreignKeyInfo{
			name:       "fk_order",
			columns:    []string{"order_id"},
			refTable:   "orders",
			refColumns: []string{"id"},
			onDelete:   "CASCADE",
			onUpdate:   "SET NULL",
		}

		stmt := generateAddForeignKeyStatement("`db`.`items`", "fk_order", fk)
		assert.Contains(t, stmt, "ON DELETE CASCADE")
		assert.Contains(t, stmt, "ON UPDATE SET NULL")
	})

	t.Run("composite foreign key", func(t *testing.T) {
		fk := &foreignKeyInfo{
			name:       "fk_composite",
			columns:    []string{"col1", "col2"},
			refTable:   "ref_table",
			refColumns: []string{"ref1", "ref2"},
			onDelete:   "RESTRICT",
			onUpdate:   "RESTRICT",
		}

		stmt := generateAddForeignKeyStatement("`db`.`test`", "fk_composite", fk)
		assert.Contains(t, stmt, "FOREIGN KEY (`col1`, `col2`)")
		assert.Contains(t, stmt, "REFERENCES ref_table (`ref1`, `ref2`)")
	})
}

func Test_generateAddIndexStatement_GoodPath(t *testing.T) {
	t.Run("regular index", func(t *testing.T) {
		idx := &indexInfo{
			name:    "idx_name",
			unique:  false,
			columns: []string{"name"},
		}

		stmt := generateAddIndexStatement("`db`.`test`", "idx_name", idx)
		assert.Contains(t, stmt, "ADD INDEX `idx_name`")
		assert.Contains(t, stmt, "(`name`)")
		assert.NotContains(t, stmt, "UNIQUE")
		assert.NotContains(t, stmt, "FULLTEXT")
	})

	t.Run("unique index", func(t *testing.T) {
		idx := &indexInfo{
			name:    "idx_email",
			unique:  true,
			columns: []string{"email"},
		}

		stmt := generateAddIndexStatement("`db`.`test`", "idx_email", idx)
		assert.Contains(t, stmt, "ADD UNIQUE INDEX `idx_email`")
		assert.Contains(t, stmt, "(`email`)")
	})

	t.Run("fulltext index", func(t *testing.T) {
		idx := &indexInfo{
			name:      "idx_content",
			indexType: "FULLTEXT",
			columns:   []string{"content"},
		}

		stmt := generateAddIndexStatement("`db`.`test`", "idx_content", idx)
		assert.Contains(t, stmt, "ADD FULLTEXT INDEX `idx_content`")
		assert.Contains(t, stmt, "(`content`)")
	})

	t.Run("composite index", func(t *testing.T) {
		idx := &indexInfo{
			name:    "idx_composite",
			unique:  false,
			columns: []string{"col1", "col2", "col3"},
		}

		stmt := generateAddIndexStatement("`db`.`test`", "idx_composite", idx)
		assert.Contains(t, stmt, "(`col1`, `col2`, `col3`)")
	})
}

func Test_compareTableDefsAndGenerateAlterStatements_GoodPath(t *testing.T) {
	ctx := context.Background()

	t.Run("no changes", func(t *testing.T) {
		sql := "CREATE TABLE test (id INT, name VARCHAR(100))"
		stmts, canInplace, err := compareTableDefsAndGenerateAlterStatements(ctx, "testdb", "test", sql, sql)
		require.NoError(t, err)
		assert.True(t, canInplace)
		assert.Len(t, stmts, 0)
	})

	t.Run("add index", func(t *testing.T) {
		oldSQL := "CREATE TABLE test (id INT, name VARCHAR(100))"
		newSQL := "CREATE TABLE test (id INT, name VARCHAR(100), INDEX idx_name (name))"

		stmts, canInplace, err := compareTableDefsAndGenerateAlterStatements(ctx, "testdb", "test", oldSQL, newSQL)
		require.NoError(t, err)
		assert.True(t, canInplace)
		assert.Len(t, stmts, 1)
		assert.Contains(t, stmts[0], "ADD INDEX `idx_name`")
	})

	t.Run("drop index", func(t *testing.T) {
		oldSQL := "CREATE TABLE test (id INT, name VARCHAR(100), INDEX idx_name (name))"
		newSQL := "CREATE TABLE test (id INT, name VARCHAR(100))"

		stmts, canInplace, err := compareTableDefsAndGenerateAlterStatements(ctx, "testdb", "test", oldSQL, newSQL)
		require.NoError(t, err)
		assert.True(t, canInplace)
		assert.Len(t, stmts, 1)
		assert.Contains(t, stmts[0], "DROP INDEX `idx_name`")
	})

	t.Run("change comment", func(t *testing.T) {
		oldSQL := "CREATE TABLE test (id INT) COMMENT 'old comment'"
		newSQL := "CREATE TABLE test (id INT) COMMENT 'new comment'"

		stmts, canInplace, err := compareTableDefsAndGenerateAlterStatements(ctx, "testdb", "test", oldSQL, newSQL)
		require.NoError(t, err)
		assert.True(t, canInplace)
		assert.Len(t, stmts, 1)
		assert.Contains(t, stmts[0], "COMMENT 'new comment'")
	})

	t.Run("rename column", func(t *testing.T) {
		oldSQL := "CREATE TABLE test (id INT, old_col VARCHAR(100))"
		newSQL := "CREATE TABLE test (id INT, new_col VARCHAR(100))"

		stmts, canInplace, err := compareTableDefsAndGenerateAlterStatements(ctx, "testdb", "test", oldSQL, newSQL)
		require.NoError(t, err)
		assert.True(t, canInplace)
		assert.Len(t, stmts, 1)
		assert.Contains(t, stmts[0], "RENAME COLUMN")
	})

	t.Run("add foreign key", func(t *testing.T) {
		oldSQL := "CREATE TABLE orders (id INT, customer_id INT)"
		newSQL := "CREATE TABLE orders (id INT, customer_id INT, FOREIGN KEY fk_customer (customer_id) REFERENCES customers(id))"

		stmts, canInplace, err := compareTableDefsAndGenerateAlterStatements(ctx, "testdb", "orders", oldSQL, newSQL)
		require.NoError(t, err)
		assert.True(t, canInplace)
		assert.Len(t, stmts, 1)
		assert.Contains(t, stmts[0], "ADD CONSTRAINT `fk_customer`")
	})

	t.Run("drop foreign key", func(t *testing.T) {
		oldSQL := "CREATE TABLE orders (id INT, customer_id INT, FOREIGN KEY fk_customer (customer_id) REFERENCES customers(id))"
		newSQL := "CREATE TABLE orders (id INT, customer_id INT)"

		stmts, canInplace, err := compareTableDefsAndGenerateAlterStatements(ctx, "testdb", "orders", oldSQL, newSQL)
		require.NoError(t, err)
		assert.True(t, canInplace)
		assert.Len(t, stmts, 1)
		assert.Contains(t, stmts[0], "DROP FOREIGN KEY `fk_customer`")
	})

	t.Run("multiple changes", func(t *testing.T) {
		oldSQL := `CREATE TABLE test (
			id INT,
			old_name VARCHAR(100),
			INDEX idx_old (old_name)
		) COMMENT 'old'`
		newSQL := `CREATE TABLE test (
			id INT,
			new_name VARCHAR(100),
			INDEX idx_new (new_name)
		) COMMENT 'new'`

		stmts, canInplace, err := compareTableDefsAndGenerateAlterStatements(ctx, "testdb", "test", oldSQL, newSQL)
		require.NoError(t, err)
		assert.True(t, canInplace)
		// Should have: rename column, drop old index, add new index, change comment
		assert.GreaterOrEqual(t, len(stmts), 3)

		// Check that statements contain expected operations
		combined := strings.Join(stmts, " ")
		assert.Contains(t, combined, "RENAME COLUMN")
		assert.Contains(t, combined, "DROP INDEX")
		assert.Contains(t, combined, "ADD INDEX")
		assert.Contains(t, combined, "COMMENT")
	})

	t.Run("add column - not inplace", func(t *testing.T) {
		oldSQL := "CREATE TABLE test (id INT)"
		newSQL := "CREATE TABLE test (id INT, name VARCHAR(100))"

		_, canInplace, err := compareTableDefsAndGenerateAlterStatements(ctx, "testdb", "test", oldSQL, newSQL)
		assert.Error(t, err)
		assert.False(t, canInplace)
	})
}

func Test_formatTypeReference_GoodPath(t *testing.T) {
	ctx := context.Background()

	t.Run("int type", func(t *testing.T) {
		sql := "CREATE TABLE test (id INT)"
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)

		cols := buildColumnMap(stmt)
		assert.NotEmpty(t, cols["id"].typ)
	})

	t.Run("varchar type", func(t *testing.T) {
		sql := "CREATE TABLE test (name VARCHAR(100))"
		stmt, err := parseCreateTableSQL(ctx, sql)
		require.NoError(t, err)

		cols := buildColumnMap(stmt)
		assert.Contains(t, cols["name"].typ, "VARCHAR")
	})

	t.Run("nil type", func(t *testing.T) {
		result := formatTypeReference(nil)
		assert.Empty(t, result)
	})
}

func Test_formatTableName_GoodPath(t *testing.T) {
	t.Run("nil table name", func(t *testing.T) {
		result := formatTableName(nil)
		assert.Empty(t, result)
	})
}

func Test_escapeSQLIdentifierForDDL_GoodPath(t *testing.T) {
	t.Run("simple identifier", func(t *testing.T) {
		result := escapeSQLIdentifierForDDL("users")
		assert.Equal(t, "users", result)
	})

	t.Run("identifier with backtick", func(t *testing.T) {
		result := escapeSQLIdentifierForDDL("user`s")
		assert.Equal(t, "user``s", result)
	})

	t.Run("identifier with multiple backticks", func(t *testing.T) {
		result := escapeSQLIdentifierForDDL("a`b`c")
		assert.Equal(t, "a``b``c", result)
	})
}
