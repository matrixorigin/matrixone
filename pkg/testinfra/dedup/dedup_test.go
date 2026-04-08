// Copyright 2024 Matrix Origin
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

package dedup

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeSQL(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "basic select",
			input: "SELECT * FROM t1 WHERE id = 42;",
			want:  "select * from t1 where id = ?",
		},
		{
			name:  "string literals",
			input: "INSERT INTO t1 VALUES ('hello', 'world');",
			want:  "insert into t1 values (?, ?)",
		},
		{
			name:  "comments removed",
			input: "SELECT * FROM t1; -- this is a comment",
			want:  "select * from t1",
		},
		{
			name:  "multi-line comment",
			input: "SELECT /* inline */ * FROM t1;",
			want:  "select * from t1",
		},
		{
			name:  "whitespace collapsed",
			input: "SELECT * FROM t1\n WHERE id = 1;",
			want:  "select * from t1 where id = ?",
		},
		{
			name:  "case insensitive",
			input: "SELECT * FROM T1 WHERE Name = 'Alice';",
			want:  "select * from t1 where name = ?",
		},
		{
			name:  "decimal numbers",
			input: "SELECT * FROM t1 WHERE val > 3.14;",
			want:  "select * from t1 where val > ?",
		},
		{
			name:  "pure numbers",
			input: "SELECT 42 FROM dual;",
			want:  "select ? from dual",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeSQL(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFingerprint(t *testing.T) {
	f := NewSQLFingerprinter()

	// Same logical query should produce same fingerprint
	fp1 := f.Fingerprint("SELECT * FROM t1 WHERE id = 42;")
	fp2 := f.Fingerprint("select * from t1 where id = 100;")
	assert.Equal(t, fp1, fp2)

	// Different queries should produce different fingerprints
	fp3 := f.Fingerprint("INSERT INTO t1 VALUES (1, 2);")
	assert.NotEqual(t, fp1, fp3)
}

func TestDedup(t *testing.T) {
	f := NewSQLFingerprinter()
	sqls := []string{
		"SELECT * FROM t1 WHERE id = 1;",
		"SELECT * FROM t1 WHERE id = 2;", // duplicate of first
		"INSERT INTO t1 VALUES (1, 'a');",
		"INSERT INTO t1 VALUES (2, 'b');", // duplicate of third
		"DELETE FROM t1 WHERE id = 1;",    // unique
	}

	result := f.Dedup(sqls)

	assert.Len(t, result.Unique, 1) // only DELETE is unique
	assert.Contains(t, result.Unique[0], "DELETE")
	assert.Len(t, result.Duplicates, 2) // SELECT group and INSERT group

	// Check that duplicate groups have 2 items each and contain the right statements
	for _, group := range result.Duplicates {
		assert.Len(t, group, 2)
	}
	// Verify specific groupings via fingerprint
	selectFP := f.Fingerprint(sqls[0])
	insertFP := f.Fingerprint(sqls[2])
	assert.Equal(t, selectFP, f.Fingerprint(sqls[1]), "sqls[0] and sqls[1] should share fingerprint")
	assert.Equal(t, insertFP, f.Fingerprint(sqls[3]), "sqls[2] and sqls[3] should share fingerprint")
	assert.NotEqual(t, selectFP, insertFP, "SELECT and INSERT fingerprints should differ")
}

func TestDedupAllUnique(t *testing.T) {
	f := NewSQLFingerprinter()
	sqls := []string{
		"SELECT * FROM t1;",
		"INSERT INTO t1 VALUES (1);",
		"DELETE FROM t1 WHERE id = 1;",
	}

	result := f.Dedup(sqls)
	assert.Len(t, result.Unique, 3)
	assert.Empty(t, result.Duplicates)
}

func TestExtractSQLStatements(t *testing.T) {
	content := `-- @bvt:issue#12345
-- This is a comment
CREATE TABLE t1 (a INT, b VARCHAR(100));
INSERT INTO t1 VALUES (1, 'hello');
INSERT INTO t1 VALUES (2, 'world');

-- @sortkey:0
SELECT * FROM t1
WHERE a > 0
ORDER BY a;

DROP TABLE t1;
`

	stmts := ExtractSQLStatements(content)
	require.Len(t, stmts, 5)
	assert.Contains(t, stmts[0], "CREATE TABLE")
	assert.Contains(t, stmts[1], "INSERT INTO")
	assert.Contains(t, stmts[2], "INSERT INTO")
	assert.Contains(t, stmts[3], "SELECT * FROM t1")
	// Multi-line SELECT should be joined
	assert.Contains(t, stmts[3], "WHERE a > 0")
	assert.Contains(t, stmts[4], "DROP TABLE")
}

func TestExtractSQLStatementsEmpty(t *testing.T) {
	stmts := ExtractSQLStatements("")
	assert.Empty(t, stmts)
}

func TestExtractSQLStatementsTagsOnly(t *testing.T) {
	content := `-- @bvt:issue#999
-- @skip:issue#888
-- Just comments
`
	stmts := ExtractSQLStatements(content)
	assert.Empty(t, stmts)
}

func TestExtractSQLStatementsNoSemicolon(t *testing.T) {
	content := `SELECT 1`
	stmts := ExtractSQLStatements(content)
	require.Len(t, stmts, 1)
	assert.Equal(t, "SELECT 1", stmts[0])
}
