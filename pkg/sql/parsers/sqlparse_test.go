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

package parsers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/postgresql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	debugSQL = struct {
		input  string
		output string
	}{
		input: "use db1",
	}
)

func TestMysql(t *testing.T) {
	ctx := context.TODO()
	if debugSQL.output == "" {
		debugSQL.output = debugSQL.input
	}
	ast, err := mysql.ParseOne(ctx, debugSQL.input, 1)
	if err != nil {
		t.Errorf("Parse(%q) err: %v", debugSQL.input, err)
		return
	}
	out := tree.String(ast, dialect.MYSQL)
	if debugSQL.output != out {
		t.Errorf("Parsing failed. \nExpected/Got:\n%s\n%s", debugSQL.output, out)
	}
}

func TestPostgresql(t *testing.T) {
	ctx := context.TODO()
	if debugSQL.output == "" {
		debugSQL.output = debugSQL.input
	}
	ast, err := postgresql.ParseOne(ctx, debugSQL.input)
	if err != nil {
		t.Errorf("Parse(%q) err: %v", debugSQL.input, err)
		return
	}
	out := tree.String(ast, dialect.POSTGRESQL)
	if debugSQL.output != out {
		t.Errorf("Parsing failed. \nExpected/Got:\n%s\n%s", debugSQL.output, out)
	}
}

func TestSplitSqlBySemicolon(t *testing.T) {
	ret := SplitSqlBySemicolon("select 1;select 2;select 3;")
	require.Equal(t, 3, len(ret))
	require.Equal(t, "select 1", ret[0])
	require.Equal(t, "select 2", ret[1])
	require.Equal(t, "select 3", ret[2])

	ret = SplitSqlBySemicolon("select 1;select 2/*;;;*/;select 3;")
	require.Equal(t, 3, len(ret))
	require.Equal(t, "select 1", ret[0])
	require.Equal(t, "select 2/*;;;*/", ret[1])
	require.Equal(t, "select 3", ret[2])

	ret = SplitSqlBySemicolon("select 1;select \"2;;\";select 3;")
	require.Equal(t, 3, len(ret))
	require.Equal(t, "select 1", ret[0])
	require.Equal(t, "select \"2;;\"", ret[1])
	require.Equal(t, "select 3", ret[2])

	ret = SplitSqlBySemicolon("select 1;select '2;;';select 3;")
	require.Equal(t, 3, len(ret))
	require.Equal(t, "select 1", ret[0])
	require.Equal(t, "select '2;;'", ret[1])
	require.Equal(t, "select 3", ret[2])

	ret = SplitSqlBySemicolon("select 1;select '2;;';select 3")
	require.Equal(t, 3, len(ret))
	require.Equal(t, "select 1", ret[0])
	require.Equal(t, "select '2;;'", ret[1])
	require.Equal(t, "select 3", ret[2])

	ret = SplitSqlBySemicolon("select 1")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "select 1", ret[0])

	ret = SplitSqlBySemicolon(";;;")
	require.Equal(t, 3, len(ret))
	require.Equal(t, "", ret[0])
	require.Equal(t, "", ret[1])
	require.Equal(t, "", ret[2])

	ret = SplitSqlBySemicolon(";;;  ")
	require.Equal(t, 3, len(ret))
	require.Equal(t, "", ret[0])
	require.Equal(t, "", ret[1])
	require.Equal(t, "", ret[2])

	ret = SplitSqlBySemicolon(";")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "", ret[0])

	ret = SplitSqlBySemicolon("")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "", ret[0])

	ret = SplitSqlBySemicolon("   ;   ")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "", ret[0])

	ret = SplitSqlBySemicolon("   ")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "", ret[0])

	ret = SplitSqlBySemicolon("  ; /* abc */ ")
	require.Equal(t, 2, len(ret))
	require.Equal(t, "", ret[0])
	require.Equal(t, "/* abc */", ret[1])

	ret = SplitSqlBySemicolon(" /* cde */  ; /* abc */ ")
	require.Equal(t, 2, len(ret))
	require.Equal(t, "/* cde */", ret[0])
	require.Equal(t, "/* abc */", ret[1])

	ret = SplitSqlBySemicolon("   ;    ;  ")
	require.Equal(t, 2, len(ret))
	require.Equal(t, "", ret[0])
	require.Equal(t, "", ret[1])

	ret = SplitSqlBySemicolon("   ;    ;")
	require.Equal(t, 2, len(ret))
	require.Equal(t, "", ret[0])
	require.Equal(t, "", ret[1])

	ret = SplitSqlBySemicolon("   ;   ")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "", ret[0])
}

func TestHandleSqlForRecord(t *testing.T) {
	// Test remove /* cloud_user */ prefix
	var ret []string
	ret = HandleSqlForRecord(" ;   ;  ")
	require.Equal(t, 2, len(ret))
	require.Equal(t, "", ret[0])
	require.Equal(t, "", ret[1])

	ret = HandleSqlForRecord(" ; /* abc */  ")
	require.Equal(t, 2, len(ret))
	require.Equal(t, "", ret[0])
	require.Equal(t, "/* abc */", ret[1])

	ret = HandleSqlForRecord(" /* cde */  ; /* abc */ ")
	require.Equal(t, 2, len(ret))
	require.Equal(t, "/* cde */", ret[0])
	require.Equal(t, "/* abc */", ret[1])

	ret = HandleSqlForRecord(" /* cde */  ; /* abc */ ; " + stripCloudNonUser + " ; " + stripCloudUser)
	require.Equal(t, 4, len(ret))
	require.Equal(t, "/* cde */", ret[0])
	require.Equal(t, "/* abc */", ret[1])
	require.Equal(t, "", ret[2])
	require.Equal(t, "", ret[3])

	ret = HandleSqlForRecord("  /* cloud_user */ select 1;   ")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "select 1", ret[0])

	ret = HandleSqlForRecord("  /* cloud_user */ select 1;  ")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "select 1", ret[0])

	ret = HandleSqlForRecord("  /* cloud_user */select * from t;/* cloud_user */select * from t;/* cloud_user */select * from t;")
	require.Equal(t, 3, len(ret))
	require.Equal(t, "select * from t", ret[0])
	require.Equal(t, "select * from t", ret[1])
	require.Equal(t, "select * from t", ret[2])

	ret = HandleSqlForRecord("  /* cloud_user */  select * from t ;  /* cloud_user */  select * from t ; /* cloud_user */ select * from t ; ")
	require.Equal(t, 3, len(ret))
	require.Equal(t, "select * from t", ret[0])
	require.Equal(t, "select * from t", ret[1])
	require.Equal(t, "select * from t", ret[2])

	ret = HandleSqlForRecord("  /* cloud_user */  select * from t ;  /* cloud_user */  select * from t ; /* cloud_user */ select * from t ; /* abc */ ")
	require.Equal(t, 4, len(ret))
	require.Equal(t, "select * from t", ret[0])
	require.Equal(t, "select * from t", ret[1])
	require.Equal(t, "select * from t", ret[2])
	require.Equal(t, "/* abc */", ret[3])

	ret = HandleSqlForRecord("  /* cloud_user */  ")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "", ret[0])

	ret = HandleSqlForRecord("  /* cloud_user */   ")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "", ret[0])

	ret = HandleSqlForRecord("   " + stripCloudNonUser + "  select 1;   ")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "select 1", ret[0])

	ret = HandleSqlForRecord("  " + stripCloudNonUser + "  select * from t  ;  " + stripCloudNonUser + "   select * from t  ;   " + stripCloudNonUser + "   select * from t  ;   ")
	require.Equal(t, 3, len(ret))
	require.Equal(t, "select * from t", ret[0])
	require.Equal(t, "select * from t", ret[1])
	require.Equal(t, "select * from t", ret[2])

	ret = HandleSqlForRecord("  " + stripCloudNonUser + "  select * from t  ;  " + stripCloudNonUser + "   select * from t  ;   " + stripCloudNonUser + "   select * from t  ; /* abc */  ")
	require.Equal(t, 4, len(ret))
	require.Equal(t, "select * from t", ret[0])
	require.Equal(t, "select * from t", ret[1])
	require.Equal(t, "select * from t", ret[2])
	require.Equal(t, "/* abc */", ret[3])

	ret = HandleSqlForRecord("   " + stripCloudNonUser + "  ")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "", ret[0])

	ret = HandleSqlForRecord("   " + stripCloudUser + "  ")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "", ret[0])

	ret = HandleSqlForRecord("")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "", ret[0])

	// Test hide secret key

	ret = HandleSqlForRecord("create user u identified by '123456';")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "create user u identified by '******'", ret[0])

	ret = HandleSqlForRecord("create user u identified with '12345';")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "create user u identified with '******'", ret[0])

	ret = HandleSqlForRecord("create user u identified by random password;")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "create user u identified by random password", ret[0])

	ret = HandleSqlForRecord("create user if not exists abc1 identified by '123', abc2 identified by '234', abc3 identified with '111', abc3 identified by random password;")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "create user if not exists abc1 identified by '******', abc2 identified by '******', abc3 identified with '******', abc3 identified by random password", ret[0])

	ret = HandleSqlForRecord("create external table t (a int) URL s3option{'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='123', 'secret_access_key'='123', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'};")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "create external table t (a int) URL s3option{'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='******', 'secret_access_key'='******', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'}", ret[0])

	ret = HandleSqlForRecord("/* cloud_user *//* save_result */select count(*) from a;")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "select count(*) from a", ret[0])

	ret = HandleSqlForRecord("/* cloud_user    *//* save_result    */select count(*) from a;")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "select count(*) from a", ret[0])

	ret = HandleSqlForRecord("/* cloud_user    *//* save_result    */ /*abc */select count(*) from a;")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "/*abc */select count(*) from a", ret[0])

	ret = HandleSqlForRecord("/* cloud_user    *//* save_result    */ /*abc */select count(*) from a // def;")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "/*abc */select count(*) from a // def;", ret[0])
}

func TestExtractLeadingHints(t *testing.T) {
	// Case 1: Provided multi-line optimizer hint with smart quotes (not JSON-parseable)
	sql1 := `/*+ { “rewrites” : {
“t1”: “select a, b, c from t1 where a = 100”,
“T2”: “select x, avg(y) from othert group by x”
}
} */

Select * from t1, t2 where t1.a = t2.x`

	got := extractLeadingHints(sql1)
	require.Equal(t, 1, len(got))
	expected1 := ` { “rewrites” : {
“t1”: “select a, b, c from t1 where a = 100”,
“T2”: “select x, avg(y) from othert group by x”
}
} `
	require.Equal(t, expected1, got[0])

	// Case 2: Valid JSON hint, verify JSON parsability
	sql2 := `/*+ {"rewrites": {"t1": "select 1", "T2": "select 2"}} */ select 1;`
	got = extractLeadingHints(sql2)
	require.Equal(t, 1, len(got))
	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(got[0]), &payload))
	inner, ok := payload["rewrites"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "select 1", inner["t1"])
	require.Equal(t, "select 2", inner["T2"])

	// Case 3: Consecutive hints and multi-statements alignment
	sql3 := `/*+ session=on */ /*++trace=on*/ /*+ use_nl(t1) */ select 1; /*+ h2 */ select 2; select 3;`
	got = extractLeadingHints(sql3)
	require.Equal(t, 3, len(got))
	require.Equal(t, ` session=on `, got[0])
	require.Equal(t, ` h2 `, got[1])
	require.Equal(t, "", got[2])

	// Case 4: Empty input returns one empty hint
	got = extractLeadingHints("")
	require.Equal(t, 1, len(got))
	require.Equal(t, "", got[0])

	// Case 5: Multi statements without hints
	got = extractLeadingHints("select 1; select 2;")
	require.Equal(t, 2, len(got))
	require.Equal(t, "", got[0])
	require.Equal(t, "", got[1])

	// Case 6: Unterminated hint collects inner content till EOF
	got = extractLeadingHints("/*+ abc")
	require.Equal(t, 1, len(got))
	require.Equal(t, " abc", got[0])
}

// helper to parse and then apply AddRewriteHints
func parseAndApply(t *testing.T, sql string) ([]tree.Statement, error) {
	t.Helper()
	ctx := context.TODO()
	stmts, err := Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	err = AddRewriteHints(ctx, stmts, sql)
	return stmts, err
}

func TestAddRewriteHints_ValidSimple(t *testing.T) {
	sql := "/*+ {\"rewrites\": {\"db1.t1\": \"select 1\"}} */ select * from db1.t1"
	stmts, err := parseAndApply(t, sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	sel, ok := stmts[0].(*tree.Select)
	require.True(t, ok)
	require.NotNil(t, sel.RewriteOption)
	require.Contains(t, sel.RewriteOption.Rewrites, "db1.t1")
	chain := sel.RewriteOption.Rewrites["db1.t1"]
	require.Len(t, chain, 1)
	r := chain[0]
	require.Equal(t, "t1", r.TableName)
	require.Equal(t, "db1", r.DbName)
	switch r.Stmt.(type) {
	case *tree.Select, *tree.ParenSelect:
		// ok
	default:
		t.Fatalf("unexpected rewrite stmt type: %T", r.Stmt)
	}
}

func TestAddRewriteHints_ValidWithBangPlusComment(t *testing.T) {
	sql := "/*+ {\"rewrites\": {\"db2.t2\": \"(select 1)\"}} */ select 2"
	stmts, err := parseAndApply(t, sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	sel, ok := stmts[0].(*tree.Select)
	require.True(t, ok)
	require.NotNil(t, sel.RewriteOption)
	require.Contains(t, sel.RewriteOption.Rewrites, "db2.t2")
	chain := sel.RewriteOption.Rewrites["db2.t2"]
	require.Len(t, chain, 1)
	r := chain[0]
	require.Equal(t, "t2", r.TableName)
	require.Equal(t, "db2", r.DbName)
	switch r.Stmt.(type) {
	case *tree.Select, *tree.ParenSelect:
	default:
		t.Fatalf("unexpected rewrite stmt type: %T", r.Stmt)
	}
}

func TestAddRewriteHints_RemapDb(t *testing.T) {
	t.Run("valid multi", func(t *testing.T) {
		stmts, err := parseAndApply(t, `/*+ {"remapdb": {"a": "b", "x": "y"}} */ select * from a.t`)
		require.NoError(t, err)
		sel, ok := stmts[0].(*tree.Select)
		require.True(t, ok)
		require.NotNil(t, sel.RewriteOption)
		require.Equal(t, map[string]string{"a": "b", "x": "y"}, sel.RewriteOption.RemapDb)
	})
	t.Run("invalid identifier", func(t *testing.T) {
		_, err := parseAndApply(t, `/*+ {"remapdb": {"a.b": "c"}} */ select 1`)
		require.ErrorContains(t, err, "valid identifiers")
	})
	t.Run("chaining rejected", func(t *testing.T) {
		_, err := parseAndApply(t, `/*+ {"remapdb": {"x": "y", "y": "z"}} */ select 1`)
		require.ErrorContains(t, err, "must not be both a source and a destination")
	})
	t.Run("self map rejected", func(t *testing.T) {
		_, err := parseAndApply(t, `/*+ {"remapdb": {"x": "x"}} */ select 1`)
		require.ErrorContains(t, err, "must not be both a source and a destination")
	})
	t.Run("system database source rejected", func(t *testing.T) {
		for _, src := range []string{"mysql", "information_schema", "system", "system_metrics", "mo_catalog", "mo_anything"} {
			_, err := parseAndApply(t, `/*+ {"remapdb": {"`+src+`": "y"}} */ select 1`)
			require.ErrorContains(t, err, "must not remap a system database", "src=%s", src)
		}
	})
	t.Run("system database destination rejected", func(t *testing.T) {
		_, err := parseAndApply(t, `/*+ {"remapdb": {"x": "mo_catalog"}} */ select 1`)
		require.ErrorContains(t, err, "must not remap a system database")
	})
}

func TestIsSystemDatabase(t *testing.T) {
	for _, n := range []string{"mysql", "MySQL", "information_schema", "system", "system_metrics", "mo_catalog", "mo_task", "mo_foo", "MO_BAR"} {
		require.True(t, IsSystemDatabase(n), n)
	}
	for _, n := range []string{"db1", "users", "moose", "system2", "mo", "mox"} {
		require.False(t, IsSystemDatabase(n), n)
	}
}

func TestAddRewriteHints_ChainArray(t *testing.T) {
	sql := `/*+ {"rewrites": {"db1.t1": ["select * from db1.t1 where a < 4", "select * from db1.t1 where a > 1"]}} */ select * from db1.t1`
	stmts, err := parseAndApply(t, sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	sel, ok := stmts[0].(*tree.Select)
	require.True(t, ok)
	require.NotNil(t, sel.RewriteOption)
	chain := sel.RewriteOption.Rewrites["db1.t1"]
	require.Len(t, chain, 2)
	for _, r := range chain {
		require.Equal(t, "t1", r.TableName)
		require.Equal(t, "db1", r.DbName)
		switch r.Stmt.(type) {
		case *tree.Select, *tree.ParenSelect:
		default:
			t.Fatalf("unexpected rewrite stmt type: %T", r.Stmt)
		}
	}
}

func TestAddRewriteHints_IgnoresNonSelectStatements(t *testing.T) {
	sql := "/*+ {\"rewrites\": {\"db3.t3\": \"select 1\"}} */ insert into db3.t3 values (1)"
	stmts, err := parseAndApply(t, sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	// Should be ignored; no panic or error and no rewrite option
	_, isSelect := stmts[0].(*tree.Select)
	if isSelect {
		sel := stmts[0].(*tree.Select)
		require.Nil(t, sel.RewriteOption)
	}
}

func TestAddRewriteHints_NoJsonObjectHint_IsIgnored(t *testing.T) {
	sql := "/*+ index(t idx) */ select 1"
	stmts, err := parseAndApply(t, sql)
	require.NoError(t, err)
	sel := stmts[0].(*tree.Select)
	require.Nil(t, sel.RewriteOption)
}

func TestAddRewriteHints_InvalidJson_Err(t *testing.T) {
	sql := "/*+ { not_valid_json } */ select 1"
	_, err := parseAndApply(t, sql)
	require.Error(t, err)
}

func TestAddRewriteHints_KeyWithoutDB_Err(t *testing.T) {
	sql := "/*+ {\"rewrites\": {\"tonly\": \"select 1\"}} */ select 1"
	_, err := parseAndApply(t, sql)
	require.ErrorContains(t, err, "include database name")
}

func TestAddRewriteHints_EmptyDBOrTable_Err(t *testing.T) {
	cases := []string{
		"/*+ {\"rewrites\": {\"db1.\": \"select 1\"}} */ select 1",
		"/*+ {\"rewrites\": {\".t1\": \"select 1\"}} */ select 1",
	}
	for _, sql := range cases {
		_, err := parseAndApply(t, sql)
		require.ErrorContains(t, err, "empty table or database")
	}
}

func TestAddRewriteHints_EmptyValue_Err(t *testing.T) {
	sql := "/*+ {\"rewrites\": {\"db1.t1\": \"\"}} */ select 1"
	_, err := parseAndApply(t, sql)
	require.ErrorContains(t, err, "statement")
}

func TestAddRewriteHints_ValueParseError_Err(t *testing.T) {
	sql := "/*+ {\"rewrites\": {\"db1.t1\": \"select from\"}} */ select 1"
	_, err := parseAndApply(t, sql)
	require.Error(t, err)
}

func TestAddRewriteHints_ValueNotSelect_Err(t *testing.T) {
	sql := "/*+ {\"rewrites\": {\"db1.t1\": \"create table x(a int)\"}} */ select 1"
	_, err := parseAndApply(t, sql)
	require.ErrorContains(t, err, "only accept SELECT-like")
}

func TestAddRewriteHints_MultipleStatements_FirstHasHint(t *testing.T) {
	sql := "/*+ {\"rewrites\": {\"db1.t1\": \"select 1\"}} */ select * from db1.t1; select 2"
	stmts, err := parseAndApply(t, sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	sel1 := stmts[0].(*tree.Select)
	require.NotNil(t, sel1.RewriteOption)
	sel2 := stmts[1].(*tree.Select)
	require.Nil(t, sel2.RewriteOption)
}

func TestAddRewriteHints_ParenSelectTopLevel(t *testing.T) {
	sql := "/*+ {\"rewrites\": {\"db1.t1\": \"select 1\"}} */ (select 2)"
	stmts, err := parseAndApply(t, sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	ps, ok := stmts[0].(*tree.ParenSelect)
	if !ok {
		// Some grammars still return *tree.Select with inner Paren; accept either
		sel, ok2 := stmts[0].(*tree.Select)
		require.True(t, ok2)
		require.NotNil(t, sel.RewriteOption)
		return
	}
	require.NotNil(t, ps.Select)
	require.NotNil(t, ps.Select.RewriteOption)
}

func TestAddRewriteHints_TrailingLineComment(t *testing.T) {
	// A trailing "stmt; -- comment" splits into two fragments but parses into a
	// single statement. The comment-only tail must not be counted, otherwise the
	// hint/statement counts mismatch and this valid query is rejected.
	t.Run("select with trailing line comment", func(t *testing.T) {
		stmts, err := parseAndApply(t, "select 1; -- a trailing comment")
		require.NoError(t, err)
		require.Len(t, stmts, 1)
	})

	t.Run("use with trailing line comment", func(t *testing.T) {
		stmts, err := parseAndApply(t, "use db1; -- USE is not remapped")
		require.NoError(t, err)
		require.Len(t, stmts, 1)
	})

	t.Run("trailing block comment", func(t *testing.T) {
		stmts, err := parseAndApply(t, "select 1; /* trailing block */")
		require.NoError(t, err)
		require.Len(t, stmts, 1)
	})

	t.Run("hint survives with trailing comment", func(t *testing.T) {
		stmts, err := parseAndApply(t, "/*+ {\"rewrites\": {\"db1.t1\": \"select 1\"}} */ select * from db1.t1; -- tail")
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		sel := stmts[0].(*tree.Select)
		require.NotNil(t, sel.RewriteOption)
		require.Contains(t, sel.RewriteOption.Rewrites, "db1.t1")
	})

	t.Run("comment-only input is accepted", func(t *testing.T) {
		stmts, err := parseAndApply(t, "-- just a comment")
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		_, ok := stmts[0].(*tree.EmptyStmt)
		require.True(t, ok)
	})
}

func TestAddRewriteHints_ParseHintsBug_MismatchedInputs(t *testing.T) {
	ctx := context.TODO()
	// Build stmts from two statements
	stmts, err := Parse(ctx, dialect.MYSQL, "select 1; select 2", 1)
	require.NoError(t, err)
	// Provide SQL with only one statement to trigger mismatch
	err = AddRewriteHints(ctx, stmts, "/*+ {\\\"rewrites\\\": {\\\"db1.t1\\\": \\\"select 1\\\"}} */ select 1")
	require.ErrorContains(t, err, "parse hints bug")
}
