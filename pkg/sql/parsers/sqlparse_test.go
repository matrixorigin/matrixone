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
	"github.com/stretchr/testify/require"
	"testing"

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
	ret1 := SplitSqlBySemicolon("select 1;select 2;select 3;")
	require.Equal(t, 3, len(ret1))
	require.Equal(t, "select 1", ret1[0])
	require.Equal(t, "select 2", ret1[1])
	require.Equal(t, "select 3", ret1[2])

	ret2 := SplitSqlBySemicolon("select 1;select 2/*;;;*/;select 3;")
	require.Equal(t, 3, len(ret2))
	require.Equal(t, "select 1", ret2[0])
	require.Equal(t, "select 2/*;;;*/", ret2[1])
	require.Equal(t, "select 3", ret2[2])

	ret3 := SplitSqlBySemicolon("select 1;select \"2;;\";select 3;")
	require.Equal(t, 3, len(ret3))
	require.Equal(t, "select 1", ret3[0])
	require.Equal(t, "select \"2;;\"", ret3[1])
	require.Equal(t, "select 3", ret3[2])

	ret4 := SplitSqlBySemicolon("select 1;select '2;;';select 3;")
	require.Equal(t, 3, len(ret4))
	require.Equal(t, "select 1", ret4[0])
	require.Equal(t, "select '2;;'", ret4[1])
	require.Equal(t, "select 3", ret4[2])

	ret5 := SplitSqlBySemicolon("select 1;select '2;;';select 3")
	require.Equal(t, 3, len(ret5))
	require.Equal(t, "select 1", ret5[0])
	require.Equal(t, "select '2;;'", ret5[1])
	require.Equal(t, "select 3", ret5[2])

	ret6 := SplitSqlBySemicolon("abc")
	require.Equal(t, 1, len(ret6))
	require.Equal(t, "abc", ret6[0])
}
