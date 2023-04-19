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

	ret = SplitSqlBySemicolon(";")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "", ret[0])
}

func TestHandleSqlForRecord(t *testing.T) {
	// Test remove /* cloud_user */ prefix

	ret := HandleSqlForRecord("/* cloud_user */select 1;")
	require.Equal(t, 1, len(ret))
	require.Equal(t, "select 1", ret[0])

	ret = HandleSqlForRecord("/* cloud_user */select * from t;/* cloud_user */select * from t;/* cloud_user */select * from t;")
	require.Equal(t, 3, len(ret))
	require.Equal(t, "select * from t", ret[0])
	require.Equal(t, "select * from t", ret[1])
	require.Equal(t, "select * from t", ret[2])

	ret = HandleSqlForRecord("/* cloud_user */")
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
}
