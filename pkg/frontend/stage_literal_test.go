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

package frontend

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestStageCatalogLiteralRoundTrip(t *testing.T) {
	ctx := context.Background()
	name := "stage 'quoted' {space}"
	values := []string{name, "s3://bucket/o'connor/界", `AWS_SECRET_KEY=one'\two=three`, "disabled", "2026-10-29 00:00:00", "qa 'quoted' comment\\\n界"}
	sql, err := getSqlForInsertIntoMoStages(ctx, values[0], values[1], values[2], values[3], values[4], values[5])
	require.NoError(t, err)
	stmt, err := mysql.ParseOne(ctx, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	insert := stmt.(*tree.Insert)
	rows := insert.Rows.Select.(*tree.ValuesClause)
	for i, value := range values {
		require.Equal(t, value, rows.Rows[0][i].(*tree.NumVal).String())
	}
	for _, tc := range []struct{ sql, want string }{
		{getsqlForUpdateStageUrl(name, values[1]), values[1]},
		{getsqlForUpdateStageCredentials(name, values[2]), values[2]},
		{getsqlForUpdateStageStatus(name, values[3]), values[3]},
		{getsqlForUpdateStageComment(name, values[5]), values[5]},
	} {
		parsed, err := mysql.ParseOne(ctx, tc.sql, 1)
		require.NoError(t, err)
		update := parsed.(*tree.Update)
		require.Equal(t, tc.want, update.Exprs[0].Expr.(*tree.NumVal).String())
		require.Equal(t, name, update.Where.Expr.(*tree.ComparisonExpr).Right.(*tree.NumVal).String())
		parsed.Free()
	}
	check, err := getSqlForCheckStage(ctx, name)
	require.NoError(t, err)
	for _, sql := range []string{check, getSqlForDropStage(name)} {
		stmt, err := mysql.ParseOne(ctx, sql, 1)
		require.NoError(t, err)
		stmt.Free()
	}
	for _, name := range []string{"", "bad\x00name", strings.Repeat("a", 65)} {
		_, err := getSqlForCheckStage(ctx, name)
		require.Error(t, err)
	}
}

func TestStageCredentialFormatBoundary(t *testing.T) {
	ctx := context.Background()
	valid := tree.StageCredentials{Exist: true, Credentials: []string{"AWS_SECRET_KEY", "quoted'\\value=ok"}}
	require.NoError(t, validateStageCredentials(ctx, valid))
	require.Equal(t, "AWS_SECRET_KEY=quoted'\\value=ok", formatCredentials(valid))
	for _, entries := range [][]string{{"key", "has,comma"}, {"bad,key", "value"}, {"bad=key", "value"}, {"unpaired"}, {"AWS_SECRET_KEY", "one", "aws_secret_key", "two"}} {
		require.Error(t, validateStageCredentials(ctx, tree.StageCredentials{Exist: true, Credentials: entries}))
	}
}

func TestAlterStageLaterUpdateFailureRollsBack(t *testing.T) {
	for _, badInput := range []bool{false, true} {
		t.Run(fmt.Sprint(badInput), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ses := newSes(&privilege{}, ctrl)
			defer ses.Close()
			ctx := ses.GetTxnHandler().GetTxnCtx()
			bh := &backgroundExecTest{}
			bh.init()
			stub := gostub.StubFunc(&NewBackgroundExec, bh)
			defer stub.Reset()
			credentials := []string{"AWS_SECRET_KEY", "quoted'value=pad"}
			if badInput {
				credentials[1] = "bad,delimiter"
			}
			statement := &tree.AlterStage{
				Name:              "stage_atomic",
				UrlOption:         tree.StageUrl{Exist: true, Url: "file:///new/path/"},
				CredentialsOption: tree.StageCredentials{Exist: true, Credentials: credentials},
				Comment:           tree.StageComment{Exist: true, Comment: "quoted'comment"},
			}
			check, err := getSqlForCheckStage(ctx, string(statement.Name))
			require.NoError(t, err)
			bh.sql2result[check] = newMrsForPasswordOfUser([][]interface{}{{1, "stage_atomic"}})
			expected := fmt.Errorf("second catalog update failed")
			credentialSQL := getsqlForUpdateStageCredentials(string(statement.Name), formatCredentials(statement.CredentialsOption))
			bh.sql2err[credentialSQL] = expected
			err = doAlterStage(ctx, ses, statement)
			if badInput {
				require.Error(t, err)
				require.Empty(t, bh.executedSQLs)
				return
			}
			require.ErrorIs(t, err, expected)
			require.Equal(t, []string{"begin;", check, getsqlForUpdateStageUrl(string(statement.Name), statement.UrlOption.Url), credentialSQL, "rollback;"}, bh.executedSQLs)
		})
	}
}

func TestStageIdentifiersHaveOneCaseInsensitiveNamespace(t *testing.T) {
	for _, lower := range []int64{0, 1} {
		for _, sql := range []string{"create stage `ÉCOLE%2F` url='file:///tmp/'", "alter stage `ÉCOLE%2F` set comment='value'", "drop stage `ÉCOLE%2F`"} {
			stmt, err := mysql.ParseOne(context.Background(), sql, lower)
			require.NoError(t, err)
			var name tree.Identifier
			switch value := stmt.(type) {
			case *tree.CreateStage:
				name = value.Name
			case *tree.AlterStage:
				name = value.Name
			case *tree.DropStage:
				name = value.Name
			}
			require.Equal(t, "école%2f", string(name))
			stmt.Free()
		}
	}
}
