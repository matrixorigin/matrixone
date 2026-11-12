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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type testInternalExecutorSession struct{}

func (s *testInternalExecutorSession) GetTempTable(dbName, alias string) (string, bool) {
	return "", false
}

func (s *testInternalExecutorSession) AddTempTable(dbName, alias, realName string) {}

func (s *testInternalExecutorSession) RemoveTempTable(dbName, alias string) {}

func (s *testInternalExecutorSession) RemoveTempTableByRealName(realName string) {}

func (s *testInternalExecutorSession) GetSqlModeNoAutoValueOnZero() (bool, bool) {
	return false, false
}

func TestAttachInternalExecutorSession(t *testing.T) {
	ctx := context.Background()
	ses := &testInternalExecutorSession{}

	attached := attachInternalExecutorSession(ctx, ses)
	if got := getInternalExecutorSession(attached); got != ses {
		t.Fatalf("expected attached session, got %v", got)
	}
}

func TestAttachInternalExecutorSessionWithNil(t *testing.T) {
	ctx := context.Background()

	attached := attachInternalExecutorSession(ctx, nil)
	if got := getInternalExecutorSession(attached); got != nil {
		t.Fatalf("expected nil session, got %v", got)
	}
}

func TestAttachInternalExecutorRewriteOption(t *testing.T) {
	ctx := context.Background()
	option := &tree.RewriteOption{}
	attached := attachInternalExecutorRewriteOption(ctx, option)
	require.Same(t, option, getInternalExecutorRewriteOption(attached))
	require.Nil(t, getInternalExecutorRewriteOption(nil))
	require.Nil(t, getInternalExecutorRewriteOption(ctx))
	require.Nil(t, getInternalExecutorRewriteOption(attachInternalExecutorRewriteOption(ctx, nil)))

	inner := &tree.Select{}
	middle := &tree.Select{Select: &tree.ParenSelect{Select: inner}}
	insert := &tree.Insert{Rows: &tree.Select{Select: &tree.ParenSelect{Select: middle}}}
	attachRewriteOptionToStatement(insert, option)
	require.Same(t, option, insert.Rows.RewriteOption)
	require.Same(t, option, inner.RewriteOption)
	require.Same(t, option, middle.RewriteOption)
	attachRewriteOptionToStatement(insert, nil)
	require.Same(t, option, insert.Rows.RewriteOption)
	for _, stmt := range []tree.Statement{nil, (*tree.Insert)(nil), &tree.Insert{}, &tree.Select{}} {
		attachRewriteOptionToStatement(stmt, option)
	}
}

func TestInternalExecutorDefaultDatabase(t *testing.T) {
	for _, database := range []string{"mapped_db", "", "mo_catalog"} {
		exec := &txnExecutor{ctx: context.Background(), opts: executor.Options{}.WithDatabase(database)}
		require.Equal(t, database, exec.getDatabase())
	}
	exec := &txnExecutor{database: "original_db", opts: executor.Options{}.WithDatabase("fallback_db")}
	require.Equal(t, "original_db", exec.getDatabase())
	exec.database = ""
	require.Equal(t, "fallback_db", exec.getDatabase())
}

func TestAttachInternalExecutorPrivilegeCheck(t *testing.T) {
	ctx := context.Background()

	if needInternalExecutorPrivilegeCheck(ctx) {
		t.Fatalf("expected privilege-check flag off by default")
	}

	ctx = attachInternalExecutorPrivilegeCheck(ctx)
	if !needInternalExecutorPrivilegeCheck(ctx) {
		t.Fatalf("expected privilege-check flag on")
	}
}
