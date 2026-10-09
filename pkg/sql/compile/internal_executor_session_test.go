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
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
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

func TestAttachInternalExecutorCompilerContext(t *testing.T) {
	ctx := context.Background()
	compilerContext := &plan.MockCompilerContext{}
	attached := attachInternalExecutorCompilerContext(ctx, compilerContext)
	if got := getInternalExecutorCompilerContext(attached); got != compilerContext {
		t.Fatalf("expected attached compiler context, got %v", got)
	}
	if got := getInternalExecutorCompilerContext(
		attachInternalExecutorCompilerContext(ctx, nil)); got != nil {
		t.Fatalf("expected nil compiler context, got %v", got)
	}
}

func TestAttachInternalExecutorRewriteOption(t *testing.T) {
	ctx := context.Background()
	option := &tree.RewriteOption{}
	attached := attachInternalExecutorRewriteOption(ctx, option)
	if got := getInternalExecutorRewriteOption(attached); got != option {
		t.Fatalf("expected attached rewrite option, got %p", got)
	}
	if got := getInternalExecutorRewriteOption(nil); got != nil {
		t.Fatalf("expected nil rewrite option for nil context, got %p", got)
	}
	if got := getInternalExecutorRewriteOption(
		attachInternalExecutorRewriteOption(ctx, nil)); got != nil {
		t.Fatalf("expected nil rewrite option after nil attach, got %p", got)
	}

	inner := &tree.Select{}
	insert := &tree.Insert{Rows: &tree.Select{
		Select: &tree.ParenSelect{Select: inner},
	}}
	attachRewriteOptionToStatement(insert, option)
	if insert.Rows.RewriteOption != option || inner.RewriteOption != option {
		t.Fatal("expected CTAS INSERT source and parenthesized source to share rewrite option")
	}
	attachRewriteOptionToStatement(&tree.Select{}, option)
	attachRewriteOptionToStatement(insert, nil)
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
