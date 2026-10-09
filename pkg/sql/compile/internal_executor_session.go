// Copyright 2025 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type internalExecutorSessionKey struct{}
type internalExecutorCompilerContextKey struct{}
type internalExecutorRewriteOptionKey struct{}

// internalExecutorPrivilegeCheckKey toggles internal SQL auth behavior:
// absent -> bypass as internal executor; present -> run normal auth checks.
type internalExecutorPrivilegeCheckKey struct{}

func attachInternalExecutorSession(ctx context.Context, ses process.Session) context.Context {
	if ses == nil {
		return ctx
	}
	return context.WithValue(ctx, internalExecutorSessionKey{}, ses)
}

// AttachInternalExecutorSession attaches the original frontend session to
// internal SQL so temp-table aliases and other session-scoped metadata resolve
// the same way they do for the user's statement.
func AttachInternalExecutorSession(ctx context.Context, ses process.Session) context.Context {
	return attachInternalExecutorSession(ctx, ses)
}

func getInternalExecutorSession(ctx context.Context) process.Session {
	if ctx == nil {
		return nil
	}
	if v := ctx.Value(internalExecutorSessionKey{}); v != nil {
		if ses, ok := v.(process.Session); ok {
			return ses
		}
	}
	return nil
}

func attachInternalExecutorCompilerContext(ctx context.Context, compilerContext plan.CompilerContext) context.Context {
	if compilerContext == nil {
		return ctx
	}
	return context.WithValue(ctx, internalExecutorCompilerContextKey{}, compilerContext)
}

func getInternalExecutorCompilerContext(ctx context.Context) plan.CompilerContext {
	if ctx == nil {
		return nil
	}
	compilerContext, _ := ctx.Value(internalExecutorCompilerContextKey{}).(plan.CompilerContext)
	return compilerContext
}

// attachInternalExecutorRewriteOption carries the effective, already parsed
// rewrite policy across an internal SQL boundary. The policy is semantic AST
// state owned by the caller; it must not be reconstructed from diagnostic SQL.
func attachInternalExecutorRewriteOption(ctx context.Context, option *tree.RewriteOption) context.Context {
	if option == nil {
		return ctx
	}
	return context.WithValue(ctx, internalExecutorRewriteOptionKey{}, option)
}

func getInternalExecutorRewriteOption(ctx context.Context) *tree.RewriteOption {
	if ctx == nil {
		return nil
	}
	option, _ := ctx.Value(internalExecutorRewriteOptionKey{}).(*tree.RewriteOption)
	return option
}

// attachRewriteOptionToStatement installs one effective policy on the source
// query that will be planned. CTAS generates an INSERT whose Rows wrapper may
// contain a parenthesized SELECT, matching the parser's normal hint placement.
func attachRewriteOptionToStatement(stmt tree.Statement, option *tree.RewriteOption) {
	if stmt == nil || option == nil {
		return
	}
	attachToSelect := func(sel *tree.Select) {
		if sel == nil {
			return
		}
		sel.RewriteOption = option
		if paren, ok := sel.Select.(*tree.ParenSelect); ok && paren.Select != nil {
			paren.Select.RewriteOption = option
		}
	}
	switch stmt := stmt.(type) {
	case *tree.Select:
		attachToSelect(stmt)
	case *tree.ParenSelect:
		if stmt.Select != nil {
			attachToSelect(stmt.Select)
		}
	case *tree.Insert:
		attachToSelect(stmt.Rows)
	case *tree.MultiInsert:
		attachToSelect(stmt.Source)
	case *tree.CreateTable:
		attachToSelect(stmt.AsSource)
	}
}

func attachInternalExecutorPrivilegeCheck(ctx context.Context) context.Context {
	// Mark this internal SQL to run with normal privilege validation path.
	return context.WithValue(ctx, internalExecutorPrivilegeCheckKey{}, true)
}

// AttachInternalExecutorPrivilegeCheck forces internal SQL to run through the
// normal privilege validation path instead of bypassing auth as trusted SQL.
func AttachInternalExecutorPrivilegeCheck(ctx context.Context) context.Context {
	return attachInternalExecutorPrivilegeCheck(ctx)
}

func needInternalExecutorPrivilegeCheck(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	if v := ctx.Value(internalExecutorPrivilegeCheckKey{}); v != nil {
		if yes, ok := v.(bool); ok {
			return yes
		}
	}
	return false
}
