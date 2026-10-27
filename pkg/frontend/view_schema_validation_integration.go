//go:build integration

// Copyright 2026 Matrix Origin
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

package frontend

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// Only _test.go can register a route. Even integration binaries default to the
// ordinary path; no SQL/session/config switch enables this validation harness.
var nativeViewSchemaRoutes sync.Map

type nativeViewSchemaRoute struct {
	database string
	build    func(context.Context, *TxnCompilerContext, tree.Statement, bool) (*plan.Plan, error)
	prepared func(context.Context, *TxnCompilerContext, tree.Statement, []plan.PreparedSourceBinding, []any) (*plan.PreparedExecutionPlan, error)
}

func buildViewSchemaValidationPlan(ctx context.Context, ses FeSession, compiler plan.CompilerContext, stmt tree.Statement, prepare bool) (*plan.Plan, bool, error) {
	route, tcc := nativeViewSchemaValidationRoute(ses, compiler, stmt)
	if route == nil {
		return nil, false, nil
	}
	p, err := route.build(ctx, tcc, stmt, prepare)
	return p, true, err
}

func buildViewSchemaValidationPreparedPlan(ctx context.Context, ses FeSession, compiler plan.CompilerContext, stmt tree.Statement, bindings []plan.PreparedSourceBinding, values []any) (*plan.PreparedExecutionPlan, bool, error) {
	route, tcc := nativeViewSchemaValidationRoute(ses, compiler, stmt)
	if route == nil {
		return nil, false, nil
	}
	p, err := route.prepared(ctx, tcc, stmt, bindings, values)
	return p, true, err
}

func nativeViewSchemaValidationRoute(ses FeSession, compiler plan.CompilerContext, stmt tree.Statement) (*nativeViewSchemaRoute, *TxnCompilerContext) {
	if ses == nil || ses.IsBackgroundSession() {
		return nil, nil
	}
	entry, exists := nativeViewSchemaRoutes.Load(ses.GetService())
	if !exists {
		return nil, nil
	}
	route := entry.(*nativeViewSchemaRoute)
	tcc, ok := compiler.(*TxnCompilerContext)
	if !ok || tcc.DefaultDatabase() != route.database || !nativeViewSchemaSelect(stmt) {
		return nil, nil
	}
	return route, tcc
}

func nativeViewSchemaSelect(stmt tree.Statement) bool {
	switch s := stmt.(type) {
	case *tree.Select, *tree.ParenSelect:
		return true
	case *tree.PrepareStmt:
		return nativeViewSchemaSelect(s.Stmt)
	default:
		return false
	}
}
