// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// BuildPlan is the opt-in SELECT/CTAS/PREPARE consumer of the read-only View binding
// contract. It binds a real executable subtree, never a description memo scan.
// The caller owns stmt and the returned plan, and must perform the usual final
// statement/plan execution authorization before executing it. Binding.Authorize
// checks each root name before lookup; it does not replace View security chains.
// Neither the plan nor its provenance retains this request or its compiler.
// Existing SQL entry points do not select this mode automatically.
func (r *ViewSchemaRequest) BuildPlan(stmt tree.Statement, isPrepare bool) (result *Plan, err error) {
	select {
	case <-r.ctx.Done():
		return nil, r.cause(nil)
	case <-r.gate:
	default:
		return nil, ErrViewSchemaBusy
	}
	completed := false
	defer func() {
		if !completed {
			r.clearMemo()
		}
		r.gate <- struct{}{}
	}()
	switch s := stmt.(type) {
	case *tree.Select, *tree.ParenSelect:
	case *tree.PrepareStmt:
	case *tree.PrepareString:
		if len(s.Sql) > viewSchemaInputLimit {
			return nil, ErrViewSchemaLimit
		}
	case *tree.CreateTable:
		if !s.IsAsSelect || s.AsSource == nil {
			return nil, moerr.NewNotSupported(r.ctx, "view schema planning requires SELECT or CTAS")
		}
	default:
		return nil, moerr.NewNotSupported(r.ctx, "view schema planning requires SELECT or CTAS")
	}
	if err = r.open(); err != nil {
		return nil, err
	}
	if r.roots == viewSchemaWorkLimit {
		return nil, ErrViewSchemaLimit
	}
	r.roots++
	state := newViewSchemaDerivation(r)
	defer state.close()
	state.planning = true
	// Inherited publication state belongs to a previous root, not to this
	// statement's namespace. bindView establishes/restores each publisher domain.
	previousSub := state.compiler.GetQueryingSubscription()
	state.compiler.SetQueryingSubscription(nil)
	defer state.compiler.SetQueryingSubscription(previousSub)
	result, err = BuildPlan(state.compiler, stmt, isPrepare)
	if err != nil {
		return nil, r.cause(err)
	}
	if err = r.check(); err != nil {
		return nil, err
	}
	inner := result
	if prepared := result.GetDcl().GetPrepare(); prepared != nil {
		inner = prepared.Plan
	}
	query := inner.GetQuery()
	if query == nil && inner.GetDdl() != nil {
		query = inner.GetDdl().GetQuery()
	}
	if query != nil {
		query.CatalogDependencies = appendPrepareSchemas(query.CatalogDependencies, state.planDependencies...)
	}
	result.IsPrepare = isPrepare
	completed = true
	return result, nil
}
