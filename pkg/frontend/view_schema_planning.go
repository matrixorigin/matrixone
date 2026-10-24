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

package frontend

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// BuildPlanWithViewSchema explicitly selects the derived View contract for one
// SELECT/CTAS/PREPARE binding. It requires an established statement/transaction and a
// root authorizer, and leaves final statement/plan execution authorization with
// the existing frontend owner. The returned plan has independent ownership and
// uses the existing CatalogDependencies/PrepareSchemas invalidation model.
// Default SQL, PREPARE and cursor routing remains unchanged until rollout.
func (tcc *TxnCompilerContext) BuildPlanWithViewSchema(
	ctx context.Context, stmt tree.Statement, isPrepare bool, authorize ViewSchemaAuthorizer,
) (*plan.Plan, error) {
	request := tcc.NewViewSchemaRequest(ctx, authorize)
	defer request.Close()
	return request.BuildPlan(stmt, isPrepare)
}
