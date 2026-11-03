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

package maintenance

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// RunSQL requires scoped generated-operator provenance from the SQL owner.
func RunSQL(ctx compileplugin.CompileContext, sql, function, indexName string) error {
	if runner, ok := ctx.(compileplugin.IndexMaintenanceRunner); ok {
		return runner.RunIndexMaintenanceSQL(sql, function, indexName)
	}
	return moerr.NewInternalErrorNoCtx("index maintenance owner cannot attest generated SQL")
}

// SelectDefinitions projects plugin roles onto an owned index group. Invalid
// roles return nil; callers retain their own context and diagnostic policy.
// An empty role list remains an empty selection for the resolver to reject.
func SelectDefinitions(group map[string]*plan.IndexDef, roles []string) map[string]*plan.IndexDef {
	selected := make(map[string]*plan.IndexDef, len(roles))
	for _, role := range roles {
		if role == "" || group[role] == nil || selected[role] != nil {
			return nil
		}
		selected[role] = group[role]
	}
	return selected
}
