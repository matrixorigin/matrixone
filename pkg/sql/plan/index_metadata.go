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

package plan

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

// validateTableIndexDefinitions enforces the dense IndexDef invariant before an
// operation can omit maintenance, locking, or lifecycle work for an unknown
// index. Healthy persisted metadata is dense; a nil entry means an in-memory
// producer supplied incomplete metadata and must fail closed rather than be
// treated as absent.
func validateTableIndexDefinitions(tableDef *TableDef) error {
	if tableDef == nil {
		return nil
	}
	for pos, idxDef := range tableDef.Indexes {
		if idxDef == nil {
			return moerr.NewInternalErrorNoCtxf(
				"nil index metadata for table %q at position %d", tableDef.Name, pos)
		}
	}
	return nil
}
