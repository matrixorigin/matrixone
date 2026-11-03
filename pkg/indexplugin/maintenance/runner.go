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
)

// RunSQL requires scoped generated-operator provenance from the SQL owner.
func RunSQL(ctx compileplugin.CompileContext, sql, function, indexName string) error {
	if runner, ok := ctx.(compileplugin.IndexMaintenanceRunner); ok {
		return runner.RunIndexMaintenanceSQL(sql, function, indexName)
	}
	return moerr.NewInternalErrorNoCtx("index maintenance owner cannot attest generated SQL")
}
