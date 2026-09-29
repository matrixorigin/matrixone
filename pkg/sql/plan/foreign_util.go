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

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/foreignext"
)

// IsForeignTableDef reports whether tableDef is an ESQL/SQL foreign external
// table and returns its parsed config.  Both the planner-owned envelope and
// the durable feature bit must agree: the envelope alone could be forged
// through the user-controlled rel_createsql JSON of a generic external table.
func IsForeignTableDef(ctx context.Context, tableDef *TableDef) (foreignext.Config, bool, error) {
	if tableDef == nil || tableDef.TableType != catalog.SystemExternalRel {
		return foreignext.Config{}, false, nil
	}
	cfg, found, err := foreignext.ParseCreateSQLEnvelope(ctx, tableDef.Createsql)
	if err != nil {
		return foreignext.Config{}, false, err
	}
	if !found {
		if features.IsForeignExternal(tableDef.FeatureFlag) {
			return foreignext.Config{}, false, moerr.NewInvalidInput(ctx, "foreign external table is missing its catalog envelope")
		}
		return foreignext.Config{}, false, nil
	}
	if !features.IsForeignExternal(tableDef.FeatureFlag) {
		return foreignext.Config{}, false, moerr.NewInvalidInput(ctx, "foreign envelope present on a table without the foreign feature flag")
	}
	return cfg, true, nil
}
