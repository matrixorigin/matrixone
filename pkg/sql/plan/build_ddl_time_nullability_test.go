// Copyright 2021 - 2026 Matrix Origin
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// TIME() over a NOT NULL string column synthesizes NULL with a truncation
// warning for invalid input, so view and CTAS materialization must derive a
// nullable column instead of promising NOT NULL metadata. The numeric
// overload keeps failing the whole statement instead of producing NULL and
// therefore keeps its NOT NULL inference.
func TestBuildCreateTimeConversionNullabilityMetadata(t *testing.T) {
	for _, tc := range []struct {
		name      string
		selectSQL string
		nullable  bool
	}{
		{
			name:      "string conversion stays nullable from a not null column",
			selectSQL: `select time(n.n_name) as tv from nation n`,
			nullable:  true,
		},
		{
			name:      "numeric conversion keeps its not null inference",
			selectSQL: `select time(n.n_nationkey) as tv from nation n`,
			nullable:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, prefix := range []string{"create view tv_view (tv) as ", "create table tv_ctas as "} {
				func() {
					sql := prefix + tc.selectSQL
					ctx := &rootSQLCompilerContext{MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)), rootSQL: sql}
					stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
					require.NoError(t, err)
					defer stmt.Free()
					p, err := BuildPlan(ctx, stmt, false)
					require.NoError(t, err)
					var cols []*plan.ColDef
					if view := p.GetDdl().GetCreateView(); view != nil {
						cols = view.TableDef.Cols
					} else {
						cols = p.GetDdl().GetCreateTable().TableDef.Cols
					}
					require.NotEmpty(t, cols)
					require.Equal(t, "tv", cols[0].Name)
					require.Equal(t, int32(types.T_time), cols[0].Typ.Id)
					require.Equal(t, tc.nullable, cols[0].Default.NullAbility,
						"materialized TIME() metadata must match the runtime NULL behavior")
				}()
			}
		})
	}
}
