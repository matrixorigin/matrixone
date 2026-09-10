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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// ZeroInt64Default is the default for a NOT NULL bigint column that a writer may omit.
//
// The provenance columns (nrow, build_ts) need it. A writer OMITS them when the table it is
// writing to does not have them yet -- the rolling-upgrade window where a new CN meets a table
// the v4_0_7 migration has not widened. That omission must also be VALID on a table which does
// have them, because the shape probe answers "narrow" whenever it cannot read the catalog: a
// transient error would otherwise turn a working write into a rejected one, since a NOT NULL
// column with no default cannot be left out.
//
// The migration's ALTER already says "not null default 0"; this is the same column, declared the
// same way on the CREATE path, so a table's two possible origins agree.
func ZeroInt64Default() *plan.Default {
	return &plan.Default{
		NullAbility: false,
		Expr: &plan.Expr{
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{Isnull: false, Value: &plan.Literal_I64Val{I64Val: 0}},
			},
			Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true},
		},
		OriginString: "0",
	}
}
