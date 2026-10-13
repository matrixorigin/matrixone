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

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const SQLSelectLimitVariable = "sql_select_limit"

// MakeSQLSelectLimitExpr returns a dynamic expression so cached and prepared
// plans resolve the current session value each time they execute.
func MakeSQLSelectLimitExpr(ctx context.Context) (*planpb.Expr, error) {
	textType := types.T_text.ToType()
	uint64Type := types.T_uint64.ToType()
	variable := &planpb.Expr{
		Typ: makePlan2Type(&textType),
		Expr: &planpb.Expr_V{
			V: &planpb.VarRef{
				Name:   SQLSelectLimitVariable,
				System: true,
			},
		},
	}
	return makePlan2CastExpr(ctx, variable, makePlan2Type(&uint64Type))
}
