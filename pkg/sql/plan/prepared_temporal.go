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
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Only a direct marker and its ordinary implicit casts own dynamic temporal
// metadata. Explicit casts and functions containing a marker keep their contract.
func temporalDirectMarker(expr *Expr) bool {
	expr = unwrapPreparedImplicitCast(expr, true)
	return expr != nil && expr.GetP() != nil && expr.GetP().Pos >= 0
}

func temporalDirectMarkerArgument(args []*Expr, original *Expr, index int) bool {
	if original != nil && original.GetF() != nil {
		args = original.GetF().Args
	}
	return index >= 0 && index < len(args) && temporalDirectMarker(args[index])
}

func bindTemporalMarkerConsumer(ctx context.Context, source *Expr, target types.Type) (*Expr, error) {
	// TIMESTAMP owns calendar parsing, including invalid-to-NULL semantics.
	// The subsequent typed conversion can then safely fix this consumer's FSP.
	source = unwrapPreparedImplicitCast(source, true)
	datetime, err := BindFuncExprImplByPlanExpr(ctx, "timestamp", []*Expr{source})
	if err != nil {
		return nil, err
	}
	return appendCastBeforeExpr(ctx, datetime, makePlan2Type(&target))
}
