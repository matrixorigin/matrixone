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
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// getLiteralUint64 returns a LIMIT/OFFSET value only when it is already a
// uint64 literal. Optimizer rules must not treat a non-literal expression as
// zero: prepared parameters and variables are evaluated at execution time.
func getLiteralUint64(expr *plan.Expr) (uint64, bool) {
	if expr == nil || expr.GetLit() == nil || expr.GetLit().Isnull {
		return 0, false
	}
	value, ok := expr.GetLit().Value.(*plan.Literal_U64Val)
	if !ok {
		return 0, false
	}
	return value.U64Val, true
}

// buildCandidateLimit computes the number of rows an internal optimization
// path must preserve before the user-visible OFFSET is applied. A rule may use
// the returned expression as an internal LIMIT, but OFFSET itself must remain
// on the final result node.
//
// Dynamic LIMIT without OFFSET is safe to forward. When OFFSET is dynamic, or
// literal LIMIT+OFFSET overflows uint64, the optimizer must leave the internal
// path unbounded instead of risking a wrong result.
func buildCandidateLimit(limit, offset *plan.Expr) (*plan.Expr, bool) {
	if limit == nil {
		return nil, false
	}
	if offset == nil {
		return DeepCopyExpr(limit), true
	}

	offsetValue, ok := getLiteralUint64(offset)
	if !ok {
		return nil, false
	}
	if offsetValue == 0 {
		return DeepCopyExpr(limit), true
	}
	limitValue, ok := getLiteralUint64(limit)
	if !ok {
		return nil, false
	}

	sum, carry := bits.Add64(limitValue, offsetValue, 0)
	if carry != 0 {
		return nil, false
	}
	return makePlan2Uint64ConstExprWithType(sum), true
}
