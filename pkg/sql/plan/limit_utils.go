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

// shouldPushFulltextCandidateLimit reports whether one fulltext stream sees the
// complete candidate domain for LIMIT+OFFSET. A residual source predicate is
// safe only when prefilter pushdown applies an exact membership filter inside
// search. A Bloom false positive can otherwise displace a true top-k row before
// the final join removes it.
func shouldPushFulltextCandidateLimit(fulltextStreams, residualFilters int, prefilterPushdown, exactPrefilter bool) bool {
	return fulltextStreams == 1 && (residualFilters == 0 || prefilterPushdown && exactPrefilter)
}

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

// composePagination collapses two consecutive LIMIT/OFFSET windows. The inner
// window is evaluated first, followed by the outer window. Once a scan already
// owns pagination, overwriting it with a newly exposed Project window changes
// both cardinality and row position; callers must either compose the windows or
// keep both operators.
//
// A previously unbounded scan can accept arbitrary runtime expressions without
// composition. Dynamic expressions also remain movable when the two windows do
// not interact (an inner OFFSET followed by an outer LIMIT). When composition
// needs arithmetic or a minimum, every participating expression must be a
// uint64 literal. This deliberately fails closed rather than manufacturing
// runtime arithmetic whose overflow and error behavior would become part of the
// optimizer contract. It also retains both operators when a nonzero outer
// OFFSET exhausts a nonempty inner LIMIT: collapsing that case to LIMIT 0 would
// let the compiler skip row filters that the inner window previously had to
// evaluate.
func composePagination(
	innerLimit, innerOffset, outerLimit, outerOffset *plan.Expr,
) (limit, offset *plan.Expr, ok bool) {
	if outerLimit == nil && outerOffset == nil {
		return innerLimit, innerOffset, true
	}
	if innerLimit == nil && innerOffset == nil {
		return outerLimit, outerOffset, true
	}
	if innerLimit == nil && outerOffset == nil {
		return outerLimit, innerOffset, true
	}

	innerLimitValue, innerLimitSet, ok := literalPaginationValue(innerLimit)
	if !ok {
		return nil, nil, false
	}
	innerOffsetValue, _, ok := literalPaginationValue(innerOffset)
	if !ok {
		return nil, nil, false
	}
	outerLimitValue, outerLimitSet, ok := literalPaginationValue(outerLimit)
	if !ok {
		return nil, nil, false
	}
	outerOffsetValue, _, ok := literalPaginationValue(outerOffset)
	if !ok {
		return nil, nil, false
	}
	if innerLimitSet && innerLimitValue > 0 && outerOffsetValue >= innerLimitValue &&
		(!outerLimitSet || outerLimitValue > 0) {
		return nil, nil, false
	}

	consumedInnerRows := outerOffsetValue
	resultLimitValue := outerLimitValue
	resultLimitSet := outerLimitSet
	if innerLimitSet {
		if consumedInnerRows > innerLimitValue {
			consumedInnerRows = innerLimitValue
		}
		remainingInnerRows := innerLimitValue - consumedInnerRows
		if !resultLimitSet || remainingInnerRows < resultLimitValue {
			resultLimitValue = remainingInnerRows
		}
		resultLimitSet = true
	}

	resultOffsetValue, carry := bits.Add64(innerOffsetValue, consumedInnerRows, 0)
	if carry != 0 {
		return nil, nil, false
	}
	if resultLimitSet {
		limit = makePlan2Uint64ConstExprWithType(resultLimitValue)
	}
	if resultOffsetValue != 0 {
		offset = makePlan2Uint64ConstExprWithType(resultOffsetValue)
	}
	return limit, offset, true
}

func literalPaginationValue(expr *plan.Expr) (value uint64, present, ok bool) {
	if expr == nil {
		return 0, false, true
	}
	value, ok = getLiteralUint64(expr)
	return value, true, ok
}
