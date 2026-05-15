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

package vectorplan

import (
	"strings"
	"unicode/utf8"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// This file hosts standalone helper functions that vector-index plugins
// call. They originated in pkg/sql/plan but moved here to eliminate the
// function-variable indirection (Phase 5b). pkg/sql/plan keeps one-line
// aliases so its own internal callers keep compiling unchanged.

// DeepCopyRankOption clones a RankOption (5 LoC, no tributaries).
// Lifted from pkg/sql/plan/deepcopy.go.
func DeepCopyRankOption(opt *plan.RankOption) *plan.RankOption {
	if opt == nil {
		return nil
	}
	return &plan.RankOption{Mode: opt.Mode}
}

// MakeRuntimeFilter constructs a RuntimeFilterSpec.
// Lifted from pkg/sql/plan/utils.go.
func MakeRuntimeFilter(tag int32, matchPrefix bool, upperlimit int32, expr *plan.Expr, notOnPk bool) *plan.RuntimeFilterSpec {
	return &plan.RuntimeFilterSpec{
		Tag:         tag,
		UpperLimit:  upperlimit,
		Expr:        expr,
		MatchPrefix: matchPrefix,
		NotOnPk:     notOnPk,
	}
}

// CalculatePostFilterOverFetchFactor returns the over-fetch multiplier
// based on limit size for post-filtered ANN queries. Smaller limits need
// more over-fetching due to higher variance.
// Lifted from pkg/sql/plan/apply_indices.go.
func CalculatePostFilterOverFetchFactor(originalLimit uint64) float64 {
	switch {
	case originalLimit < 10:
		return 5.0
	case originalLimit < 50:
		return 2.0
	case originalLimit < 100:
		return 1.5
	case originalLimit < 200:
		return 1.3
	default:
		return 1.2
	}
}

// CalculateFilteredPostModeOverFetchFactor is the conservative variant
// IVF-FLAT uses in post mode when filters remain on the scan. Fixed
// buckets, no stats — predictable across plans.
// Lifted from pkg/sql/plan/apply_indices.go.
func CalculateFilteredPostModeOverFetchFactor(originalLimit uint64) float64 {
	switch {
	case originalLimit < 50:
		return 5.0
	case originalLimit < 100:
		return 2.0
	case originalLimit < 200:
		return 1.5
	default:
		return 1.3
	}
}

// ParseIncludedColumnsFromParams reads the comma-joined "included_columns"
// entry from an index's algo-params JSON. Returns nil when the key is
// absent or empty.
// Lifted from pkg/sql/plan/filter_predicate.go.
func ParseIncludedColumnsFromParams(indexAlgoParams string) ([]string, error) {
	if indexAlgoParams == "" {
		return nil, nil
	}
	val, err := sonic.Get([]byte(indexAlgoParams), catalog.IncludedColumns)
	if err != nil {
		return nil, nil
	}
	joined, err := val.StrictString()
	if err != nil || joined == "" {
		return nil, nil
	}
	raw := strings.Split(joined, ",")
	out := make([]string, 0, len(raw))
	for _, n := range raw {
		n = strings.TrimSpace(n)
		if n != "" {
			out = append(out, n)
		}
	}
	return out, nil
}

// MakePlan2StringConstExprWithType wraps a string literal as a typed
// *plan.Expr (T_varchar or T_char for the empty string).
// Lifted from pkg/sql/plan/make.go (plus its tributary
// makePlan2StringConstExpr, inlined here).
func MakePlan2StringConstExprWithType(v string, isBin ...bool) *plan.Expr {
	width := int32(utf8.RuneCountInString(v))
	id := int32(types.T_varchar)
	if width == 0 {
		id = int32(types.T_char)
	}
	lit := &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value:  &plan.Literal_Sval{Sval: v},
	}}
	if len(isBin) > 0 {
		lit.Lit.IsBin = isBin[0]
	}
	return &plan.Expr{
		Expr: lit,
		Typ: plan.Type{
			Id:          id,
			NotNullable: true,
			Width:       width,
		},
	}
}
