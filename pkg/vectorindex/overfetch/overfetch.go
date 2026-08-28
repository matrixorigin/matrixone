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

// Package overfetch computes the candidate budget a post-filtered vector index
// search must fetch so that k rows still survive a filter applied after the
// search (a residual predicate evaluated by the JOIN, not pushed into the
// index). The multiplier depends only on k: smaller limits have higher variance
// in how many candidates survive, so they need proportionally more headroom.
//
// The math lives here — rather than in the planner — because for a prepared
// statement k (LIMIT ?) is not known until EXECUTE. The table-function search
// resolves k at EXECUTE and calls into this package, so a literal limit and a
// bound parameter go through the identical calculation.
package overfetch

import "math"

// MinExtraCandidates is the additive floor in Limit: every budget fetches at
// least k+10, so a small k still has headroom the multiplier alone would not give.
const MinExtraCandidates uint64 = 10

// FactorStep is one bucket of the multiplier step function: k < Below uses Factor.
// Steps are ordered ascending by Below and the last one falls through to the
// package's default factor.
type FactorStep struct {
	Below  uint64
	Factor float64
}

// postFilterSteps and filteredPostModeSteps are the single definition of each
// step function. Both the Go helpers below and the planner's equivalent SQL
// expression (BuildOverFetchLimitExpr) are derived from these tables, so the
// value a new CN computes in Go and the value an old CN computes by evaluating
// the pushed expression cannot drift apart.
var (
	postFilterSteps = []FactorStep{
		{Below: 10, Factor: 5.0},  // Small limits: 5x
		{Below: 50, Factor: 2.0},  // Medium limits: 2x
		{Below: 100, Factor: 1.5}, // Large limits: 1.5x
		{Below: 200, Factor: 1.3}, // Very large limits: 1.3x
	}
	postFilterDefaultFactor = 1.2 // Huge limits: 1.2x

	filteredPostModeSteps = []FactorStep{
		{Below: 50, Factor: 5.0},
		{Below: 100, Factor: 2.0},
		{Below: 200, Factor: 1.5},
	}
	filteredPostModeDefaultFactor = 1.3
)

// PostFilterFactorSteps returns the post-filter step table. The slice is copied so
// a caller building an expression from it cannot mutate the shared definition.
func PostFilterFactorSteps() []FactorStep {
	return append([]FactorStep(nil), postFilterSteps...)
}

// FilteredPostModeFactorSteps returns the ivfflat filtered-post-mode step table.
func FilteredPostModeFactorSteps() []FactorStep {
	return append([]FactorStep(nil), filteredPostModeSteps...)
}

// DefaultFactor is the multiplier used above the last step of the chosen table.
func DefaultFactor(filteredPostMode bool) float64 {
	if filteredPostMode {
		return filteredPostModeDefaultFactor
	}
	return postFilterDefaultFactor
}

func factorFromSteps(k uint64, steps []FactorStep, defaultFactor float64) float64 {
	for _, step := range steps {
		if k < step.Below {
			return step.Factor
		}
	}
	return defaultFactor
}

// PostFilterFactor returns the over-fetch multiplier for a post-filtered vector
// search whose candidates are dropped by a residual filter after the search.
func PostFilterFactor(k uint64) float64 {
	return factorFromSteps(k, postFilterSteps, postFilterDefaultFactor)
}

// FilteredPostModeFactor returns a fixed, more conservative multiplier for
// ivfflat filtered post mode. It intentionally avoids statistics-based
// heuristics so the behavior is predictable across plans.
func FilteredPostModeFactor(k uint64) float64 {
	return factorFromSteps(k, filteredPostModeSteps, filteredPostModeDefaultFactor)
}

// Limit applies factor to k, floored so it always fetches at least k+10, and
// saturates at MaxUint64 instead of overflowing.
func Limit(k uint64, factor float64) uint64 {
	if k == 0 {
		return 0
	}
	if factor < 1 {
		factor = 1
	}
	multiplied := k
	if factor > 1 {
		product := float64(k) * factor
		if product >= float64(math.MaxUint64) {
			multiplied = math.MaxUint64
		} else {
			multiplied = uint64(product)
		}
	}

	withFloor := k
	if k > math.MaxUint64-MinExtraCandidates {
		withFloor = math.MaxUint64
	} else {
		withFloor += MinExtraCandidates
	}
	return max(multiplied, withFloor)
}

// PostFilterLimit is Limit(k, PostFilterFactor(k)) — the budget for hnsw / ivfpq
// / cagra post-filter searches.
func PostFilterLimit(k uint64) uint64 {
	return Limit(k, PostFilterFactor(k))
}

// FilteredPostModeLimit is Limit(k, FilteredPostModeFactor(k)) — the budget for
// ivfflat filtered post mode.
func FilteredPostModeLimit(k uint64) uint64 {
	return Limit(k, FilteredPostModeFactor(k))
}
