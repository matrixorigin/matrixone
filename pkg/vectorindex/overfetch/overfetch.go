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

// PostFilterFactor returns the over-fetch multiplier for a post-filtered vector
// search whose candidates are dropped by a residual filter after the search.
func PostFilterFactor(k uint64) float64 {
	if k < 10 {
		return 5.0 // Small limits: 5x
	} else if k < 50 {
		return 2.0 // Medium limits: 2x
	} else if k < 100 {
		return 1.5 // Large limits: 1.5x
	} else if k < 200 {
		return 1.3 // Very large limits: 1.3x
	}
	return 1.2 // Huge limits: 1.2x
}

// FilteredPostModeFactor returns a fixed, more conservative multiplier for
// ivfflat filtered post mode. It intentionally avoids statistics-based
// heuristics so the behavior is predictable across plans.
func FilteredPostModeFactor(k uint64) float64 {
	if k < 50 {
		return 5.0
	} else if k < 100 {
		return 2.0
	} else if k < 200 {
		return 1.5
	}
	return 1.3
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
	if k > math.MaxUint64-10 {
		withFloor = math.MaxUint64
	} else {
		withFloor += 10
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
