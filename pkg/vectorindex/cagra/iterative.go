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

package cagra

import (
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// MaxInternalTopK is the largest count of keys one cuVS CAGRA search returns.
const MaxInternalTopK = 1024

// DefaultMaxInternalTopK caps the requested count of a widening search when neither
// max_itopk nor vector_max_itopk is set.
const DefaultMaxInternalTopK = MaxInternalTopK

// IterativeSearch is the CAGRA widening policy of mode=iterative. A round requests
// ITopK keys (cuVS itopk_size is at least the request); the filter runs inside the
// search, so every key returned qualifies. The first round requests the candidate
// budget; each further round doubles it. A round that returns LIMIT rows, or
// requests the cap or every indexed row, is the last. cuVS cannot resume a search,
// so every round searches again.
type IterativeSearch struct{}

var _ searchplugin.IterativeSearchHooks = IterativeSearch{}

// StartRound returns the first round of req.
func (IterativeSearch) StartRound(_ *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats) searchplugin.Round {
	start := max(req.CandidateBudget, req.ResultLimit, 1)
	return searchplugin.Round{ITopK: min(start, iterativeCap(req, stats))}
}

// NextRound returns the round after prev, which returned got rows.
func (IterativeSearch) NextRound(_ *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats, prev searchplugin.Round, got uint64) (searchplugin.Round, bool) {
	capITopK := iterativeCap(req, stats)
	if got >= max(req.ResultLimit, 1) || prev.ITopK >= capITopK {
		return prev, false
	}
	return searchplugin.Round{ITopK: min(prev.ITopK*2, capITopK)}, true
}

// iterativeCap is the largest ITopK of a round: max_itopk (DefaultMaxInternalTopK when
// unset), never below the candidate budget, at most the indexed rows when known.
func iterativeCap(req searchplugin.Request, stats searchplugin.SearchStats) uint64 {
	capITopK := req.MaxInternalTopK
	if capITopK == 0 {
		capITopK = DefaultMaxInternalTopK
	}
	capITopK = max(capITopK, req.CandidateBudget, req.ResultLimit, 1)
	if stats.IndexRows != 0 {
		capITopK = min(capITopK, stats.IndexRows)
	}
	return capITopK
}
