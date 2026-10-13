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

package ivfpq

import (
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	ivfpqplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/plan"
)

// AdvancedSearch is the IVF-PQ widening policy of mode=iterative. A round probes
// NProbe lists and requests ITopK keys, the candidate budget; the filter runs inside
// the search, so every key returned qualifies. The first round probes the scan's
// nprobe lists; each further round doubles them, up to max_nprobe (every list when
// unset). A round that returns LIMIT rows, or probes the cap, is the last. cuVS
// ivf_pq::search takes no list subset, so every round searches the lists again.
type AdvancedSearch struct{}

var _ searchplugin.AdvancedSearchHooks = AdvancedSearch{}

// StartRound returns the first round of req.
func (AdvancedSearch) StartRound(spec *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats) searchplugin.Round {
	opts, _ := ivfpqplan.DecodeScanOptions(spec.GetAlgoOptions())
	nprobe := max(uint64(opts.Nprobe), 1)
	if capLists := advancedListCap(req, stats); capLists != 0 {
		nprobe = min(nprobe, capLists)
	}
	return searchplugin.Round{ITopK: max(req.CandidateBudget, req.ResultLimit, 1), NProbe: nprobe}
}

// NextRound returns the round after prev, which returned got rows.
func (AdvancedSearch) NextRound(_ *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats, prev searchplugin.Round, got uint64) (searchplugin.Round, bool) {
	capLists := advancedListCap(req, stats)
	if got >= max(req.ResultLimit, 1) || (capLists != 0 && prev.NProbe >= capLists) {
		return prev, false
	}
	next := prev.NProbe * 2
	if capLists != 0 {
		next = min(next, capLists)
	}
	return searchplugin.Round{ITopK: prev.ITopK, NProbe: next}, true
}

// advancedListCap is the most lists a round probes: max_nprobe, at most the index
// lists; 0 when neither is known.
func advancedListCap(req searchplugin.Request, stats searchplugin.SearchStats) uint64 {
	switch {
	case req.MaxNprobe != 0 && stats.Lists != 0:
		return min(req.MaxNprobe, stats.Lists)
	case req.MaxNprobe != 0:
		return req.MaxNprobe
	default:
		return stats.Lists
	}
}
