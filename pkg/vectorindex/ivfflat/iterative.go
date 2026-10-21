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

package ivfflat

import (
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
)

// maxRoundLists is the most lists one round probes.
const maxRoundLists = 4096

// predictionSafety multiplies the lists predicted from the selectivity.
const predictionSafety = 2

// IterativeSearch is the ivfflat widening policy. Rounds continue one centroid ranking:
// each probes the next lists after those of earlier rounds. The first round probes the
// scan's nprobe lists, raised under a membership filter to lists·k/|S| times
// predictionSafety; each further round probes twice the lists of the previous one, at
// most maxRoundLists. A round requests ITopK rows: the INCLUDE first-round limit when the
// scan has one, else the candidate budget. The search stops when it holds the candidate
// budget (at most |S|) rows, or the lists probed reach max_nprobe (all lists when unset).
type IterativeSearch struct{}

var _ searchplugin.IterativeSearchHooks = IterativeSearch{}

// StartRound returns the first round of req.
func (IterativeSearch) StartRound(spec *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats) searchplugin.Round {
	opts, _ := ivfflatplan.DecodeScanOptions(spec.GetAlgoOptions())
	nprobe := max(uint64(opts.InitialProbeCount), 1)
	if stats.Members != 0 && stats.Lists != 0 {
		k := max(req.CandidateBudget, 1)
		predicted := (stats.Lists*k + stats.Members - 1) / stats.Members * predictionSafety
		nprobe = max(nprobe, min(predicted, stats.Lists))
	}
	if capLists := iterativeListCap(req, stats); capLists != 0 {
		nprobe = min(nprobe, capLists)
	}
	return searchplugin.Round{ITopK: roundITopK(req), NProbe: nprobe}
}

// NextRound returns the round after prev; got is the rows of all rounds.
func (IterativeSearch) NextRound(_ *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats, prev searchplugin.Round, got uint64) (searchplugin.Round, bool) {
	target := max(req.CandidateBudget, 1)
	if stats.Members != 0 {
		target = min(target, stats.Members)
	}
	capLists := iterativeListCap(req, stats)
	if got >= target || (capLists != 0 && stats.Probed >= capLists) {
		return prev, false
	}
	next := min(prev.NProbe*2, maxRoundLists)
	if capLists != 0 {
		next = min(next, capLists-stats.Probed)
	}
	return searchplugin.Round{ITopK: prev.ITopK, NProbe: max(next, 1)}, true
}

// iterativeListCap is the most lists the rounds probe in total: max_nprobe, at most the
// index lists; 0 when neither is known.
func iterativeListCap(req searchplugin.Request, stats searchplugin.SearchStats) uint64 {
	switch {
	case req.MaxNprobe != 0 && stats.Lists != 0:
		return min(req.MaxNprobe, stats.Lists)
	case req.MaxNprobe != 0:
		return req.MaxNprobe
	default:
		return stats.Lists
	}
}

// roundITopK is the rows each round requests: the INCLUDE first-round limit, else the
// candidate budget.
func roundITopK(req searchplugin.Request) uint64 {
	if limit, ok, err := ivfflatplan.FirstRoundLimit(req); err == nil && ok {
		return limit
	}
	return req.CandidateBudget
}
