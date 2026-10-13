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

package hnsw

import (
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// DefaultMaxInternalTopK caps the requested count of a widening search when neither
// max_itopk nor vector_max_itopk is set (pgvector's hnsw.max_scan_tuples default).
const DefaultMaxInternalTopK = 20000

// predictionSafety multiplies the requested count predicted from the selectivity.
const predictionSafety = 2

// IterativeSearch is the HNSW widening policy. A round requests ITopK keys from usearch,
// which searches with max(ITopK, ef_search). The first round requests the larger of the
// candidate budget and, when the selectivity is known, k·N/|S| times predictionSafety;
// each further round doubles it. A round that returns min(k, |S|) rows, or requests the
// cap or every indexed row, is the last. usearch cannot resume a search, so every round
// searches again.
type IterativeSearch struct{}

var _ searchplugin.IterativeSearchHooks = IterativeSearch{}

// StartRound returns the first round of req.
func (IterativeSearch) StartRound(_ *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats) searchplugin.Round {
	k := max(req.ResultLimit, 1)
	start := max(req.CandidateBudget, k)
	if stats.Members != 0 && stats.IndexRows != 0 {
		predicted := (k*stats.IndexRows + stats.Members - 1) / stats.Members * predictionSafety
		start = max(start, min(predicted, stats.IndexRows))
	}
	return searchplugin.Round{ITopK: min(start, iterativeCap(req, stats))}
}

// NextRound returns the round after prev, which returned got rows.
func (IterativeSearch) NextRound(_ *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats, prev searchplugin.Round, got uint64) (searchplugin.Round, bool) {
	target := max(req.ResultLimit, 1)
	if stats.Members != 0 {
		target = min(target, stats.Members)
	}
	capITopK := iterativeCap(req, stats)
	if got >= target || prev.ITopK >= capITopK {
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
	capITopK = max(capITopK, req.CandidateBudget, max(req.ResultLimit, 1))
	if stats.IndexRows != 0 {
		capITopK = min(capITopK, stats.IndexRows)
	}
	return capITopK
}

// searchIterative answers a membership-filtered search in the rounds of IterativeSearch,
// each one a searchOnce.
func (s *HnswSearch[T]) searchIterative(
	sqlproc *sqlexec.SqlProcess,
	query []T,
	indexCounts []uint,
	rt vectorindex.RuntimeConfig,
) ([]int64, []float64, error) {
	var total uint64
	for _, n := range indexCounts {
		total += uint64(n)
	}
	if total == 0 {
		return []int64{}, []float64{}, nil
	}
	req := searchplugin.Request{
		ResultLimit:     uint64(rt.ResultLimit),
		CandidateBudget: uint64(rt.Limit),
		MaxInternalTopK: uint64(rt.MaxLimit),
	}
	stats := searchplugin.SearchStats{IndexRows: total, Members: rt.MembershipCount}
	policy := IterativeSearch{}
	round := policy.StartRound(nil, req, stats)
	for {
		keys, distances, err := s.searchOnce(sqlproc, query, indexCounts, uint(round.ITopK), rt.Membership, rt.OrigFuncName)
		if err != nil {
			return nil, nil, err
		}
		next, more := policy.NextRound(nil, req, stats, round, uint64(len(keys)))
		if !more {
			return keys, distances, nil
		}
		round = next
	}
}
