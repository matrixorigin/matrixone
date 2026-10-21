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
	"math"
	"testing"

	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	ivfpqplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/plan"
	"github.com/stretchr/testify/require"
)

func TestIterativeSearchRounds(t *testing.T) {
	opts, err := ivfpqplan.EncodeScanOptions(ivfpqplan.ScanOptions{Nprobe: 3})
	require.NoError(t, err)
	spec := &plan.IndexSearchScan{AlgoOptions: opts}
	p := IterativeSearch{}
	req := searchplugin.Request{ResultLimit: 10, CandidateBudget: 20}
	stats := searchplugin.SearchStats{Lists: 16}

	// the first round probes nprobe lists for the candidate budget; each further
	// round doubles the lists
	r := p.StartRound(spec, req, stats)
	require.Equal(t, searchplugin.Round{ITopK: 20, NProbe: 3}, r)
	r, more := p.NextRound(spec, req, stats, r, 2)
	require.True(t, more)
	require.Equal(t, searchplugin.Round{ITopK: 20, NProbe: 6}, r)

	// stops when LIMIT rows qualify, and at every list
	_, more = p.NextRound(spec, req, stats, r, 10)
	require.False(t, more)
	r, more = p.NextRound(spec, req, stats, searchplugin.Round{ITopK: 20, NProbe: 12}, 2)
	require.True(t, more)
	require.Equal(t, uint64(16), r.NProbe)
	_, more = p.NextRound(spec, req, stats, r, 2)
	require.False(t, more)

	// max_nprobe caps the lists, also the first round
	capped := req
	capped.MaxNprobe = 4
	r, more = p.NextRound(spec, capped, stats, searchplugin.Round{ITopK: 20, NProbe: 3}, 2)
	require.True(t, more)
	require.Equal(t, uint64(4), r.NProbe)
	_, more = p.NextRound(spec, capped, stats, r, 2)
	require.False(t, more)
	capped.MaxNprobe = 2
	require.Equal(t, uint64(2), p.StartRound(spec, capped, stats).NProbe)

	// no index lists: only the overflow is searched, so the first round is the last
	require.Equal(t, uint64(5), iterativeListCap(searchplugin.Request{MaxNprobe: 5}, searchplugin.SearchStats{}))
	_, more = p.NextRound(spec, req, searchplugin.SearchStats{}, searchplugin.Round{NProbe: 3}, 2)
	require.False(t, more)
	_, more = p.NextRound(spec, capped, searchplugin.SearchStats{}, searchplugin.Round{NProbe: 2}, 2)
	require.False(t, more)
	require.Equal(t, uint64(1), p.StartRound(nil, req, stats).NProbe)
}

// iterativeRounds runs the rounds of req to the last one, every round returning
// got(round) rows, and returns the rounds and the last round.
func iterativeRounds(t *testing.T, spec *plan.IndexSearchScan, req searchplugin.Request, stats searchplugin.SearchStats, got func(searchplugin.Round) uint64) (int, searchplugin.Round) {
	p := IterativeSearch{}
	r := p.StartRound(spec, req, stats)
	for rounds := 1; rounds <= 70; rounds++ {
		require.NotZero(t, r.NProbe)
		next, more := p.NextRound(spec, req, stats, r, got(r))
		if !more {
			return rounds, r
		}
		require.Greater(t, next.NProbe, r.NProbe)
		r = next
	}
	require.FailNow(t, "iterative search did not terminate", "req=%+v stats=%+v round=%+v", req, stats, r)
	return 0, r
}

func TestIterativeSearchTerminates(t *testing.T) {
	opts, err := ivfpqplan.EncodeScanOptions(ivfpqplan.ScanOptions{Nprobe: 3})
	require.NoError(t, err)
	spec := &plan.IndexSearchScan{AlgoOptions: opts}
	req := searchplugin.Request{ResultLimit: 10, CandidateBudget: 20}
	short := func(searchplugin.Round) uint64 { return 3 }

	// omitted lists, overflow only: five rows, LIMIT 10
	rounds, _ := iterativeRounds(t, spec, req, searchplugin.SearchStats{IndexRows: 5}, func(searchplugin.Round) uint64 { return 5 })
	require.Equal(t, 1, rounds)
	// overflow only, a filter keeps fewer rows than LIMIT
	rounds, _ = iterativeRounds(t, spec, req, searchplugin.SearchStats{IndexRows: 1000}, short)
	require.Equal(t, 1, rounds)
	// built lists, no max_nprobe: rounds stop at every list
	rounds, last := iterativeRounds(t, spec, req, searchplugin.SearchStats{IndexRows: 100000, Lists: 1024}, short)
	require.Equal(t, uint64(1024), last.NProbe)
	require.Equal(t, 10, rounds) // 3, 6, ..., 768, 1024
	// max_nprobe below the lists
	capped := req
	capped.MaxNprobe = 40
	_, last = iterativeRounds(t, spec, capped, searchplugin.SearchStats{IndexRows: 100000, Lists: 1024}, short)
	require.Equal(t, uint64(40), last.NProbe)
	// every indexed row returned before LIMIT
	rounds, _ = iterativeRounds(t, spec, req, searchplugin.SearchStats{IndexRows: 7, Lists: 4}, func(searchplugin.Round) uint64 { return 7 })
	require.Equal(t, 1, rounds)
	// full page
	rounds, _ = iterativeRounds(t, spec, req, searchplugin.SearchStats{IndexRows: 100000, Lists: 1024}, func(searchplugin.Round) uint64 { return 10 })
	require.Equal(t, 1, rounds)
	// every member of a smaller membership
	rounds, _ = iterativeRounds(t, spec, req, searchplugin.SearchStats{IndexRows: 100000, Lists: 1024, Members: 4}, func(searchplugin.Round) uint64 { return 4 })
	require.Equal(t, 1, rounds)
	// doubling saturates instead of wrapping
	huge := searchplugin.SearchStats{IndexRows: 100000, Lists: math.MaxUint64}
	r, more := IterativeSearch{}.NextRound(spec, req, huge, searchplugin.Round{ITopK: 20, NProbe: 1 << 63}, 3)
	require.True(t, more)
	require.Equal(t, uint64(math.MaxUint64), r.NProbe)
	_, more = IterativeSearch{}.NextRound(spec, req, huge, r, 3)
	require.False(t, more)
}
