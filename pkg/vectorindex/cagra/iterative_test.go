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
	"testing"

	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/stretchr/testify/require"
)

func TestIterativeSearchRounds(t *testing.T) {
	p := IterativeSearch{}
	req := searchplugin.Request{ResultLimit: 10, CandidateBudget: 20}
	stats := searchplugin.SearchStats{IndexRows: 1000}

	// the first round requests the candidate budget; each further round doubles it
	r := p.StartRound(nil, req, stats)
	require.Equal(t, uint64(20), r.ITopK)
	r, more := p.NextRound(nil, req, stats, r, 3)
	require.True(t, more)
	require.Equal(t, uint64(40), r.ITopK)

	// stops when LIMIT rows qualify
	_, more = p.NextRound(nil, req, stats, r, 10)
	require.False(t, more)

	// stops at max_itopk, never below the budget, and at the indexed rows
	capped := req
	capped.MaxInternalTopK = 30
	r, more = p.NextRound(nil, capped, stats, searchplugin.Round{ITopK: 20}, 3)
	require.True(t, more)
	require.Equal(t, uint64(30), r.ITopK)
	_, more = p.NextRound(nil, capped, stats, r, 3)
	require.False(t, more)
	capped.MaxInternalTopK = 5
	require.Equal(t, uint64(20), p.StartRound(nil, capped, stats).ITopK)
	require.Equal(t, uint64(8), p.StartRound(nil, req, searchplugin.SearchStats{IndexRows: 8}).ITopK)
	require.Equal(t, uint64(DefaultMaxInternalTopK), iterativeCap(req, searchplugin.SearchStats{}))
}

func TestIterativeSearchCapIsCuvsLimit(t *testing.T) {
	p := IterativeSearch{}
	req := searchplugin.Request{ResultLimit: 20, CandidateBudget: 20}
	stats := searchplugin.SearchStats{IndexRows: 1000000}
	require.Equal(t, uint64(MaxInternalTopK), iterativeCap(req, stats))

	// rounds that never fill LIMIT stop at MaxInternalTopK
	r := p.StartRound(nil, req, stats)
	for more := true; more; {
		require.LessOrEqual(t, r.ITopK, uint64(MaxInternalTopK))
		r, more = p.NextRound(nil, req, stats, r, 10)
	}
	require.Equal(t, uint64(MaxInternalTopK), r.ITopK)
}
