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
	"testing"

	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/stretchr/testify/require"
)

func TestAdvancedSearchStartRound(t *testing.T) {
	p := AdvancedSearch{}
	// no selectivity: the candidate budget, at least k
	require.Equal(t, uint64(20), p.StartRound(nil, searchplugin.Request{ResultLimit: 10, CandidateBudget: 20}, searchplugin.SearchStats{}).ITopK)
	require.Equal(t, uint64(10), p.StartRound(nil, searchplugin.Request{ResultLimit: 10}, searchplugin.SearchStats{}).ITopK)
	// k·N/|S| times 2: 10·1000/500·2 = 40
	require.Equal(t, uint64(40), p.StartRound(nil, searchplugin.Request{ResultLimit: 10, CandidateBudget: 20},
		searchplugin.SearchStats{IndexRows: 1000, Members: 500}).ITopK)
	// the prediction is bounded by the indexed rows and the cap
	require.Equal(t, uint64(1000), p.StartRound(nil, searchplugin.Request{ResultLimit: 10},
		searchplugin.SearchStats{IndexRows: 1000, Members: 1}).ITopK)
	require.Equal(t, uint64(300), p.StartRound(nil, searchplugin.Request{ResultLimit: 10, MaxInternalTopK: 300},
		searchplugin.SearchStats{IndexRows: 1000, Members: 1}).ITopK)
	// a larger candidate budget wins
	require.Equal(t, uint64(800), p.StartRound(nil, searchplugin.Request{ResultLimit: 10, CandidateBudget: 800},
		searchplugin.SearchStats{IndexRows: 1000, Members: 500}).ITopK)
}

func TestAdvancedSearchNextRound(t *testing.T) {
	p := AdvancedSearch{}
	req := searchplugin.Request{ResultLimit: 10, MaxInternalTopK: 100}
	stats := searchplugin.SearchStats{IndexRows: 1000, Members: 500}
	// short: double
	next, more := p.NextRound(nil, req, stats, searchplugin.Round{ITopK: 20}, 3)
	require.True(t, more)
	require.Equal(t, uint64(40), next.ITopK)
	// doubling stops at the cap, and the cap round is the last
	next, more = p.NextRound(nil, req, stats, searchplugin.Round{ITopK: 80}, 3)
	require.True(t, more)
	require.Equal(t, uint64(100), next.ITopK)
	_, more = p.NextRound(nil, req, stats, searchplugin.Round{ITopK: 100}, 3)
	require.False(t, more)
	// filled: stop
	_, more = p.NextRound(nil, req, stats, searchplugin.Round{ITopK: 20}, 10)
	require.False(t, more)
	// fewer members than k: the target is the member count
	_, more = p.NextRound(nil, req, searchplugin.SearchStats{IndexRows: 1000, Members: 4}, searchplugin.Round{ITopK: 20}, 4)
	require.False(t, more)
	// every indexed row requested: stop
	_, more = p.NextRound(nil, searchplugin.Request{ResultLimit: 10}, searchplugin.SearchStats{IndexRows: 50, Members: 500}, searchplugin.Round{ITopK: 50}, 3)
	require.False(t, more)
	// the default cap applies without max_itopk
	next, more = p.NextRound(nil, searchplugin.Request{ResultLimit: 10}, searchplugin.SearchStats{}, searchplugin.Round{ITopK: DefaultMaxInternalTopK/2 + 1}, 3)
	require.True(t, more)
	require.Equal(t, uint64(DefaultMaxInternalTopK), next.ITopK)
}
