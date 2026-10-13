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

	// unknown lists: max_nprobe alone caps, else rounds double
	require.Equal(t, uint64(5), iterativeListCap(searchplugin.Request{MaxNprobe: 5}, searchplugin.SearchStats{}))
	r, more = p.NextRound(spec, req, searchplugin.SearchStats{}, searchplugin.Round{NProbe: 3}, 2)
	require.True(t, more)
	require.Equal(t, uint64(6), r.NProbe)
	require.Equal(t, uint64(1), p.StartRound(nil, req, stats).NProbe)
}
