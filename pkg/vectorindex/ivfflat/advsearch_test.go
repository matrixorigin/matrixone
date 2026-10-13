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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	"github.com/stretchr/testify/require"
)

func advSpec(t *testing.T, nprobe uint32) *plan.IndexSearchScan {
	opts, err := ivfflatplan.EncodeScanOptions(ivfflatplan.ScanOptions{InitialProbeCount: nprobe})
	require.NoError(t, err)
	return &plan.IndexSearchScan{AlgoOptions: opts}
}

func TestAdvancedSearchStartRound(t *testing.T) {
	p := AdvancedSearch{}
	spec := advSpec(t, 2)
	req := searchplugin.Request{ResultLimit: 10, CandidateBudget: 15}

	// without a membership count the start is nprobe and the round requests the budget
	r := p.StartRound(spec, req, searchplugin.SearchStats{Lists: 100})
	require.Equal(t, searchplugin.Round{ITopK: 15, NProbe: 2}, r)

	// a membership count raises nprobe to lists·k/|S| times the safety factor
	r = p.StartRound(spec, req, searchplugin.SearchStats{Lists: 100, Members: 500})
	require.Equal(t, uint64(6), r.NProbe)

	// at most the lists, and at most max_nprobe
	r = p.StartRound(spec, req, searchplugin.SearchStats{Lists: 100, Members: 1})
	require.Equal(t, uint64(100), r.NProbe)
	req.MaxNprobe = 4
	r = p.StartRound(spec, req, searchplugin.SearchStats{Lists: 100, Members: 1})
	require.Equal(t, uint64(4), r.NProbe)

	// a missing scan option starts at one list
	r = p.StartRound(&plan.IndexSearchScan{}, searchplugin.Request{CandidateBudget: 3}, searchplugin.SearchStats{})
	require.Equal(t, searchplugin.Round{ITopK: 3, NProbe: 1}, r)
}

func TestAdvancedSearchNextRound(t *testing.T) {
	p := AdvancedSearch{}
	spec := advSpec(t, 2)
	req := searchplugin.Request{ResultLimit: 10, CandidateBudget: 10}
	prev := searchplugin.Round{ITopK: 10, NProbe: 2}

	// doubles the lists of the previous round
	next, more := p.NextRound(spec, req, searchplugin.SearchStats{Lists: 10, Probed: 2}, prev, 3)
	require.True(t, more)
	require.Equal(t, searchplugin.Round{ITopK: 10, NProbe: 4}, next)

	// at most the lists left
	next, more = p.NextRound(spec, req, searchplugin.SearchStats{Lists: 10, Probed: 6}, next, 3)
	require.True(t, more)
	require.Equal(t, uint64(4), next.NProbe)

	// stops at the candidate budget, at the member count, at the lists, at max_nprobe
	_, more = p.NextRound(spec, req, searchplugin.SearchStats{Lists: 10, Probed: 2}, prev, 10)
	require.False(t, more)
	_, more = p.NextRound(spec, req, searchplugin.SearchStats{Lists: 10, Probed: 2, Members: 3}, prev, 3)
	require.False(t, more)
	_, more = p.NextRound(spec, req, searchplugin.SearchStats{Lists: 10, Probed: 10}, prev, 3)
	require.False(t, more)
	capped := req
	capped.MaxNprobe = 3
	next, more = p.NextRound(spec, capped, searchplugin.SearchStats{Lists: 10, Probed: 2}, prev, 3)
	require.True(t, more)
	require.Equal(t, uint64(1), next.NProbe)
	_, more = p.NextRound(spec, capped, searchplugin.SearchStats{Lists: 10, Probed: 3}, next, 3)
	require.False(t, more)

	// unknown lists: max_nprobe alone caps, else the cursor ends the search
	next, more = p.NextRound(spec, req, searchplugin.SearchStats{Probed: 2}, prev, 3)
	require.True(t, more)
	require.Equal(t, uint64(4), next.NProbe)
	_, more = p.NextRound(spec, capped, searchplugin.SearchStats{Probed: 3}, prev, 3)
	require.False(t, more)

	// a round never probes more than maxRoundLists
	next, _ = p.NextRound(spec, req, searchplugin.SearchStats{Lists: 1 << 20, Probed: 4096}, searchplugin.Round{NProbe: 4096}, 0)
	require.Equal(t, uint64(maxRoundLists), next.NProbe)
}

func TestExactMembershipCount(t *testing.T) {
	n, err := exactMembershipCount(nil, false)
	require.NoError(t, err)
	require.Zero(t, n)
	_, err = exactMembershipCount([]byte{1}, false)
	require.Error(t, err)

	mp := mpool.MustNewZero()
	payload := func(typ types.Type, appendKeys func(*vector.Vector)) []byte {
		v := vector.NewVec(typ)
		defer v.Free(mp)
		appendKeys(v)
		b, err := v.MarshalBinary()
		require.NoError(t, err)
		return b
	}
	ints := payload(types.T_int64.ToType(), func(v *vector.Vector) {
		require.NoError(t, vector.AppendFixedList(v, []int64{1, 2, 3}, nil, mp))
	})
	strs := payload(types.T_varchar.ToType(), func(v *vector.Vector) {
		require.NoError(t, vector.AppendBytes(v, []byte("a"), false, mp))
		require.NoError(t, vector.AppendBytes(v, []byte("b"), false, mp))
	})
	// integer keys are an exact set; other keys build a bloom filter unless required
	n, err = exactMembershipCount(ints, false)
	require.NoError(t, err)
	require.Equal(t, uint64(3), n)
	n, err = exactMembershipCount(strs, false)
	require.NoError(t, err)
	require.Zero(t, n)
	n, err = exactMembershipCount(strs, true)
	require.NoError(t, err)
	require.Equal(t, uint64(2), n)
}

func TestPlanReaderWidensMembership(t *testing.T) {
	single := &planSearchGeneration{parallelism: 1}
	parallel := &planSearchGeneration{parallelism: 2}
	for _, tc := range []struct {
		name       string
		member     bool
		partitions int32
		generation *planSearchGeneration
		widens     bool
	}{
		{"no membership", false, 0, nil, false},
		{"one reader", true, 0, nil, true},
		{"one reader of a generation", true, 1, single, true},
		{"local readers of a generation", true, 1, parallel, false},
		{"one partition", true, 2, nil, false},
		{"one partition of a generation", true, 2, single, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &planReader{generation: tc.generation}
			r.req.HasMembershipFilter = tc.member
			r.req.Identity.PartitionCount = tc.partitions
			require.Equal(t, tc.widens, r.widensMembership())
		})
	}
}
