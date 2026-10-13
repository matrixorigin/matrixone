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

package search

import (
	"testing"

	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	"github.com/stretchr/testify/require"
)

func TestHooksRejectIncompleteExecutionContext(t *testing.T) {
	_, err := (Hooks{}).NewReader(nil, &plan.IndexSearchScan{
		Index: &plan.IndexDef{IndexAlgo: "ivfflat"},
	}, searchplugin.Request{CandidateBudget: 1})
	require.Error(t, err)
}

func TestHooksParallelizeOnlySingleRoundScans(t *testing.T) {
	can, err := Hooks{}.CanParallelize(&plan.IndexSearchScan{AlgoOptions: []byte(`{"initial_probe_count":4}`)})
	require.NoError(t, err)
	require.True(t, can)
	can, err = Hooks{}.CanParallelize(&plan.IndexSearchScan{AlgoOptions: []byte(`{"bucket_expand_step":4}`)})
	require.NoError(t, err)
	require.False(t, can)
	can, err = Hooks{}.CanParallelize(&plan.IndexSearchScan{AlgoExprNames: []string{"first_round_limit"}})
	require.NoError(t, err)
	require.False(t, can)
	_, err = Hooks{}.CanParallelize(&plan.IndexSearchScan{AlgoOptions: []byte(`{`)})
	require.Error(t, err)
}

func TestHooksExplainSettingsPreservesStaticSettings(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts ivfflatplan.ScanOptions
		want []string
	}{
		{"zero", ivfflatplan.ScanOptions{}, []string{"NProbe: 0", "BucketExpandStep: 0", "ThreadsSearch: 0"}},
		{"probe only", ivfflatplan.ScanOptions{InitialProbeCount: 4}, []string{"NProbe: 4", "BucketExpandStep: 0", "ThreadsSearch: 0"}},
		{"expansion only", ivfflatplan.ScanOptions{BucketExpandStep: 8}, []string{"NProbe: 0", "BucketExpandStep: 8", "ThreadsSearch: 0"}},
		{"threads only", ivfflatplan.ScanOptions{ThreadsSearch: 3}, []string{"NProbe: 0", "BucketExpandStep: 0", "ThreadsSearch: 3"}},
		{"integer boundaries", ivfflatplan.ScanOptions{InitialProbeCount: 4294967295, BucketExpandStep: 4294967295, ThreadsSearch: 9223372036854775807}, []string{"NProbe: 4294967295", "BucketExpandStep: 4294967295", "ThreadsSearch: 9223372036854775807"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := ivfflatplan.EncodeScanOptions(tc.opts)
			require.NoError(t, err)
			before := append([]byte(nil), data...)
			spec := &plan.IndexSearchScan{AlgoOptions: data}
			detailed, err := Hooks{}.ExplainSettings(spec, true)
			require.NoError(t, err)
			require.Equal(t, tc.want, detailed)
			compact, err := Hooks{}.ExplainSettings(spec, false)
			require.NoError(t, err)
			require.Equal(t, tc.want[:1], compact)
			require.Equal(t, before, spec.AlgoOptions)
		})
	}
	for _, detailed := range []bool{false, true} {
		for _, spec := range []*plan.IndexSearchScan{nil, {}} {
			settings, err := Hooks{}.ExplainSettings(spec, detailed)
			require.NoError(t, err)
			want := []string{"NProbe: 0"}
			if detailed {
				want = append(want, "BucketExpandStep: 0", "ThreadsSearch: 0")
			}
			require.Equal(t, want, settings)
		}
		settings, err := Hooks{}.ExplainSettings(&plan.IndexSearchScan{AlgoOptions: []byte("{")}, detailed)
		require.Error(t, err)
		require.Nil(t, settings)
	}
}
