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

func TestHooksExplainSettingsShowsProbeCount(t *testing.T) {
	settings, err := Hooks{}.ExplainSettings(&plan.IndexSearchScan{AlgoOptions: []byte(`{"initial_probe_count":4}`)})
	require.NoError(t, err)
	require.Equal(t, []string{"NProbe: 4"}, settings)
	settings, err = Hooks{}.ExplainSettings(&plan.IndexSearchScan{})
	require.NoError(t, err)
	require.Equal(t, []string{"NProbe: 0"}, settings)
	_, err = Hooks{}.ExplainSettings(&plan.IndexSearchScan{AlgoOptions: []byte(`{`)})
	require.Error(t, err)
}
