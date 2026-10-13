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

package vectorscan

import (
	"testing"

	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestStampSessionKnobs(t *testing.T) {
	session := map[string]any{SessionVarITopK: int64(100), SessionVarMaxITopK: int64(1000), SessionVarMaxNprobe: int64(32)}
	resolve := func(name string, _, _ bool) (any, error) { return session[name], nil }

	spec := &plan.IndexSearchScan{}
	require.NoError(t, StampSessionKnobs(spec, resolve))
	require.Equal(t, uint64(100), spec.InternalTopk)
	require.Equal(t, uint64(1000), spec.MaxInternalTopk)
	require.Equal(t, uint64(32), spec.MaxNprobe)

	// a BY RANK option wins over the session variable
	spec = &plan.IndexSearchScan{InternalTopk: 7, MaxNprobe: 9}
	require.NoError(t, StampSessionKnobs(spec, resolve))
	require.Equal(t, uint64(7), spec.InternalTopk)
	require.Equal(t, uint64(1000), spec.MaxInternalTopk)
	require.Equal(t, uint64(9), spec.MaxNprobe)

	// no resolver (a CN without a session) and unset variables leave spec as is
	spec = &plan.IndexSearchScan{}
	require.NoError(t, StampSessionKnobs(spec, nil))
	require.NoError(t, StampSessionKnobs(spec, func(string, bool, bool) (any, error) { return nil, nil }))
	require.Equal(t, &plan.IndexSearchScan{}, spec)
	require.NoError(t, StampSessionKnobs(nil, resolve))

	require.Error(t, StampSessionKnobs(&plan.IndexSearchScan{}, func(string, bool, bool) (any, error) { return "x", nil }))
	require.Error(t, StampSessionKnobs(&plan.IndexSearchScan{}, func(string, bool, bool) (any, error) { return int64(-1), nil }))
}

func TestApplySearchKnobs(t *testing.T) {
	// unset knobs keep the existing candidate budget
	req := searchplugin.Request{ResultLimit: 10, CandidateBudget: 20}
	require.NoError(t, ApplySearchKnobs(&req, &plan.IndexSearchScan{}, nil))
	require.Equal(t, uint64(20), req.CandidateBudget)
	require.Zero(t, req.InternalTopK)

	// itopk is the budget, at least the result limit, at most its cap
	for _, tc := range []struct {
		itopk, maxITopK, limit, want uint64
	}{
		{200, 0, 10, 200},
		{5, 0, 10, 10},
		{200, 50, 10, 50},
		{200, 5, 10, 10},
	} {
		req := searchplugin.Request{ResultLimit: tc.limit, CandidateBudget: 20}
		require.NoError(t, ApplySearchKnobs(&req, &plan.IndexSearchScan{InternalTopk: tc.itopk, MaxInternalTopk: tc.maxITopK, MaxNprobe: 64}, nil))
		require.Equal(t, tc.want, req.CandidateBudget, "%+v", tc)
		require.Equal(t, tc.maxITopK, req.MaxInternalTopK)
		require.Equal(t, uint64(64), req.MaxNprobe)
	}
}

func TestApplySearchKnobsSessionFallback(t *testing.T) {
	session := map[string]any{SessionVarITopK: int64(300), SessionVarMaxITopK: int64(0), SessionVarMaxNprobe: int64(16)}
	resolve := func(name string, _, _ bool) (any, error) { return session[name], nil }
	spec := &plan.IndexSearchScan{}
	req := searchplugin.Request{ResultLimit: 10, CandidateBudget: 20}
	require.NoError(t, ApplySearchKnobs(&req, spec, resolve))
	require.Equal(t, uint64(300), req.CandidateBudget)
	require.Equal(t, uint64(16), req.MaxNprobe)
	require.Equal(t, &plan.IndexSearchScan{}, spec, "the spec is not changed")

	// the BY RANK option wins over the session variable
	req = searchplugin.Request{ResultLimit: 10, CandidateBudget: 20}
	require.NoError(t, ApplySearchKnobs(&req, &plan.IndexSearchScan{InternalTopk: 50}, resolve))
	require.Equal(t, uint64(50), req.CandidateBudget)

	// a later session value applies to the same spec
	session[SessionVarITopK] = int64(0)
	req = searchplugin.Request{ResultLimit: 10, CandidateBudget: 20}
	require.NoError(t, ApplySearchKnobs(&req, spec, resolve))
	require.Equal(t, uint64(20), req.CandidateBudget)

	require.Error(t, ApplySearchKnobs(&searchplugin.Request{}, spec, func(string, bool, bool) (any, error) { return "x", nil }))
}

func TestRequiredProtocolVersion(t *testing.T) {
	require.Equal(t, IndexSearchScanProtocolVersion, RequiredProtocolVersion(&plan.IndexSearchScan{}))
	require.Equal(t, IndexSearchScanProtocolVersion, RequiredProtocolVersion(nil))
	for _, spec := range []*plan.IndexSearchScan{{InternalTopk: 1}, {MaxInternalTopk: 1}, {MaxNprobe: 1}} {
		require.Equal(t, SearchKnobsProtocolVersion, RequiredProtocolVersion(spec))
	}
	require.Greater(t, SearchKnobsProtocolVersion, IndexSearchScanProtocolVersion)
}
