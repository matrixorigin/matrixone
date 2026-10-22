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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin"
	"github.com/stretchr/testify/require"
)

func TestStampSessionKnobs(t *testing.T) {
	session := map[string]any{SessionVarITopK: int64(100), SessionVarMaxITopK: int64(1000), SessionVarMaxNprobe: int64(32)}
	resolve := func(name string, _, _ bool) (any, error) { return session[name], nil }
	ivfflat := &plan.IndexDef{IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString()}

	spec := &plan.IndexSearchScan{Index: ivfflat}
	require.NoError(t, StampSessionKnobs(spec, resolve))
	require.Equal(t, uint64(100), spec.InternalTopk)
	require.Equal(t, uint64(1000), spec.MaxInternalTopk)
	require.Equal(t, uint64(32), spec.MaxNprobe)

	// a BY RANK option wins over the session variable
	spec = &plan.IndexSearchScan{Index: ivfflat, InternalTopk: 7, MaxNprobe: 9}
	require.NoError(t, StampSessionKnobs(spec, resolve))
	require.Equal(t, uint64(7), spec.InternalTopk)
	require.Equal(t, uint64(1000), spec.MaxInternalTopk)
	require.Equal(t, uint64(9), spec.MaxNprobe)

	// an index that probes no lists takes no max_nprobe; a non-vector index no knob
	spec = &plan.IndexSearchScan{Index: &plan.IndexDef{IndexAlgo: catalog.MoIndexHnswAlgo.ToString()}}
	require.NoError(t, StampSessionKnobs(spec, resolve))
	require.Equal(t, uint64(100), spec.InternalTopk)
	require.Zero(t, spec.MaxNprobe)
	spec = &plan.IndexSearchScan{Index: &plan.IndexDef{IndexAlgo: catalog.MOIndexFullTextAlgo.ToString()}}
	require.NoError(t, StampSessionKnobs(spec, resolve))
	require.Zero(t, spec.InternalTopk)
	require.Zero(t, spec.MaxInternalTopk)
	require.Zero(t, spec.MaxNprobe)
	require.Equal(t, int64(IndexSearchScanProtocolVersion), RequiredProtocolVersion(spec))

	// no resolver (a CN without a session) and unset variables leave spec as is
	spec = &plan.IndexSearchScan{Index: ivfflat}
	require.NoError(t, StampSessionKnobs(spec, nil))
	require.NoError(t, StampSessionKnobs(spec, func(string, bool, bool) (any, error) { return nil, nil }))
	require.Equal(t, &plan.IndexSearchScan{Index: ivfflat}, spec)
	require.NoError(t, StampSessionKnobs(nil, resolve))

	require.Error(t, StampSessionKnobs(&plan.IndexSearchScan{Index: ivfflat}, func(string, bool, bool) (any, error) { return "x", nil }))
	require.Error(t, StampSessionKnobs(&plan.IndexSearchScan{Index: ivfflat}, func(string, bool, bool) (any, error) { return int64(-1), nil }))
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
	ivfflat := &plan.IndexDef{IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString()}
	spec := &plan.IndexSearchScan{Index: ivfflat}
	req := searchplugin.Request{ResultLimit: 10, CandidateBudget: 20}
	require.NoError(t, ApplySearchKnobs(&req, spec, resolve))
	require.Equal(t, uint64(300), req.CandidateBudget)
	require.Equal(t, uint64(16), req.MaxNprobe)
	require.Equal(t, &plan.IndexSearchScan{Index: ivfflat}, spec, "the spec is not changed")

	// the BY RANK option wins over the session variable
	req = searchplugin.Request{ResultLimit: 10, CandidateBudget: 20}
	require.NoError(t, ApplySearchKnobs(&req, &plan.IndexSearchScan{Index: ivfflat, InternalTopk: 50}, resolve))
	require.Equal(t, uint64(50), req.CandidateBudget)

	// a later session value applies to the same spec
	session[SessionVarITopK] = int64(0)
	req = searchplugin.Request{ResultLimit: 10, CandidateBudget: 20}
	require.NoError(t, ApplySearchKnobs(&req, spec, resolve))
	require.Equal(t, uint64(20), req.CandidateBudget)

	require.Error(t, ApplySearchKnobs(&searchplugin.Request{}, spec, func(string, bool, bool) (any, error) { return "x", nil }))
}

func TestApplySearchKnobsInternalTopKLimit(t *testing.T) {
	cagra := &plan.IndexDef{IndexAlgo: catalog.MoIndexCagraAlgo.ToString()}
	hnsw := &plan.IndexDef{IndexAlgo: catalog.MoIndexHnswAlgo.ToString()}
	session := map[string]any{SessionVarITopK: int64(0), SessionVarMaxITopK: int64(0)}
	resolve := func(name string, _, _ bool) (any, error) { return session[name], nil }

	req := searchplugin.Request{ResultLimit: 10}
	require.NoError(t, ApplySearchKnobs(&req, &plan.IndexSearchScan{Index: cagra, InternalTopk: 1024, MaxInternalTopk: 1024}, resolve))
	require.Equal(t, uint64(1024), req.CandidateBudget)

	for _, spec := range []*plan.IndexSearchScan{
		{Index: cagra, InternalTopk: 1025},
		{Index: cagra, MaxInternalTopk: 1025},
	} {
		err := ApplySearchKnobs(&searchplugin.Request{ResultLimit: 10}, spec, resolve)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
		require.ErrorContains(t, err, "exceeds the cagra limit 1024")
	}

	// session variables are lowered to the limit, locally and on the stamped copy
	session[SessionVarITopK] = int64(2000)
	req = searchplugin.Request{ResultLimit: 10}
	require.NoError(t, ApplySearchKnobs(&req, &plan.IndexSearchScan{Index: cagra}, resolve))
	require.Equal(t, uint64(1024), req.InternalTopK)
	require.Equal(t, uint64(1024), req.CandidateBudget)
	session[SessionVarITopK] = int64(0)
	session[SessionVarMaxITopK] = int64(5000)
	req = searchplugin.Request{ResultLimit: 10}
	require.NoError(t, ApplySearchKnobs(&req, &plan.IndexSearchScan{Index: cagra}, resolve))
	require.Equal(t, uint64(1024), req.MaxInternalTopK)
	session[SessionVarITopK] = int64(2000)
	stamped := &plan.IndexSearchScan{Index: cagra}
	require.NoError(t, StampSessionKnobs(stamped, resolve))
	require.Equal(t, uint64(1024), stamped.InternalTopk)
	require.Equal(t, uint64(1024), stamped.MaxInternalTopk)
	// a BY RANK value is not lowered: it is an error with the session set too
	err := ApplySearchKnobs(&searchplugin.Request{ResultLimit: 10}, &plan.IndexSearchScan{Index: cagra, InternalTopk: 1025}, resolve)
	require.ErrorContains(t, err, "itopk 1025 exceeds the cagra limit 1024")

	// hnsw keeps the session value
	stamped = &plan.IndexSearchScan{Index: hnsw}
	require.NoError(t, StampSessionKnobs(stamped, resolve))
	require.Equal(t, uint64(2000), stamped.InternalTopk)
	require.Equal(t, uint64(5000), stamped.MaxInternalTopk)
	session[SessionVarITopK] = int64(0)
	session[SessionVarMaxITopK] = int64(0)

	// hnsw has no limit
	require.NoError(t, ApplySearchKnobs(&searchplugin.Request{ResultLimit: 10}, &plan.IndexSearchScan{Index: hnsw, InternalTopk: 20000}, resolve))
}

func TestRequiredProtocolVersion(t *testing.T) {
	require.Equal(t, IndexSearchScanProtocolVersion, RequiredProtocolVersion(&plan.IndexSearchScan{}))
	require.Equal(t, IndexSearchScanProtocolVersion, RequiredProtocolVersion(nil))
	for _, spec := range []*plan.IndexSearchScan{{InternalTopk: 1}, {MaxInternalTopk: 1}, {MaxNprobe: 1}} {
		require.Equal(t, SearchKnobsProtocolVersion, RequiredProtocolVersion(spec))
	}
	require.Greater(t, SearchKnobsProtocolVersion, IndexSearchScanProtocolVersion)
}
