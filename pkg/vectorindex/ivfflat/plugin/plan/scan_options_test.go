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

package plan

import (
	"testing"

	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestScanOptionsRoundTrip(t *testing.T) {
	for _, opts := range []ScanOptions{{}, {InitialProbeCount: 5, BucketExpandStep: 5, ThreadsSearch: 3}} {
		data, err := EncodeScanOptions(opts)
		require.NoError(t, err)
		got, err := DecodeScanOptions(data)
		require.NoError(t, err)
		require.Equal(t, opts, got)
	}
	got, err := DecodeScanOptions(nil)
	require.NoError(t, err)
	require.Zero(t, got)
	_, err = DecodeScanOptions([]byte(`{`))
	require.ErrorContains(t, err, "invalid ivfflat scan options")
}

func TestMultiRound(t *testing.T) {
	multi, err := MultiRound(nil)
	require.NoError(t, err)
	require.False(t, multi)
	multi, err = MultiRound(&plan.IndexSearchScan{})
	require.NoError(t, err)
	require.False(t, multi)
	multi, err = MultiRound(&plan.IndexSearchScan{AlgoExprNames: []string{FirstRoundLimitExpr}})
	require.NoError(t, err)
	require.True(t, multi)
	multi, err = MultiRound(&plan.IndexSearchScan{AlgoOptions: []byte(`{"bucket_expand_step":2}`)})
	require.NoError(t, err)
	require.True(t, multi)
	_, err = MultiRound(&plan.IndexSearchScan{AlgoOptions: []byte(`{`)})
	require.Error(t, err)
}

func TestFirstRoundLimit(t *testing.T) {
	limit, ok, err := FirstRoundLimit(searchplugin.Request{})
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, limit)

	value := func(lit *plan.Literal) searchplugin.Request {
		return searchplugin.Request{AlgoValues: []searchplugin.AlgoValue{{Name: FirstRoundLimitExpr, Value: lit}}}
	}
	limit, ok, err = FirstRoundLimit(value(&plan.Literal{Value: &plan.Literal_U64Val{U64Val: 9}}))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(9), limit)
	for _, bad := range []*plan.Literal{
		{Isnull: true},
		{Value: &plan.Literal_I64Val{I64Val: 9}},
	} {
		_, _, err = FirstRoundLimit(value(bad))
		require.ErrorContains(t, err, "first-round limit is not uint64")
	}
}
