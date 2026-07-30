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

package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdapterFeasibilityLookupsCloneResults(t *testing.T) {
	nonREST := NonRESTAdapterFeasibility()
	require.NotEmpty(t, nonREST)
	require.Equal(t, "glue", nonREST[0].Name)
	nonREST[0].CatalogTypes[0] = "mutated"
	require.Equal(t, "glue", NonRESTAdapterFeasibility()[0].CatalogTypes[0])

	icebergGo := IcebergGoAdapterFeasibility()
	require.Equal(t, AdapterIcebergGo, icebergGo.Name)
	require.True(t, icebergGo.RequiresBuildTag)
	icebergGo.CatalogTypes[0] = "mutated"
	require.Equal(t, AdapterIcebergGo, IcebergGoAdapterFeasibility().CatalogTypes[0])
}

func TestAdapterFeasibilityForTypeNormalizesAliases(t *testing.T) {
	cases := []struct {
		in     string
		name   string
		status AdapterFeasibilityStatus
	}{
		{" AWS-Glue ", "glue", AdapterNeedsBridge},
		{"hms", "hive", AdapterNeedsBridge},
		{"UNITY-CATALOG", "unity", AdapterRESTPreferred},
		{"sql", AdapterIcebergGo, AdapterFeasibleViaFacade},
	}
	for _, tc := range cases {
		got, ok := AdapterFeasibilityForType(tc.in)
		require.True(t, ok, tc.in)
		require.Equal(t, tc.name, got.Name)
		require.Equal(t, tc.status, got.Status)
	}
	_, ok := AdapterFeasibilityForType("unknown")
	require.False(t, ok)
}
