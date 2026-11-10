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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

func TestScanOptionsRoundTrip(t *testing.T) {
	for _, opts := range []ScanOptions{{}, {ThreadsSearch: 2, BatchWindow: 64, GpuMultiSimulation: 2, KeyPartType: int32(types.T_array_float16), FilterJSON: `[{"op":"="}]`}} {
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
	require.ErrorContains(t, err, "invalid cagra scan options")
}

func TestSearchIndexConfig(t *testing.T) {
	cfg, err := SearchIndexConfig(`{"op_type":"vector_l2_ops","intermediate_graph_degree":"128","graph_degree":"64","itopk_size":"32","distribution_mode":"sharded","quantization":"int8"}`)
	require.NoError(t, err)
	require.Equal(t, vectorindex.CAGRA, cfg.Type)
	require.Equal(t, "vector_l2_ops", cfg.OpType)
	require.Equal(t, uint16(metric.Metric_L2sqDistance), cfg.CuvsCagra.Metric)
	require.Equal(t, uint64(128), cfg.CuvsCagra.IntermediateGraphDegree)
	require.Equal(t, uint64(64), cfg.CuvsCagra.GraphDegree)
	require.Equal(t, uint64(32), cfg.CuvsCagra.ITopkSize)
	require.Equal(t, uint16(vectorindex.DistributionMode_SHARDED), cfg.CuvsCagra.DistributionMode)
	require.Equal(t, uint16(metric.Quantization_INT8), cfg.CuvsCagra.Quantization)

	cfg, err = SearchIndexConfig(`{"op_type":"vector_ip_ops","distribution_mode":"replicated","quantization":"float16"}`)
	require.NoError(t, err)
	require.Equal(t, uint16(vectorindex.DistributionMode_REPLICATED), cfg.CuvsCagra.DistributionMode)
	require.Equal(t, uint16(metric.Quantization_F16), cfg.CuvsCagra.Quantization)

	cfg, err = SearchIndexConfig(`{"op_type":"vector_l2_ops","quantization":"uint8"}`)
	require.NoError(t, err)
	require.Equal(t, uint16(vectorindex.DistributionMode_SINGLE_GPU), cfg.CuvsCagra.DistributionMode)
	require.Equal(t, uint16(metric.Quantization_UINT8), cfg.CuvsCagra.Quantization)

	cfg, err = SearchIndexConfig(`{"op_type":"vector_l2_ops"}`)
	require.NoError(t, err)
	require.Equal(t, uint16(metric.Quantization_F32), cfg.CuvsCagra.Quantization)

	_, err = SearchIndexConfig("")
	require.Error(t, err)
	_, err = SearchIndexConfig(`{`)
	require.Error(t, err)
	_, err = SearchIndexConfig(`{"op_type":"bogus"}`)
	require.Error(t, err)
	_, err = SearchIndexConfig(`{"op_type":"vector_l2_ops","graph_degree":"x"}`)
	require.Error(t, err)
}
