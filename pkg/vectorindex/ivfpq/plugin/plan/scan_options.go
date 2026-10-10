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
	"strconv"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// ScanOptions are the static ivfpq settings an IndexSearchScan carries in
// algo_options.
type ScanOptions struct {
	ThreadsSearch      int64 `json:"threads_search,omitempty"`
	BatchWindow        int64 `json:"batch_window,omitempty"`
	GpuMultiSimulation int64 `json:"gpu_multi_simulation,omitempty"`
	// Nprobe is the number of lists a search probes.
	Nprobe uint `json:"nprobe,omitempty"`
	// KeyPartType is the types.T of the indexed vector column.
	KeyPartType int32 `json:"parttype,omitempty"`
	// FilterJSON holds the INCLUDE and primary-key predicates the search
	// applies on the device; empty means unfiltered.
	FilterJSON string `json:"filter_json,omitempty"`
}

// EncodeScanOptions returns the algo_options bytes of opts.
func EncodeScanOptions(opts ScanOptions) ([]byte, error) {
	return sonic.Marshal(opts)
}

// DecodeScanOptions returns the ScanOptions of algo_options bytes; empty bytes
// are the zero ScanOptions.
func DecodeScanOptions(data []byte) (ScanOptions, error) {
	var opts ScanOptions
	if len(data) == 0 {
		return opts, nil
	}
	if err := sonic.Unmarshal(data, &opts); err != nil {
		return opts, moerr.NewInvalidInputNoCtxf("invalid ivfpq scan options: %v", err)
	}
	return opts, nil
}

// SearchIndexConfig returns the cuVS IVF-PQ settings of an index from its
// IndexAlgoParams JSON; dimensions come from the query.
func SearchIndexConfig(algoParams string) (vectorindex.IndexConfig, error) {
	var idxcfg vectorindex.IndexConfig
	var param vectorindex.IvfpqParam
	if len(algoParams) > 0 {
		if err := sonic.Unmarshal([]byte(algoParams), &param); err != nil {
			return idxcfg, err
		}
	}
	metricType, ok := metric.OpTypeToIvfMetric[param.OpType]
	if !ok {
		return idxcfg, moerr.NewInternalErrorNoCtx("invalid op_type for IVF-PQ")
	}
	idxcfg.CuvsIvfpq.Metric = uint16(metricType)
	idxcfg.OpType = param.OpType
	for _, p := range []struct {
		text string
		dst  *uint
	}{
		{param.Lists, &idxcfg.CuvsIvfpq.Lists},
		{param.M, &idxcfg.CuvsIvfpq.M},
		{param.BitsPerCode, &idxcfg.CuvsIvfpq.BitsPerCode},
	} {
		if len(p.text) == 0 {
			continue
		}
		val, err := strconv.ParseUint(p.text, 10, 64)
		if err != nil {
			return idxcfg, err
		}
		*p.dst = uint(val)
	}
	switch param.Distribution {
	case vectorindex.DistributionMode_REPLICATED_Str:
		idxcfg.CuvsIvfpq.DistributionMode = uint16(vectorindex.DistributionMode_REPLICATED)
	case vectorindex.DistributionMode_SHARDED_Str:
		idxcfg.CuvsIvfpq.DistributionMode = uint16(vectorindex.DistributionMode_SHARDED)
	default:
		idxcfg.CuvsIvfpq.DistributionMode = uint16(vectorindex.DistributionMode_SINGLE_GPU)
	}
	switch param.Quantization {
	case metric.Quantization_F16_Str:
		idxcfg.CuvsIvfpq.Quantization = uint16(metric.Quantization_F16)
	case metric.Quantization_INT8_Str:
		idxcfg.CuvsIvfpq.Quantization = uint16(metric.Quantization_INT8)
	case metric.Quantization_UINT8_Str:
		idxcfg.CuvsIvfpq.Quantization = uint16(metric.Quantization_UINT8)
	default:
		idxcfg.CuvsIvfpq.Quantization = uint16(metric.Quantization_F32)
	}
	idxcfg.Type = vectorindex.IVFPQ
	return idxcfg, nil
}
