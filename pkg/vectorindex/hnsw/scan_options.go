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

package hnsw

import (
	"encoding/json"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// ScanOptions are the static hnsw settings an IndexSearchScan carries in
// algo_options.
type ScanOptions struct {
	ThreadsSearch int64 `json:"threads_search,omitempty"`
}

// EncodeScanOptions returns the algo_options bytes of opts.
func EncodeScanOptions(opts ScanOptions) ([]byte, error) {
	return json.Marshal(opts)
}

// DecodeScanOptions returns the ScanOptions of algo_options bytes; empty bytes
// are the zero ScanOptions.
func DecodeScanOptions(data []byte) (ScanOptions, error) {
	var opts ScanOptions
	if len(data) == 0 {
		return opts, nil
	}
	if err := json.Unmarshal(data, &opts); err != nil {
		return opts, moerr.NewInvalidInputNoCtxf("invalid hnsw scan options: %v", err)
	}
	return opts, nil
}

// SearchIndexConfig returns the usearch settings of an hnsw index from its
// IndexAlgoParams JSON; quantization and dimensions come from the query.
func SearchIndexConfig(algoParams string) (vectorindex.IndexConfig, error) {
	var idxcfg vectorindex.IndexConfig
	var param vectorindex.HnswParam
	if len(algoParams) > 0 {
		if err := json.Unmarshal([]byte(algoParams), &param); err != nil {
			return idxcfg, err
		}
	}
	if len(param.M) > 0 {
		val, err := strconv.Atoi(param.M)
		if err != nil {
			return idxcfg, err
		}
		idxcfg.Usearch.Connectivity = uint(val)
	}
	metrictype, ok := metric.OpTypeToUsearchMetric[param.OpType]
	if !ok {
		return idxcfg, moerr.NewInternalErrorNoCtx("Invalid op_type")
	}
	idxcfg.OpType = param.OpType
	idxcfg.Usearch.Metric = metrictype
	if len(param.EfConstruction) > 0 {
		val, err := strconv.Atoi(param.EfConstruction)
		if err != nil {
			return idxcfg, err
		}
		idxcfg.Usearch.ExpansionAdd = uint(val)
	}
	if len(param.EfSearch) > 0 {
		val, err := strconv.Atoi(param.EfSearch)
		if err != nil {
			return idxcfg, err
		}
		idxcfg.Usearch.ExpansionSearch = uint(val)
	}
	idxcfg.Type = vectorindex.HNSW
	return idxcfg, nil
}
