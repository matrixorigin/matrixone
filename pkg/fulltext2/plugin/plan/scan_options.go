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
	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ScanOptions are the static fulltext2 settings an IndexSearchScan carries in
// algo_options.
type ScanOptions struct {
	// Config is the fulltext2.TableConfig JSON of the index.
	Config string `json:"config"`
	// Mode is the MATCH search mode.
	Mode int64 `json:"mode"`
	// IncludePreds is the INCLUDE / primary-key predicate JSON applied inside
	// the search; empty means none.
	IncludePreds string `json:"include_preds,omitempty"`
	// ScoreRange is the fulltext2.ScoreRange JSON of the pushed score bounds;
	// empty means none.
	ScoreRange string `json:"score_range,omitempty"`
	// ThreadsSearch is the per-query degree of parallelism for the multi-segment
	// top-k search, resolved from the fulltext2_threads_search session variable at
	// plan time (0 = use GOMAXPROCS, see vectorindex.GetConcurrency). omitempty so
	// an older CN that lacks the field simply ignores it (serial search).
	ThreadsSearch int64 `json:"threads_search,omitempty"`
	// SearchDFCutoff drops a ranked bag-of-words term from the BM25 search when its
	// global df/N reaches this fraction, resolved from the fulltext2_search_df_cutoff
	// session variable at plan time (0 = off, the default). omitempty so an older CN
	// that lacks the field simply ignores it (no cutoff).
	SearchDFCutoff float64 `json:"search_df_cutoff,omitempty"`
}

// EncodeScanOptions returns the algo_options bytes of opts.
func EncodeScanOptions(opts ScanOptions) ([]byte, error) {
	return sonic.Marshal(opts)
}

// DecodeScanOptions returns the ScanOptions of algo_options bytes.
func DecodeScanOptions(data []byte) (ScanOptions, error) {
	var opts ScanOptions
	if len(data) == 0 {
		return opts, moerr.NewInvalidInputNoCtx("fulltext2 index search has no scan options")
	}
	if err := sonic.Unmarshal(data, &opts); err != nil {
		return opts, moerr.NewInvalidInputNoCtxf("invalid fulltext2 scan options: %v", err)
	}
	return opts, nil
}
