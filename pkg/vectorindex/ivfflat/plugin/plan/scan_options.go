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
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// FirstRoundLimitExpr names the IndexSearchScan algorithm expression holding
// the first search round's limit of an INCLUDE-mode scan.
const FirstRoundLimitExpr = "first_round_limit"

// ScanOptions are the static ivfflat settings an IndexSearchScan carries in
// algo_options.
type ScanOptions struct {
	// InitialProbeCount is the number of centroid buckets of the first round.
	InitialProbeCount uint32 `json:"initial_probe_count,omitempty"`
	// BucketExpandStep is the number of buckets each further round adds; zero
	// means one round.
	BucketExpandStep uint32 `json:"bucket_expand_step,omitempty"`
	ThreadsSearch    int64  `json:"threads_search,omitempty"`
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
		return opts, moerr.NewInvalidInputNoCtxf("invalid ivfflat scan options: %v", err)
	}
	return opts, nil
}

// MultiRound reports whether spec runs more than one search round.
func MultiRound(spec *plan.IndexSearchScan) (bool, error) {
	if spec == nil {
		return false, nil
	}
	for _, name := range spec.AlgoExprNames {
		if name == FirstRoundLimitExpr {
			return true, nil
		}
	}
	opts, err := DecodeScanOptions(spec.AlgoOptions)
	return opts.BucketExpandStep > 0, err
}

// FirstRoundLimit returns the evaluated first-round limit of req.
func FirstRoundLimit(req searchplugin.Request) (limit uint64, ok bool, err error) {
	lit, ok := req.AlgoValue(FirstRoundLimitExpr)
	if !ok {
		return 0, false, nil
	}
	value, isU64 := lit.Value.(*plan.Literal_U64Val)
	if lit.Isnull || !isU64 {
		return 0, false, moerr.NewInvalidInputNoCtx("vector index first-round limit is not uint64")
	}
	return value.U64Val, true, nil
}
