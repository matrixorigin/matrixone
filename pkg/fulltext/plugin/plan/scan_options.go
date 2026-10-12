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

// ScanOptions are the static classic fulltext settings an IndexSearchScan
// carries in algo_options.
type ScanOptions struct {
	// SourceTable is the quoted, qualified name of the source table.
	SourceTable string `json:"source_table"`
	// IndexTable is the quoted, qualified name of the index table.
	IndexTable string `json:"index_table"`
	// Mode is the MATCH search mode.
	Mode int64 `json:"mode"`
}

// EncodeScanOptions returns the algo_options bytes of opts.
func EncodeScanOptions(opts ScanOptions) ([]byte, error) {
	return sonic.Marshal(opts)
}

// DecodeScanOptions returns the ScanOptions of algo_options bytes.
func DecodeScanOptions(data []byte) (ScanOptions, error) {
	var opts ScanOptions
	if len(data) == 0 {
		return opts, moerr.NewInvalidInputNoCtx("fulltext index search has no scan options")
	}
	if err := sonic.Unmarshal(data, &opts); err != nil {
		return opts, moerr.NewInvalidInputNoCtxf("invalid fulltext scan options: %v", err)
	}
	return opts, nil
}
