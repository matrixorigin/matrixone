// Copyright 2022 Matrix Origin
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
	vimemory "github.com/matrixorigin/matrixone/pkg/vectorindex/memory"
	usearch "github.com/unum-cloud/usearch/golang"
)

// defaultConnectivity is the M usearch uses when the index sets none.
const defaultConnectivity = 16

// unmeasuredCapacity is the model capacity used when host memory cannot be measured.
const unmeasuredCapacity = int64(1000000)

// rowOverheadBytes is the per-row cost of a model besides its vector and links: the key, the
// node header and level, and the key and slot lookup tables.
const rowOverheadBytes = 64

var hostRowsFitting = vimemory.HostRowsFitting

// ElementBytes is the size of one stored vector element.
func ElementBytes(kind usearch.Quantization) uint64 {
	switch kind {
	case usearch.F64:
		return 8
	case usearch.F16, usearch.BF16:
		return 2
	case usearch.I8, usearch.U8:
		return 1
	}
	return 4
}

// ModelBytesPerRow estimates the host memory one row costs in a model: the stored vector, its
// base-layer links (2·M), the expected upper-layer links (M per level, a node reaching level
// l with probability M^-l) and the per-row overhead.
func ModelBytesPerRow(cfg usearch.IndexConfig) uint64 {
	m := uint64(cfg.Connectivity)
	if m == 0 {
		m = defaultConnectivity
	}
	if m < 2 {
		m = 2
	}
	links := 4*(2*m+1) + 4*(m+1)/(m-1)
	return uint64(cfg.Dimensions)*ElementBytes(cfg.Quantization) + links + rowOverheadBytes
}

// PlanCapacity returns the number of rows one model holds. A positive explicit capacity is
// used up to what fits 75% of the available host memory; 0 takes all that fits.
func PlanCapacity(explicit int64, cfg usearch.IndexConfig) (int64, error) {
	rows, _, err := hostRowsFitting(ModelBytesPerRow(cfg), 0)
	if err != nil {
		return 0, err
	}
	if rows <= 0 {
		if explicit > 0 {
			return explicit, nil
		}
		return unmeasuredCapacity, nil
	}
	if explicit > 0 && explicit < rows {
		return explicit, nil
	}
	return rows, nil
}
