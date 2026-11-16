// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import "github.com/matrixorigin/matrixone/pkg/common/mpool"

// UnionMultiFunc appends repeated source rows with UnionMulti's ordinary
// admission, alias/COW, metadata and partial-failure contract.
type UnionMultiFunc func(dst, src *Vector, row int64, count int, mp *mpool.MPool) error

// UnionBatchFunc appends a source range with UnionBatch's ordinary contract.
type UnionBatchFunc func(dst, src *Vector, offset int64, count int, flags []uint8, mp *mpool.MPool) error

// GetUnionMultiFunction selects a stateless copier for a read-only input batch.
// Refresh the selection whenever that batch starts a new input generation,
// including reuse of its headers. The function captures no source or buffers.
// A mixed, incomplete or unknown batch keeps the safe generic entrypoint.
func GetUnionMultiFunction(sources []*Vector) UnionMultiFunc {
	if !bulkCopySourcesAreFlat(sources) {
		return (*Vector).UnionMulti
	}
	return (*Vector).unionMultiFlat
}

// GetUnionBatchFunction has the same input-generation binding as
// GetUnionMultiFunction. Flat includes ordinary constant vectors.
func GetUnionBatchFunction(sources []*Vector) UnionBatchFunc {
	if !bulkCopySourcesAreFlat(sources) {
		return (*Vector).UnionBatch
	}
	return (*Vector).unionBatchFlatRange
}

func bulkCopySourcesAreFlat(sources []*Vector) bool {
	if len(sources) == 0 {
		return false
	}
	for _, source := range sources {
		if source == nil || (source.class != FLAT && source.class != CONSTANT) {
			return false
		}
	}
	return true
}

func (v *Vector) unionBatchFlatRange(w *Vector, offset int64, count int, flags []uint8, mp *mpool.MPool) error {
	return v.unionBatchFlat(w, offset, count, flags, nil, mp, 0, 0, false)
}
