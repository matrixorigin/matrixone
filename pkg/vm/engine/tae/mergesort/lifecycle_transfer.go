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

package mergesort

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

// NewLifecycleTransferTable copies a decoded Lifecycle booking into the same
// native slab representation produced by DoMergeAndWrite. The caller owns the
// returned table and must Release it or transfer ownership to a transaction
// entry. The decoded maps remain caller-owned and are not retained.
func NewLifecycleTransferTable(maps api.TransferMaps) (*TransferTable, error) {
	stride := 0
	for _, mapping := range maps {
		if len(mapping) == 0 {
			continue
		}
		if stride == 0 {
			stride = len(mapping)
		} else if len(mapping) != stride {
			return nil, moerr.NewInvalidInputNoCtx(
				"Lifecycle booking block strides differ from the merge producer",
			)
		}
	}
	if stride != 0 && len(maps) > int(^uint(0)>>1)/stride {
		return nil, moerr.NewInvalidInputNoCtx("Lifecycle booking slab size overflows")
	}
	table := &TransferTable{
		Stride:      stride,
		BlockActive: make([]bool, len(maps)),
	}
	if stride == 0 {
		return table, nil
	}
	slab, err := getTransferSlab(len(maps) * stride)
	if err != nil {
		return nil, err
	}
	table.Slab = slab
	for block, mapping := range maps {
		if len(mapping) == 0 {
			continue
		}
		table.BlockActive[block] = true
		copy(table.GetBlockMap(block), mapping)
	}
	return table, nil
}
