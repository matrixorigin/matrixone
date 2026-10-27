//go:build mo_m0

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

package tables

import (
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func init() { activeM0MemoryFills = proveM0MemoryFills }

func proveM0MemoryFills(read, write *catalog.Schema, data *containers.Batch, cols []int) (map[int]bool, error) {
	if read == nil || read.Extra.AppendMissingState == nil {
		return nil, nil
	}
	if write == nil || data == nil || len(data.Attrs) != len(write.ColDefs) || len(data.Vecs) != len(write.ColDefs) {
		return nil, objectio.ErrMissingColumnWithoutProof
	}
	for i, col := range write.ColDefs {
		if data.Attrs[i] != col.Name || data.Vecs[i] == nil || *data.Vecs[i].GetType() != col.Type {
			return nil, objectio.ErrMissingColumnWithoutProof
		}
	}
	fills := make(map[int]bool)
	for _, idx := range cols {
		if idx >= int(objectio.SEQNUM_UPPER) {
			continue
		}
		if idx < 0 || idx >= len(read.ColDefs) {
			return nil, objectio.ErrMissingColumnWithoutProof
		}
		col := read.ColDefs[idx]
		if _, ok := write.SeqnumMap[col.SeqNum]; ok {
			continue
		}
		kind, err := catalog.M0MemorySuffixKind(read, write, col.SeqNum)
		if err != nil {
			return nil, err
		}
		switch kind {
		case plan.MissingFillKind_MISSING_FILL_NULL_V1:
			fills[idx] = true
		case plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1:
			fills[idx] = false
		default:
			return nil, objectio.ErrMissingColumnWithoutProof
		}
	}
	return fills, nil
}
