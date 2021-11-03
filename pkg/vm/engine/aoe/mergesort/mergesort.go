// Copyright 2021 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort/float32s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort/float64s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort/int16s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort/int32s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort/int64s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort/int8s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort/uint16s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort/uint32s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort/uint64s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort/uint8s"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/mergesort/varchar"
)

func SortBlockColumns(cols []*vector.Vector,pk int) error {
	sortedIdx := make([]uint32, cols[pk].Length())

	switch cols[pk].Typ.Oid {
	case types.T_int8:
		int8s.Sort(cols[pk], sortedIdx)
	case types.T_int16:
		int16s.Sort(cols[pk], sortedIdx)
	case types.T_int32:
		int32s.Sort(cols[pk], sortedIdx)
	case types.T_int64:
		int64s.Sort(cols[pk], sortedIdx)
	case types.T_uint8:
		uint8s.Sort(cols[pk], sortedIdx)
	case types.T_uint16:
		uint16s.Sort(cols[pk], sortedIdx)
	case types.T_uint32:
		uint32s.Sort(cols[pk], sortedIdx)
	case types.T_uint64:
		uint64s.Sort(cols[pk], sortedIdx)
	case types.T_float32:
		float32s.Sort(cols[pk], sortedIdx)
	case types.T_float64:
		float64s.Sort(cols[pk], sortedIdx)
	case types.T_char, types.T_json, types.T_varchar:
		varchar.Sort(cols[pk], sortedIdx)
	}

	for i := 0; i < len(cols); i++ {
		if i==pk {
			continue
		}
		switch cols[i].Typ.Oid {
		case types.T_int8:
			int8s.Shuffle(cols[i], sortedIdx)
		case types.T_int16:
			int16s.Shuffle(cols[i], sortedIdx)
		case types.T_int32:
			int32s.Shuffle(cols[i], sortedIdx)
		case types.T_int64:
			int64s.Shuffle(cols[i], sortedIdx)
		case types.T_uint8:
			uint8s.Shuffle(cols[i], sortedIdx)
		case types.T_uint16:
			uint16s.Shuffle(cols[i], sortedIdx)
		case types.T_uint32:
			uint32s.Shuffle(cols[i], sortedIdx)
		case types.T_uint64:
			uint64s.Shuffle(cols[i], sortedIdx)
		case types.T_float32:
			float32s.Shuffle(cols[i], sortedIdx)
		case types.T_float64:
			float64s.Shuffle(cols[i], sortedIdx)
		case types.T_char, types.T_json, types.T_varchar:
			varchar.Shuffle(cols[i], sortedIdx)
		}
	}

	return nil
}

func MergeBlocksToSegment(blks []*batch.Batch) error {
	n := len(blks) * blks[0].Vecs[0].Length()
	mergedSrc := make([]uint16, n)

	col := make([]*vector.Vector, len(blks))
	for i := 0; i < len(blks); i++ {
		col[i] = blks[i].Vecs[0]
	}

	switch blks[0].Vecs[0].Typ.Oid {
	case types.T_int8:
		int8s.Merge(col, mergedSrc)
	case types.T_int16:
		int16s.Merge(col, mergedSrc)
	case types.T_int32:
		int32s.Merge(col, mergedSrc)
	case types.T_int64:
		int64s.Merge(col, mergedSrc)
	case types.T_uint8:
		uint8s.Merge(col, mergedSrc)
	case types.T_uint16:
		uint16s.Merge(col, mergedSrc)
	case types.T_uint32:
		uint32s.Merge(col, mergedSrc)
	case types.T_uint64:
		uint64s.Merge(col, mergedSrc)
	case types.T_float32:
		float32s.Merge(col, mergedSrc)
	case types.T_float64:
		float64s.Merge(col, mergedSrc)
	case types.T_char, types.T_json, types.T_varchar:
		varchar.Merge(col, mergedSrc)
	}

	for j := 1; j < len(blks); j++ {
		for i := 0; i < len(blks); i++ {
			col[i] = blks[i].Vecs[j]
		}

		switch blks[0].Vecs[j].Typ.Oid {
		case types.T_int8:
			int8s.Multiplex(col, mergedSrc)
		case types.T_int16:
			int16s.Multiplex(col, mergedSrc)
		case types.T_int32:
			int32s.Multiplex(col, mergedSrc)
		case types.T_int64:
			int64s.Multiplex(col, mergedSrc)
		case types.T_uint8:
			uint8s.Multiplex(col, mergedSrc)
		case types.T_uint16:
			uint16s.Multiplex(col, mergedSrc)
		case types.T_uint32:
			uint32s.Multiplex(col, mergedSrc)
		case types.T_uint64:
			uint64s.Multiplex(col, mergedSrc)
		case types.T_float32:
			float32s.Multiplex(col, mergedSrc)
		case types.T_float64:
			float64s.Multiplex(col, mergedSrc)
		case types.T_char, types.T_json, types.T_varchar:
			varchar.Multiplex(col, mergedSrc)
		}
	}

	return nil
}
