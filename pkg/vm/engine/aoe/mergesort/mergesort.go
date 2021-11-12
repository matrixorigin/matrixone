package mergesort

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine/aoe/mergesort/float32s"
	"matrixone/pkg/vm/engine/aoe/mergesort/float64s"
	"matrixone/pkg/vm/engine/aoe/mergesort/int16s"
	"matrixone/pkg/vm/engine/aoe/mergesort/int32s"
	"matrixone/pkg/vm/engine/aoe/mergesort/int64s"
	"matrixone/pkg/vm/engine/aoe/mergesort/int8s"
	"matrixone/pkg/vm/engine/aoe/mergesort/uint16s"
	"matrixone/pkg/vm/engine/aoe/mergesort/uint32s"
	"matrixone/pkg/vm/engine/aoe/mergesort/uint64s"
	"matrixone/pkg/vm/engine/aoe/mergesort/uint8s"
	"matrixone/pkg/vm/engine/aoe/mergesort/varchar"
)

func SortBlockColumns(cols []*vector.Vector) error {
	sortedIdx := make([]uint32, vector.Length(cols[0]))

	switch cols[0].Typ.Oid {
	case types.T_int8:
		int8s.Sort(cols[0], sortedIdx)
	case types.T_int16:
		int16s.Sort(cols[0], sortedIdx)
	case types.T_int32:
		int32s.Sort(cols[0], sortedIdx)
	case types.T_int64:
		int64s.Sort(cols[0], sortedIdx)
	case types.T_uint8:
		uint8s.Sort(cols[0], sortedIdx)
	case types.T_uint16:
		uint16s.Sort(cols[0], sortedIdx)
	case types.T_uint32:
		uint32s.Sort(cols[0], sortedIdx)
	case types.T_uint64:
		uint64s.Sort(cols[0], sortedIdx)
	case types.T_float32:
		float32s.Sort(cols[0], sortedIdx)
	case types.T_float64:
		float64s.Sort(cols[0], sortedIdx)
	case types.T_char, types.T_json, types.T_varchar:
		varchar.Sort(cols[0], sortedIdx)
	}

	for i := 1; i < len(cols); i++ {
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
	n := len(blks) * vector.Length(blks[0].Vecs[0])
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
