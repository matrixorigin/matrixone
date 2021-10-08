package mergesort

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
)

func SortBlockColumns(cols []*vector.Vector) error {
	sortedIdx := make([]uint32, cols[0].Length())

	switch cols[0].Typ.Oid {
	case types.T_int8:
		numericSort[int8](cols[0], sortedIdx)
	case types.T_int16:
		numericSort[int16](cols[0], sortedIdx)
	case types.T_int32:
		numericSort[int32](cols[0], sortedIdx)
	case types.T_int64:
		numericSort[int64](cols[0], sortedIdx)
	case types.T_uint8:
		numericSort[uint8](cols[0], sortedIdx)
	case types.T_uint16:
		numericSort[uint16](cols[0], sortedIdx)
	case types.T_uint32:
		numericSort[uint32](cols[0], sortedIdx)
	case types.T_uint64:
		numericSort[uint64](cols[0], sortedIdx)
	case types.T_float32:
		numericSort[float32](cols[0], sortedIdx)
	case types.T_float64:
		numericSort[float64](cols[0], sortedIdx)
	case types.T_char, types.T_json, types.T_varchar:
		strSort(cols[0], sortedIdx)
	}

	for i := 1; i < len(cols); i++ {
		switch cols[i].Typ.Oid {
		case types.T_int8:
			numericShuffle[int8](cols[i], sortedIdx)
		case types.T_int16:
			numericShuffle[int16](cols[i], sortedIdx)
		case types.T_int32:
			numericShuffle[int32](cols[i], sortedIdx)
		case types.T_int64:
			numericShuffle[int64](cols[i], sortedIdx)
		case types.T_uint8:
			numericShuffle[uint8](cols[i], sortedIdx)
		case types.T_uint16:
			numericShuffle[uint16](cols[i], sortedIdx)
		case types.T_uint32:
			numericShuffle[uint32](cols[i], sortedIdx)
		case types.T_uint64:
			numericShuffle[uint64](cols[i], sortedIdx)
		case types.T_float32:
			numericShuffle[float32](cols[i], sortedIdx)
		case types.T_float64:
			numericShuffle[float64](cols[i], sortedIdx)
		case types.T_char, types.T_json, types.T_varchar:
			strShuffle(cols[i], sortedIdx)
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
		numericMerge[int8](col, mergedSrc)
	case types.T_int16:
		numericMerge[int16](col, mergedSrc)
	case types.T_int32:
		numericMerge[int32](col, mergedSrc)
	case types.T_int64:
		numericMerge[int64](col, mergedSrc)
	case types.T_uint8:
		numericMerge[uint8](col, mergedSrc)
	case types.T_uint16:
		numericMerge[uint16](col, mergedSrc)
	case types.T_uint32:
		numericMerge[uint32](col, mergedSrc)
	case types.T_uint64:
		numericMerge[uint64](col, mergedSrc)
	case types.T_float32:
		numericMerge[float32](col, mergedSrc)
	case types.T_float64:
		numericMerge[float64](col, mergedSrc)
	case types.T_char, types.T_json, types.T_varchar:
		strMerge(col, mergedSrc)
	}

	for j := 1; j < len(blks); j++ {
		for i := 0; i < len(blks); i++ {
			col[i] = blks[i].Vecs[j]
		}

		switch blks[0].Vecs[j].Typ.Oid {
		case types.T_int8:
			numericMultiplex[int8](col, mergedSrc)
		case types.T_int16:
			numericMultiplex[int16](col, mergedSrc)
		case types.T_int32:
			numericMultiplex[int32](col, mergedSrc)
		case types.T_int64:
			numericMultiplex[int64](col, mergedSrc)
		case types.T_uint8:
			numericMultiplex[uint8](col, mergedSrc)
		case types.T_uint16:
			numericMultiplex[uint16](col, mergedSrc)
		case types.T_uint32:
			numericMultiplex[uint32](col, mergedSrc)
		case types.T_uint64:
			numericMultiplex[uint64](col, mergedSrc)
		case types.T_float32:
			numericMultiplex[float32](col, mergedSrc)
		case types.T_float64:
			numericMultiplex[float64](col, mergedSrc)
		case types.T_char, types.T_json, types.T_varchar:
			strMultiplex(col, mergedSrc)
		}
	}

	return nil
}
