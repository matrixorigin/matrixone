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
	sortedIdx := make([]uint32, cols[0].Length())

	switch cols[0].Typ.Oid {
	case types.T_int8:
		int8s.Sort(cols[0].Col.([]int8), sortedIdx)
	case types.T_int16:
		int16s.Sort(cols[0].Col.([]int16), sortedIdx)
	case types.T_int32:
		int32s.Sort(cols[0].Col.([]int32), sortedIdx)
	case types.T_int64:
		int64s.Sort(cols[0].Col.([]int64), sortedIdx)
	case types.T_uint8:
		uint8s.Sort(cols[0].Col.([]uint8), sortedIdx)
	case types.T_uint16:
		uint16s.Sort(cols[0].Col.([]uint16), sortedIdx)
	case types.T_uint32:
		uint32s.Sort(cols[0].Col.([]uint32), sortedIdx)
	case types.T_uint64:
		uint64s.Sort(cols[0].Col.([]uint64), sortedIdx)
	case types.T_float32:
		float32s.Sort(cols[0].Col.([]float32), sortedIdx)
	case types.T_float64:
		float64s.Sort(cols[0].Col.([]float64), sortedIdx)
	case types.T_char, types.T_json, types.T_varchar:
		varchar.Sort(cols[0].Col.(*types.Bytes), sortedIdx)
	}

	for i := 1; i < len(cols); i++ {
		switch cols[i].Typ.Oid {
		case types.T_int8:
			int8s.ShuffleBlock(cols[i].Col.([]int8), sortedIdx)
		case types.T_int16:
			int16s.ShuffleBlock(cols[i].Col.([]int16), sortedIdx)
		case types.T_int32:
			int32s.ShuffleBlock(cols[i].Col.([]int32), sortedIdx)
		case types.T_int64:
			int64s.ShuffleBlock(cols[i].Col.([]int64), sortedIdx)
		case types.T_uint8:
			uint8s.ShuffleBlock(cols[i].Col.([]uint8), sortedIdx)
		case types.T_uint16:
			uint16s.ShuffleBlock(cols[i].Col.([]uint16), sortedIdx)
		case types.T_uint32:
			uint32s.ShuffleBlock(cols[i].Col.([]uint32), sortedIdx)
		case types.T_uint64:
			uint64s.ShuffleBlock(cols[i].Col.([]uint64), sortedIdx)
		case types.T_float32:
			float32s.ShuffleBlock(cols[i].Col.([]float32), sortedIdx)
		case types.T_float64:
			float64s.ShuffleBlock(cols[i].Col.([]float64), sortedIdx)
		case types.T_char, types.T_json, types.T_varchar:
			varchar.ShuffleBlock(cols[i].Col.(*types.Bytes), sortedIdx)
		}
	}

	return nil
}

func MergeBlocksToSegment(blks []*batch.Batch) error {
	n := len(blks) * blks[0].Vecs[0].Length()
	mergedSrc := make([]uint16, n)

	switch blks[0].Vecs[0].Typ.Oid {
	case types.T_int8:
		data := make([][]int8, len(blks))
		for i := 0; i < len(blks); i++ {
			data[i] = blks[i].Vecs[0].Col.([]int8)
		}
		int8s.Merge(data, mergedSrc)
	case types.T_int16:
		data := make([][]int16, len(blks))
		for i := 0; i < len(blks); i++ {
			data[i] = blks[i].Vecs[0].Col.([]int16)
		}
		int16s.Merge(data, mergedSrc)
	case types.T_int32:
		data := make([][]int32, len(blks))
		for i := 0; i < len(blks); i++ {
			data[i] = blks[i].Vecs[0].Col.([]int32)
		}
		int32s.Merge(data, mergedSrc)
	case types.T_int64:
		data := make([][]int64, len(blks))
		for i := 0; i < len(blks); i++ {
			data[i] = blks[i].Vecs[0].Col.([]int64)
		}
		int64s.Merge(data, mergedSrc)
	case types.T_uint8:
		data := make([][]uint8, len(blks))
		for i := 0; i < len(blks); i++ {
			data[i] = blks[i].Vecs[0].Col.([]uint8)
		}
		uint8s.Merge(data, mergedSrc)
	case types.T_uint16:
		data := make([][]uint16, len(blks))
		for i := 0; i < len(blks); i++ {
			data[i] = blks[i].Vecs[0].Col.([]uint16)
		}
		uint16s.Merge(data, mergedSrc)
	case types.T_uint32:
		data := make([][]uint32, len(blks))
		for i := 0; i < len(blks); i++ {
			data[i] = blks[i].Vecs[0].Col.([]uint32)
		}
		uint32s.Merge(data, mergedSrc)
	case types.T_uint64:
		data := make([][]uint64, len(blks))
		for i := 0; i < len(blks); i++ {
			data[i] = blks[i].Vecs[0].Col.([]uint64)
		}
		uint64s.Merge(data, mergedSrc)
	case types.T_float32:
		data := make([][]float32, len(blks))
		for i := 0; i < len(blks); i++ {
			data[i] = blks[i].Vecs[0].Col.([]float32)
		}
		float32s.Merge(data, mergedSrc)
	case types.T_float64:
		data := make([][]float64, len(blks))
		for i := 0; i < len(blks); i++ {
			data[i] = blks[i].Vecs[0].Col.([]float64)
		}
		float64s.Merge(data, mergedSrc)
	case types.T_char, types.T_json, types.T_varchar:
		data := make([]*types.Bytes, len(blks))
		for i := 0; i < len(blks); i++ {
			data[i] = blks[i].Vecs[0].Col.(*types.Bytes)
		}
		varchar.Merge(data, mergedSrc)
	}

	for j := 1; j < len(blks); j++ {
		switch blks[0].Vecs[j].Typ.Oid {
		case types.T_int8:
			data := make([][]int8, len(blks))
			for i := 0; i < len(blks); i++ {
				data[i] = blks[i].Vecs[j].Col.([]int8)
			}
			int8s.ShuffleSegment(data, mergedSrc)
		case types.T_int16:
			data := make([][]int16, len(blks))
			for i := 0; i < len(blks); i++ {
				data[i] = blks[i].Vecs[j].Col.([]int16)
			}
			int16s.ShuffleSegment(data, mergedSrc)
		case types.T_int32:
			data := make([][]int32, len(blks))
			for i := 0; i < len(blks); i++ {
				data[i] = blks[i].Vecs[j].Col.([]int32)
			}
			int32s.ShuffleSegment(data, mergedSrc)
		case types.T_int64:
			data := make([][]int64, len(blks))
			for i := 0; i < len(blks); i++ {
				data[i] = blks[i].Vecs[j].Col.([]int64)
			}
			int64s.ShuffleSegment(data, mergedSrc)
		case types.T_uint8:
			data := make([][]uint8, len(blks))
			for i := 0; i < len(blks); i++ {
				data[i] = blks[i].Vecs[j].Col.([]uint8)
			}
			uint8s.ShuffleSegment(data, mergedSrc)
		case types.T_uint16:
			data := make([][]uint16, len(blks))
			for i := 0; i < len(blks); i++ {
				data[i] = blks[i].Vecs[j].Col.([]uint16)
			}
			uint16s.ShuffleSegment(data, mergedSrc)
		case types.T_uint32:
			data := make([][]uint32, len(blks))
			for i := 0; i < len(blks); i++ {
				data[i] = blks[i].Vecs[j].Col.([]uint32)
			}
			uint32s.ShuffleSegment(data, mergedSrc)
		case types.T_uint64:
			data := make([][]uint64, len(blks))
			for i := 0; i < len(blks); i++ {
				data[i] = blks[i].Vecs[j].Col.([]uint64)
			}
			uint64s.ShuffleSegment(data, mergedSrc)
		case types.T_float32:
			data := make([][]float32, len(blks))
			for i := 0; i < len(blks); i++ {
				data[i] = blks[i].Vecs[j].Col.([]float32)
			}
			float32s.ShuffleSegment(data, mergedSrc)
		case types.T_float64:
			data := make([][]float64, len(blks))
			for i := 0; i < len(blks); i++ {
				data[i] = blks[i].Vecs[j].Col.([]float64)
			}
			float64s.ShuffleSegment(data, mergedSrc)
		case types.T_char, types.T_json, types.T_varchar:
			data := make([]*types.Bytes, len(blks))
			for i := 0; i < len(blks); i++ {
				data[i] = blks[i].Vecs[j].Col.(*types.Bytes)
			}
			varchar.ShuffleSegment(data, mergedSrc)
		}
	}

	return nil
}
