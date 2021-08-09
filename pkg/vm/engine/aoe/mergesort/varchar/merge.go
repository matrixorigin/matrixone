package varchar

import "matrixone/pkg/container/types"

func Merge(data []*types.Bytes, idx []uint16) {
	nElem := len(data[0].Offsets)
	nBlk := len(data)
	heap := make(heapSlice, nBlk)
	strings := make([][]byte, nBlk)
	merged := make([]*types.Bytes, nBlk)

	for i := 0; i < nBlk; i++ {
		heap[i] = heapElem{data: data[i].Get(0), src: uint16(i), next: 1}
	}
	heapInit(heap)

	k := 0
	var offset uint32
	for i := 0; i < nBlk; i++ {
		offset = 0
		for j := 0; j < nElem; j++ {
			top := heapPop(&heap)
			offset += uint32(len(top.data))
			strings[j] = top.data
			idx[k] = top.src
			k++
			if int(top.next) < nElem {
				heapPush(&heap, heapElem{data: data[top.src].Get(int64(top.next)), src: top.src, next: top.next + 1})
			}
		}

		newData := make([]byte, offset)
		newOffsets := make([]uint32, nElem)
		newLengths := make([]uint32, nElem)
		offset = 0
		for j := 0; j < nElem; j++ {
			newOffsets[j] = offset
			l := uint32(len(strings[j]))
			newLengths[j] = l
			copy(newData[offset:], strings[j])
			offset += l
		}

		merged[i] = &types.Bytes{
			Data:    newData,
			Offsets: newOffsets,
			Lengths: newLengths,
		}
	}

	copy(data, merged)
}

func ShuffleSegment(data []*types.Bytes, idx []uint16) {
	nElem := len(data[0].Offsets)
	nBlk := len(data)
	cursors := make([]uint32, nBlk)
	strings := make([][]byte, nBlk)
	merged := make([]*types.Bytes, nBlk)

	k := 0
	var offset uint32
	for i := 0; i < nBlk; i++ {
		offset = 0
		for j := 0; j < nElem; j++ {
			d, i := data[idx[k]], cursors[idx[k]]
			strings[j] = d.Get(int64(i))
			offset += d.Lengths[i]
			cursors[idx[k]]++
			k++
		}

		newData := make([]byte, offset)
		newOffsets := make([]uint32, nElem)
		newLengths := make([]uint32, nElem)
		offset = 0
		for j := 0; j < nElem; j++ {
			newOffsets[j] = offset
			l := uint32(len(strings[j]))
			newLengths[j] = l
			copy(newData[offset:], strings[j])
			offset += l
		}

		merged[i] = &types.Bytes{
			Data:    newData,
			Offsets: newOffsets,
			Lengths: newLengths,
		}
	}
	copy(data, merged)
}
