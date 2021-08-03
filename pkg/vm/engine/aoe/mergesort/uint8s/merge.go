package uint8s

func Merge(data [][]uint8, src []uint16) {
	nElem := len(data[0])
	nBlk := len(data)
	heap := make(heapSlice, nBlk)
	merged := make([][]uint8, nBlk)

	for i := 0; i < nBlk; i++ {
		heap[i] = heapElem{data: data[i][0], src: uint16(i), next: 1}
		merged[i] = make([]uint8, nElem)
	}
	heapInit(heap)

	k := 0
	for i := 0; i < nBlk; i++ {
		for j := 0; j < nElem; j++ {
			top := heapPop(&heap)
			merged[i][j], src[k] = top.data, top.src
			k++
			if int(top.next) < nElem {
				heapPush(&heap, heapElem{data: data[top.src][top.next], src: top.src, next: top.next + 1})
			}
		}
	}

	for i := 0; i < nBlk; i++ {
		copy(data[i], merged[i])
	}
}

func ShuffleSegment(data [][]uint8, src []uint16) {
	nElem := len(data[0])
	nBlk := len(data)
	cursors := make([]int, nBlk)
	merged := make([][]uint8, nBlk)

	for i := 0; i < nBlk; i++ {
		merged[i] = make([]uint8, nElem)
	}

	k := 0
	for i := 0; i < nBlk; i++ {
		for j := 0; j < nElem; j++ {
			merged[i][j] = data[src[k]][cursors[src[k]]]
			cursors[src[k]]++
			k++
		}
	}

	for i := 0; i < nBlk; i++ {
		copy(data[i], merged[i])
	}
}
