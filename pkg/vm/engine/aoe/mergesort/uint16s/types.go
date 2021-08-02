package uint16s

type sortElem struct {
	elem uint16
	idx  uint32
}

type sortSlice []sortElem

func (x sortSlice) Less(i, j int) bool { return x[i].elem < x[j].elem }
func (x sortSlice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

type heapElem struct {
	elem uint16
	src  uint16
	next uint32
}

type heapSlice []heapElem

func (x heapSlice) Less(i, j int) bool { return x[i].elem < x[j].elem }
func (x heapSlice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
