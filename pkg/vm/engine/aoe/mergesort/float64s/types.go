package float64s

type sortElem struct {
	data float64
	idx  uint32
}

type sortSlice []sortElem

func (x sortSlice) Less(i, j int) bool { return x[i].data < x[j].data }
func (x sortSlice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

type heapElem struct {
	data float64
	src  uint16
	next uint32
}

type heapSlice []heapElem

func (x heapSlice) Less(i, j int) bool { return x[i].data < x[j].data }
func (x heapSlice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
