package fastmap

var (
	Find func([]uint64, uint64) int
)

func find(xs []uint64, v uint64) int {
	for i, x := range xs {
		if x == v {
			return i
		}
	}
	return -1
}
