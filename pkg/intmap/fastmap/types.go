package fastmap

const (
	Width     = 4
	Group     = 16
	GroupMask = 0xF
)

type Map struct {
	Vs [][]int
	Ks [][]uint64
}
