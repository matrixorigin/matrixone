package fastmap

const (
	Group     = 16
	GroupMask = 0xF
)

type Map struct {
	Vs [][]int
	Ks [][]uint64
}
