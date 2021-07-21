package and

var (
	selAnd func([]int64, []int64, []int64) int64
)

func init() {
	selAnd = andX86Asm
}

func SelAnd(xs, ys, rs []int64) []int64 {
	n := andX86Asm(xs, ys, rs)
	return rs[:n]
}
