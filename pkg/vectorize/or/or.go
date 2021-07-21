package or

var (
	selOr func([]int64, []int64, []int64) int64
)

func init() {
	selOr = orX86Asm
}

func SelOr(xs, ys, rs []int64) []int64 {
	n := selOr(xs, ys, xs)
	return rs[:n]
}
