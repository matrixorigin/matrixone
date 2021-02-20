package and

var (
	selAnd func([]int64, []int64) []int64
)

func init() {
	selAnd = selAndPure
}

func SelAnd(xs, ys []int64) []int64 {
	return selAndPure(xs, ys)
}

func selAndPure(xs, ys []int64) []int64 {
	cnt := 0
	i, j, n, m := 0, 0, len(xs), len(ys)
	for i < n && j < m {
		switch {
		case xs[i] > ys[j]:
			j++
		case xs[i] < ys[j]:
			i++
		default:
			xs[cnt] = xs[i]
			i++
			j++
			cnt++
		}
	}
	return xs[:cnt]
}
