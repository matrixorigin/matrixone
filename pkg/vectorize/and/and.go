package and

var (
	SelAnd func([]int64, []int64, []int64) int64
)

func selAnd(xs, ys, rs []int64) int64 {
	cnt := 0
	i, j, n, m := 0, 0, len(xs), len(ys)
	for i < n && j < m {
		switch {
		case xs[i] > ys[j]:
			j++
		case xs[i] < ys[j]:
			i++
		default:
			rs[cnt] = xs[i]
			i++
			j++
			cnt++
		}
	}
	return int64(cnt)
}
