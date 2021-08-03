package or

var (
	SelOr func([]int64, []int64, []int64) int64
)

func selOr(xs, ys, rs []int64) int64 {
	cnt := 0
	i, j, n, m := 0, 0, len(xs), len(ys)
	for i < n && j < m {
		switch {
		case xs[i] > ys[j]:
			rs[cnt] = ys[j]
			cnt++
			j++
		case xs[i] < ys[j]:
			rs[cnt] = xs[i]
			cnt++
			i++
		default:
			rs[cnt] = xs[i]
			cnt++
			i++
			j++
		}
	}
	for ; i < n; i++ {
		rs[cnt] = xs[i]
		cnt++
	}
	for ; j < m; j++ {
		rs[cnt] = ys[j]
		cnt++
	}
	return int64(cnt)
}
