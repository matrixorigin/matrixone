package avg

type int64Avg struct {
	v   int64
	cnt int64
}

type float64Avg struct {
	cnt int64
	v   float64
}

type sumCountAvg struct {
	v   float64
	cnt float64
}
