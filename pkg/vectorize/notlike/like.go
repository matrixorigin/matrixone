package notlike

import "matrixone/pkg/container/vector"

var (
	sLike func(*vector.Bytes, []byte, []int64) ([]int64, error)
)
