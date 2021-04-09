package match

import "matrixone/pkg/container/vector"

var (
	sMatch func(*vector.Bytes, []byte) ([]int64, error)
)
