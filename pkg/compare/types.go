package compare

import "matrixbase/pkg/container/vector"

type Compare interface {
	Vector() *vector.Vector
	Set(int, *vector.Vector)
	Copy(int, int, int64, int64) int // return the increased memory space
	Compare(int, int, int64, int64) int
}
