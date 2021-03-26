package compare

import (
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/vm/process"
)

type Compare interface {
	Vector() *vector.Vector
	Set(int, *vector.Vector)
	Compare(int, int, int64, int64) int
	Copy(int, int, int64, int64, *process.Process) error
}
