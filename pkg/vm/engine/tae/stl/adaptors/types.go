package adaptors

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

type Buffer struct {
	// storage *containers.StdVector[byte]
	storage stl.Vector[byte]
}
