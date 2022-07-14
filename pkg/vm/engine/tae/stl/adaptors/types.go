package adaptors

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
)

type Buffer struct {
	storage stl.Vector[byte]
}

type BitSet struct {
	storage stl.Vector[byte]
	set     []byte
	size    int
	bitcnt  int
}
