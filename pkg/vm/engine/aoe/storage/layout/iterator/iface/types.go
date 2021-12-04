package iface

import "github.com/matrixorigin/matrixone/pkg/container/vector"

type BlockIterator interface {
	FetchColumn() ([]*vector.Vector, error)
	BlockCount() uint32
	Reset(uint16)
	Clear()
}
