package iface

import "github.com/matrixorigin/matrixone/pkg/container/vector"

type BacktrackingBlockIterator interface {
	FetchColumn() ([]*vector.Vector, error)
	BlockCount() uint32
	Reset(uint16)
	Clear()
}
