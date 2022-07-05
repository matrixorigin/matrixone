//go:build !cgo
// +build !cgo

package alloc

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"

func NewAllocator(capacity int) stl.MemAllocator {
	return stl.NewSimpleAllocator()

}
