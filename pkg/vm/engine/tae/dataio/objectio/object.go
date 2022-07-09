package objectio

import (
	"os"
	"sync"
)

type ObjectType uint8

const (
	DATA ObjectType = iota
	METADATA
)

type Object struct {
	mutex     sync.Mutex
	oFile     *os.File
	allocator *ObjectAllocator
	oType     ObjectType
}
