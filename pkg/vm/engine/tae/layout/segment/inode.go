package segment

import "sync"

type Inode struct {
	inode      uint64
	size       uint64
	mutex      sync.Mutex
	extents    []Extent
	logExtents Extent
}
