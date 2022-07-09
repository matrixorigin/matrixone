package objectio

import "sync"

type StateType uint8
type InodeType uint8

const (
	RESIDENT StateType = iota
	REMOVE
)

const (
	FILE InodeType = iota
	DIR
)

type Inode struct {
	magic      uint64
	inode      uint64
	algo       uint8
	size       uint64
	originSize uint64
	rows       uint32
	cols       uint32
	idxs       uint32
	mutex      sync.RWMutex
	extents    []Extent
	typ        InodeType
	seq        uint64
}

func (i *Inode) GetFileSize() int64 {
	return int64(i.size)
}

func (i *Inode) GetOriginSize() int64 {
	return int64(i.originSize)
}

func (i *Inode) GetAlgo() uint8 {
	return i.algo
}

func (i *Inode) GetRows() uint32 {
	return i.rows
}

func (i *Inode) GetCols() uint32 {
	return i.cols
}

func (i *Inode) GetIdxs() uint32 {
	return i.idxs
}
