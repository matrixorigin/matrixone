package base

import "io"

type IBlockHandle interface {
	io.Closer
}

type ISegmentHandle interface {
	NewIterator() IBlockIterator
}

type Iterator interface {
	io.Closer
	Next()
	Valid() bool
}

type IBlockIterator interface {
	Iterator
	GetBlockHandle() IBlockHandle
}

type ISegmentIterator interface {
	Iterator
	GetSegmentHandle() ISegmentHandle
}
