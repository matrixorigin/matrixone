package base

type IBlockHandle interface {
}

type ISegmentHandle interface {
	NewIterator() IBlockIterator
}

type Iterator interface {
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
