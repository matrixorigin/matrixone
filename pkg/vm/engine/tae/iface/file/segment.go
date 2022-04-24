package file

type SegmentFileFactory = func(dir string, id uint64) Segment

type Segment interface {
	Base
	OpenBlock(id uint64, colCnt int, indexCnt map[int]int) (Block, error)
	WriteTS(ts uint64) error
	ReadTS() uint64
	String() string
	// IsAppendable() bool
}
