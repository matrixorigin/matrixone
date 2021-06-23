package base

type SegmentType uint8

const (
	UNSORTED_SEG SegmentType = iota
	SORTED_SEG
)

func (st SegmentType) String() string {
	switch st {
	case UNSORTED_SEG:
		return "USSEG"
	case SORTED_SEG:
		return "SSEG"
	}
	panic("unsupported")
}
