package base

type BlockType uint8

const (
	TRANSIENT_BLK BlockType = iota
	PERSISTENT_BLK
	PERSISTENT_SORTED_BLK
)

func (bt BlockType) String() string {
	switch bt {
	case TRANSIENT_BLK:
		return "TBLK"
	case PERSISTENT_BLK:
		return "PBLK"
	case PERSISTENT_SORTED_BLK:
		return "PSBLK"
	}
	panic("unsupported")
}
