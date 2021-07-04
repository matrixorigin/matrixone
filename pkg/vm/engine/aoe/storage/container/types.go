package container

type Mask = uint64

const (
	ReadonlyMask Mask = 0x01000000
	HasNullMask  Mask = 0x02000000
	PosMask      Mask = 0x00FFFFFF
)
