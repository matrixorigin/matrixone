package base

type IndexType uint16

const (
	ZoneMap IndexType = iota
)

func (t IndexType) String() string {
	switch t {
	case ZoneMap:
		return "ZoneMap"
	}
	panic("unsupported")
}
