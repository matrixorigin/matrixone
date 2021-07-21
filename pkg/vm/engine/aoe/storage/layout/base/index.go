package base

type IndexType = uint16

const (
	ZoneMap IndexType = iota
	NumBsi
	FixStrBsi
)

// func (t IndexType) String() string {
// 	switch t {
// 	case ZoneMap:
// 		return "ZoneMap"
// 	}
// 	panic("unsupported")
// }
