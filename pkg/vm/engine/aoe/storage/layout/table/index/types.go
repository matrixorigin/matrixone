package index

type Type uint16

const (
	ZoneMap Type = iota
)

type Index interface {
	Type() Type
}
