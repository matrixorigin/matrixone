package index

type Type uint16

const (
	ZoneMap Type = iota
)

// TODO: Just for index framework implementation placeholder
type Index interface {
	Type() Type
	Eq(interface{}) bool
	Ne(interface{}) bool
	Lt(interface{}) bool
	Le(interface{}) bool
	Gt(interface{}) bool
	Ge(interface{}) bool
	Btw(interface{}) bool
}
