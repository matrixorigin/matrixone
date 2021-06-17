package index

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
)

// TODO: Just for index framework implementation placeholder
type Index interface {
	Type() base.IndexType
	Eq(interface{}) bool
	Ne(interface{}) bool
	Lt(interface{}) bool
	Le(interface{}) bool
	Gt(interface{}) bool
	Ge(interface{}) bool
	Btw(interface{}) bool

	Marshall() ([]byte, error)
	Unmarshall([]byte) error
}
