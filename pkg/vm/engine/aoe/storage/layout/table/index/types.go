package index

import (
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
)

// TODO: Just for index framework implementation placeholder
type Index interface {
	buf.IMemoryNode
	Type() base.IndexType
	Eq(interface{}) bool
	Ne(interface{}) bool
	Lt(interface{}) bool
	Le(interface{}) bool
	Gt(interface{}) bool
	Ge(interface{}) bool
	Btw(interface{}) bool
}
