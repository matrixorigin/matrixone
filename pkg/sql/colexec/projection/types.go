package projection

import "matrixone/pkg/sql/colexec/extend"

type Argument struct {
	Rs []uint64 // reference count list
	As []string // alias name list
	Es []extend.Extend
}
