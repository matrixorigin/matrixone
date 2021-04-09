package restrict

import "matrixone/pkg/sql/colexec/extend"

type Argument struct {
	Attrs []string
	E     extend.Extend
}
