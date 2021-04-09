package projection

import "matrixone/pkg/sql/colexec/extend"

type Argument struct {
	Attrs []string
	Es    []extend.Extend
}
