package restrict

import "matrixbase/pkg/sql/colexec/extend"

type Argument struct {
	Attrs []string
	E     extend.Extend
}
