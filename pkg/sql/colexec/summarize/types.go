package summarize

import "matrixbase/pkg/sql/colexec/aggregation"

type Argument struct {
	Attrs []string
	Es    []aggregation.Extend
}
