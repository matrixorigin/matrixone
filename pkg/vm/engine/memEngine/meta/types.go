package meta

import "matrixone/pkg/vm/engine"

type Metadata struct {
	Segs  int64
	Rows  int64
	Name  string
	Attrs []engine.Attribute
}
