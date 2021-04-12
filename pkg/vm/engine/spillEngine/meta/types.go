package meta

import "matrixone/pkg/vm/metadata"

type Metadata struct {
	Rows  int64 // number of rows
	Segs  int64 // number of segments
	Name  string
	Attrs []metadata.Attribute
}
