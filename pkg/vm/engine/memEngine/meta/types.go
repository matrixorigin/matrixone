package meta

import (
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
)

type Metadata struct {
	Segs  int64
	Rows  int64
	Name  string
	Attrs []metadata.Attribute
}
