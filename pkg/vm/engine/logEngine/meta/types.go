package meta

import "matrixone/pkg/vm/metadata"

type Metadata struct {
	Part    int32 // 1, 2, 3, 4... -  max part == max segment row count
	IsDel   int32 // 0 for exist, 1 for delete
	Rows    int64 // current rows
	Limit   int64 // max segment row count
	Version int64
	Name    string
	Attrs   []metadata.Attribute
}
