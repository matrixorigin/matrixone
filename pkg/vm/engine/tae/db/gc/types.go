package gc

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	PrefixGcMeta = "gc"
	GcMetaDir    = "gc/"
)

type BatchType int8

const (
	CreateBlock BatchType = iota
	DeleteBlock
	DropTable
)
const (
	GcAttrObjectName = "name"
	GcAttrBlockId    = "block_id"
	GcAttrTableId    = "table_id"
	GcAttrSegmentId  = "segment_id"
	GcAttrDbId       = "db_id"
)

var (
	BlockSchemaAttr = []string{
		GcAttrBlockId,
		GcAttrSegmentId,
		GcAttrTableId,
		GcAttrDbId,
		GcAttrObjectName,
	}
	BlockSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0, 0),
		types.New(types.T_uint64, 0, 0, 0),
		types.New(types.T_uint64, 0, 0, 0),
		types.New(types.T_uint64, 0, 0, 0),
		types.New(types.T_varchar, 5000, 0, 0),
	}

	DropTableSchemaAttr = []string{
		GcAttrTableId,
		GcAttrDbId,
		GcAttrObjectName,
	}
	DropTableSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0, 0),
		types.New(types.T_uint64, 0, 0, 0),
		types.New(types.T_varchar, 5000, 0, 0),
	}
)
