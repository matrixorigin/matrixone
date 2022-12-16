package gc

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	PrefixGCMeta = "gc"
	GCMetaDir    = "gc"
)

type BatchType int8

const (
	CreateBlock BatchType = iota
	DeleteBlock
	DropTable
)
const (
	GCAttrObjectName = "name"
	GCAttrBlockId    = "block_id"
	GCAttrTableId    = "table_id"
	GCAttrSegmentId  = "segment_id"
	GCAttrDBId       = "db_id"
)

var (
	BlockSchemaAttr = []string{
		GCAttrBlockId,
		GCAttrSegmentId,
		GCAttrTableId,
		GCAttrDBId,
		GCAttrObjectName,
	}
	BlockSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0, 0),
		types.New(types.T_uint64, 0, 0, 0),
		types.New(types.T_uint64, 0, 0, 0),
		types.New(types.T_uint32, 0, 0, 0),
		types.New(types.T_varchar, 5000, 0, 0),
	}

	DropTableSchemaAttr = []string{
		GCAttrTableId,
		GCAttrDBId,
		GCAttrObjectName,
	}
	DropTableSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0, 0),
		types.New(types.T_uint64, 0, 0, 0),
		types.New(types.T_varchar, 5000, 0, 0),
	}
)
