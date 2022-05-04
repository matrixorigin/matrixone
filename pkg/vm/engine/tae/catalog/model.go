package catalog

import "github.com/matrixorigin/matrixone/pkg/container/types"

type EntryType uint8

const (
	ETDatabase EntryType = iota
	ETTable
	ETSegment
	ETBlock
	ETColDef
)

// UINT8 UINT64  VARCHAR UINT64  INT8   CHAR    VARCHAR    UINT64
//  ET  |  ID  |  NAME  |  TS  | OPT | LOGIDX |  INFO   | PARENTID |
var ModelSchema *Schema

const (
	ModelSchemaName   = "_ModelSchema"
	ModelAttrET       = "ET"
	ModelAttrID       = "ID"
	ModelAttrName     = "NAME"
	ModelAttrTS       = "TS"
	ModelAttrOpT      = "OPT"
	ModelAttrLogIdx   = "LOGIDX"
	ModelAttrInfo     = "INFO"
	ModelAttrParentID = "PARENTID"
)

func init() {
	ModelSchema = NewEmptySchema(ModelSchemaName)
	t := types.Type{
		Oid:   types.T_uint8,
		Size:  1,
		Width: 8,
	}
	ModelSchema.AppendCol(ModelAttrET, t)
	t = types.Type{
		Oid:   types.T_uint64,
		Size:  8,
		Width: 64,
	}
	ModelSchema.AppendCol(ModelAttrID, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	ModelSchema.AppendCol(ModelAttrName, t)
	t = types.Type{
		Oid:   types.T_uint64,
		Size:  8,
		Width: 64,
	}
	ModelSchema.AppendCol(ModelAttrTS, t)
	t = types.Type{
		Oid:   types.T_int8,
		Size:  1,
		Width: 8,
	}
	ModelSchema.AppendCol(ModelAttrOpT, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	ModelSchema.AppendCol(ModelAttrLogIdx, t)
	t = types.Type{
		Oid:   types.T_varchar,
		Size:  24,
		Width: 100,
	}
	ModelSchema.AppendCol(ModelAttrInfo, t)
	t = types.Type{
		Oid:   types.T_uint64,
		Size:  8,
		Width: 64,
	}
	ModelSchema.AppendCol(ModelAttrParentID, t)
}
