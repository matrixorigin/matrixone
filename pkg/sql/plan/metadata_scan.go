// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

/*
type MetadataScanInfo struct {
	ColName      string
	BlockId      []byte
	IsHidden     bool
	EntryState   bool
	Sorted       bool
	MetaLoc      []byte
	DelLoc       []byte
	CommitTs     types.TS
	CreateTs     types.TS
	DeleteTs     types.TS
	SegId        types.Uuid
	RowCnt       int64
	NullCnt      int64
	CompressSize int64
	OriginSize   int64
	Min          []byte
	Max          []byte
}

var (
	MetadataScanInfoTypes = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // column_name
		types.New(types.T_Blockid, types.MaxVarcharLen, 0), // block_id
		types.New(types.T_bool, 0, 0),                      // entry_state
		types.New(types.T_bool, 0, 0),                      // sorted
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // meta_loc
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // delta_loc
		types.New(types.T_TS, 0, 0),                        // commit_ts
		types.New(types.T_TS, 0, 0),                        // create_ts
		types.New(types.T_TS, 0, 0),                        // delete_ts
		types.New(types.T_uuid, 0, 0),                      // meta_seg
		types.New(types.T_int64, 0, 0),                     // row_count
		types.New(types.T_int64, 0, 0),                     // null_count
		types.New(types.T_int64, 0, 0),                     // compress_size
		types.New(types.T_int64, 0, 0),                     // origin_size
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // min
		types.New(types.T_varchar, types.MaxVarcharLen, 0), // max
	}

	MetadataScanInfoNames = []string{
		"column_name",
		"block_id",
		"entry_state",
		"sorted",
		"meta_loc",
		"delta_loc",
		"commit_ts",
		"create_ts",
		"delete_ts",
		"meta_seg",
		"rows_count",
		"null_count",
		"compress_size",
		"origin_size",
		"min",
		"max",
	}
)

const (
	COL_NAME      = 0
	BLOCK_ID      = 1
	ENTRY_STATE   = 2
	SORTED        = 3
	META_LOC      = 4
	DELTA_LOC     = 5
	COMMIT_TS     = 6
	CREATE_TS     = 7
	DELETE_TS     = 8
	SEG_ID        = 9
	ROWS_CNT      = 10
	NULL_CNT      = 11
	COMPRESS_SIZE = 12
	ORIGIN_SIZE   = 13
	MIN           = 14
	MAX           = 15
)

var (
	MetadataScanColDefs = []*plan.ColDef{
		{
			Name: MetadataScanInfoNames[COL_NAME],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[COL_NAME].Oid),
				NotNullable: true,
			},
		},
		{
			Name: MetadataScanInfoNames[BLOCK_ID],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[BLOCK_ID].Oid),
				NotNullable: true,
			},
		},
		{
			Name: MetadataScanInfoNames[ENTRY_STATE],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[ENTRY_STATE].Oid),
				NotNullable: true,
			},
		},
		{
			Name: MetadataScanInfoNames[SORTED],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[SORTED].Oid),
				NotNullable: true,
			},
		},
		{
			Name: MetadataScanInfoNames[META_LOC],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[META_LOC].Oid),
				NotNullable: true,
			},
		},
		{
			Name: MetadataScanInfoNames[DELTA_LOC],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[DELTA_LOC].Oid),
				NotNullable: true,
			},
		},
		{
			Name: MetadataScanInfoNames[CREATE_TS],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[CREATE_TS].Oid),
				NotNullable: true,
			},
		},
		{
			Name: MetadataScanInfoNames[DELETE_TS],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[DELETE_TS].Oid),
				NotNullable: true,
			},
		},
		{
			Name: MetadataScanInfoNames[COMMIT_TS],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[COMMIT_TS].Oid),
				NotNullable: true,
			},
		},
		{
			Name: MetadataScanInfoNames[SEG_ID],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[SEG_ID].Oid),
				NotNullable: true,
			},
		},
		{
			Name: MetadataScanInfoNames[ROWS_CNT],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[ROWS_CNT].Oid),
				NotNullable: true,
			},
		},
		{
			Name: MetadataScanInfoNames[NULL_CNT],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[NULL_CNT].Oid),
				NotNullable: true,
			},
		},
		{
			Name: MetadataScanInfoNames[COMPRESS_SIZE],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[COMPRESS_SIZE].Oid),
				NotNullable: true,
			},
		},
		{
			Name: MetadataScanInfoNames[ORIGIN_SIZE],
			Typ: &plan.Type{
				Id:          int32(MetadataScanInfoTypes[ORIGIN_SIZE].Oid),
				NotNullable: true,
			},
		},

			{
				Name: .MetadataScanInfoNames[catalog.MIN],
				Typ: &plan.Type{
					Id:          int32(catalog.MetadataScanInfoTypes[catalog.MIN].Oid),
					NotNullable: false,
				},
			},
			{
				Name: .MetadataScanInfoNames[catalog.MAX],
				Typ: &plan.Type{
					Id:          int32(catalog.MetadataScanInfoTypes[catalog.MAX].Oid),
					NotNullable: false,
				},
			},

	}
)
*/

var (
	MetadataScanColDefs  = []*plan.ColDef{}
	MetadataScanColTypes = []types.Type{}
)

func init() {
	mlen := len(plan.MetadataScanInfo_MetadataScanInfoType_name)
	MetadataScanColTypes = make([]types.Type, mlen)
	MetadataScanColDefs = make([]*plan.ColDef, mlen)
	for i := range plan.MetadataScanInfo_MetadataScanInfoType_name {
		colname := plan.MetadataScanInfo_MetadataScanInfoType_name[i]
		var tp types.Type
		switch colname {
		case "COL_NAME":
			tp = types.New(types.T_varchar, types.MaxVarcharLen, 0)

		case "BLOCK_ID":
			tp = types.New(types.T_Blockid, types.MaxVarcharLen, 0)

		case "ENTRY_STATE":
			tp = types.New(types.T_bool, 0, 0)

		case "SORTED":
			tp = types.New(types.T_bool, 0, 0)

		case "IS_HIDDEN":
			tp = types.New(types.T_bool, 0, 0)

		case "META_LOC":
			tp = types.New(types.T_varchar, types.MaxVarcharLen, 0)

		case "DELTA_LOC":
			tp = types.New(types.T_varchar, types.MaxVarcharLen, 0)

		case "COMMIT_TS":
			tp = types.New(types.T_TS, types.MaxVarcharLen, 0)

		case "CREATE_TS":
			tp = types.New(types.T_TS, types.MaxVarcharLen, 0)

		case "DELETE_TS":
			tp = types.New(types.T_TS, types.MaxVarcharLen, 0)

		case "SEG_ID":
			tp = types.New(types.T_uuid, types.MaxVarcharLen, 0)

		case "ROWS_CNT":
			tp = types.New(types.T_int64, 0, 0)

		case "NULL_CNT":
			tp = types.New(types.T_int64, 0, 0)

		case "COMPRESS_SIZE":
			tp = types.New(types.T_int64, 0, 0)

		case "ORIGIN_SIZE":
			tp = types.New(types.T_int64, 0, 0)

		case "MIN":
			tp = types.New(types.T_varchar, types.MaxVarcharLen, 0)

		case "MAX":
			tp = types.New(types.T_varchar, types.MaxVarcharLen, 0)
		default:
			panic("unknow types when gen metadata scan info")
		}

		coldef := &plan.ColDef{
			Name: colname,
			Typ: &plan.Type{
				Id:          int32(tp.Oid),
				NotNullable: false,
			},
		}

		MetadataScanColTypes[i] = tp
		MetadataScanColDefs[i] = coldef
	}
}

func (builder *QueryBuilder) buildMetadataScan(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) int32 {
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "metadata_scan",
			},
			Cols: MetadataScanColDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		Children:        []int32{childId},
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx)
}
