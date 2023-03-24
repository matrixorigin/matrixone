// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	PrefixGCMeta = "gc"
	GCMetaDir    = "gc/"
)

type BatchType int8
type CleanerState int8

const (
	CreateBlock BatchType = iota
	DeleteBlock
	DropTable
	DropDB
	DeleteFile
)

const (
	Idle CleanerState = iota
	Running
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
		types.New(types.T_Blockid, 0, 0),
		types.New(types.T_uuid, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_varchar, 5000, 0),
	}

	DropTableSchemaAttr = []string{
		GCAttrTableId,
		GCAttrDBId,
	}
	DropTableSchemaTypes = []types.Type{
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint32, 0, 0),
	}

	DropDBSchemaAtt = []string{
		GCAttrDBId,
	}
	DropDBSchemaTypes = []types.Type{
		types.New(types.T_uint32, 0, 0),
	}

	DeleteFileSchemaAtt = []string{
		GCAttrObjectName,
	}
	DeleteFileSchemaTypes = []types.Type{
		types.New(types.T_varchar, 5000, 0),
	}
)
