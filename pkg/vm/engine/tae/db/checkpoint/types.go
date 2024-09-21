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

package checkpoint

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type State int8

const (
	ST_Running State = iota
	ST_Pending
	ST_Finished
)

type EntryType int8

const (
	ET_Global EntryType = iota
	ET_Incremental
	ET_Backup
)

type Runner interface {
	TestRunner
	RunnerReader
	Start()
	Stop()
	String() string
	EnqueueWait(any) error
	Replay(catalog.DataFactory) *CkpReplayer

	FlushTable(ctx context.Context, dbID, tableID uint64, ts types.TS) error
	GCByTS(ctx context.Context, ts types.TS) error

	// for test, delete in next phase
	DebugUpdateOptions(opts ...Option)
	GetAllCheckpoints() []*CheckpointEntry
}

type DirtyCtx struct {
	force bool
	tree  *logtail.DirtyTreeEntry
}

type Observer interface {
	OnNewCheckpoint(ts types.TS)
}

type observers struct {
	os []Observer
}

func (os *observers) add(o Observer) {
	os.os = append(os.os, o)
}

func (os *observers) OnNewCheckpoint(ts types.TS) {
	for _, o := range os.os {
		o.OnNewCheckpoint(ts)
	}
}

const (
	PrefixIncremental = "incremental"
	PrefixGlobal      = "global"
	PrefixMetadata    = "meta"
	CheckpointDir     = "ckp/"
)

const (
	CheckpointAttr_StartTS       = "start_ts"
	CheckpointAttr_EndTS         = "end_ts"
	CheckpointAttr_MetaLocation  = "meta_location"
	CheckpointAttr_EntryType     = "entry_type"
	CheckpointAttr_Version       = "version"
	CheckpointAttr_AllLocations  = "all_locations"
	CheckpointAttr_CheckpointLSN = "checkpoint_lsn"
	CheckpointAttr_TruncateLSN   = "truncate_lsn"
	CheckpointAttr_Type          = "type"

	CheckpointSchemaColumnCountV1 = 5 // start, end, loc, type, ver
	CheckpointSchemaColumnCountV2 = 9
)

var (
	CheckpointSchema *catalog.Schema
)

var (
	CheckpointSchemaAttr = []string{
		CheckpointAttr_StartTS,
		CheckpointAttr_EndTS,
		CheckpointAttr_MetaLocation,
		CheckpointAttr_EntryType,
		CheckpointAttr_Version,
		CheckpointAttr_AllLocations,
		CheckpointAttr_CheckpointLSN,
		CheckpointAttr_TruncateLSN,
		CheckpointAttr_Type,
	}
	CheckpointSchemaTypes = []types.Type{
		types.New(types.T_TS, 0, 0),
		types.New(types.T_TS, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_bool, 0, 0), // true for incremental
		types.New(types.T_uint32, 0, 0),
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_uint64, 0, 0),
		types.New(types.T_int8, 0, 0),
	}
)

func init() {
	var err error
	CheckpointSchema = catalog.NewEmptySchema("checkpoint")
	for i, colname := range CheckpointSchemaAttr {
		if err = CheckpointSchema.AppendCol(colname, CheckpointSchemaTypes[i]); err != nil {
			panic(err)
		}
	}
}

func makeRespBatchFromSchema(schema *catalog.Schema) *containers.Batch {
	bat := containers.NewBatch()
	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], common.CheckpointAllocator))
	}
	return bat
}
