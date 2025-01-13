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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type Cleaner interface {
	Replay(context.Context) error
	Process(context.Context) error
	TryGC(context.Context) error
	FastExecute(context.Context) error
	AddChecker(checker func(item any) bool, key string) int
	RemoveChecker(key string) error
	GetScanWaterMark() *checkpoint.CheckpointEntry
	GetCheckpointGCWaterMark() *types.TS
	GetScannedWindow() *GCWindow
	Stop()
	GetMinMerged() *checkpoint.CheckpointEntry
	DoCheck() error
	GetPITRs() (*logtail.PitrInfo, error)
	SetTid(tid uint64)
	EnableGC()
	DisableGC()
	GCEnabled() bool
	GetMPool() *mpool.MPool
	GetSnapshots() (map[uint32]containers.Vector, error)

	// For testing
	GetTablePK(tableId uint64) string
}

var ObjectTableAttrs []string
var ObjectTableTypes []types.Type
var ObjectTableSeqnums []uint16
var ObjectTableMetaAttrs []string
var ObjectTableMetaTypes []types.Type

var FSinkerFactory ioutil.FileSinkerFactory

const ObjectTablePrimaryKeyIdx = 0
const ObjectTableVersion = 0

func init() {
	ObjectTableAttrs = []string{
		"stats",
		"created_ts",
		"deleted_ts",
		"db_id",
		"table_id",
	}
	ObjectTableTypes = []types.Type{
		types.New(types.T_varchar, types.MaxVarcharLen, 0),
		objectio.TSType,
		objectio.TSType,
		objectio.Uint64Type,
		objectio.Uint64Type,
	}
	ObjectTableSeqnums = []uint16{0, 1, 2, 3, 4}

	ObjectTableMetaAttrs = []string{
		"stats",
	}

	ObjectTableMetaTypes = []types.Type{
		objectio.VarcharType,
	}

	FSinkerFactory = ioutil.NewFSinkerImplFactory(
		ObjectTableSeqnums,
		ObjectTablePrimaryKeyIdx,
		true,
		false,
		ObjectTableVersion,
	)
}

func addObjectToBatch(
	bat *batch.Batch,
	stats *objectio.ObjectStats,
	object *ObjectEntry,
	mPool *mpool.MPool,
) error {
	err := vector.AppendBytes(bat.Vecs[0], stats[:], false, mPool)
	if err != nil {
		return err
	}
	err = vector.AppendFixed[types.TS](bat.Vecs[1], object.createTS, false, mPool)
	if err != nil {
		return err
	}
	err = vector.AppendFixed[types.TS](bat.Vecs[2], object.dropTS, false, mPool)
	if err != nil {
		return err
	}
	err = vector.AppendFixed[uint64](bat.Vecs[3], object.db, false, mPool)
	if err != nil {
		return err
	}
	err = vector.AppendFixed[uint64](bat.Vecs[4], object.table, false, mPool)
	if err != nil {
		return err
	}
	return nil
}
