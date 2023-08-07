// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"

	checkpoint2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
)

const (
	ModuleName    = "TAEDB"
	defaultTestDB = "db"
)

func initDB(ctx context.Context, t *testing.T, opts *options.Options) *db.DB {
	dir := testutils.InitTestEnv(ModuleName, t)
	db, _ := db.Open(ctx, dir, opts)
	// only ut executes this checker
	db.DiskCleaner.AddChecker(
		func(item any) bool {
			min := db.TxnMgr.MinTSForTest()
			checkpoint := item.(*checkpoint2.CheckpointEntry)
			//logutil.Infof("min: %v, checkpoint: %v", min.ToString(), checkpoint.GetStart().ToString())
			return !checkpoint.GetEnd().GreaterEq(min)
		})
	return db
}

func getSingleSortKeyValue(bat *containers.Batch, schema *catalog.Schema, row int) (v any) {
	v = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(row)
	return
}

func mockCNDeleteInS3(fs *objectio.ObjectFS, blk data.Block, schema *catalog.Schema, txn txnif.AsyncTxn, deleteRows []uint32) (location objectio.Location, err error) {
	pkDef := schema.GetPrimaryKey()
	view, err := blk.GetColumnDataById(context.Background(), txn, schema, pkDef.Idx)
	pkVec := containers.MakeVector(pkDef.Type)
	rowIDVec := containers.MakeVector(types.T_Rowid.ToType())
	blkID := &blk.GetMeta().(*catalog.BlockEntry).ID
	if err != nil {
		return
	}
	for _, row := range deleteRows {
		pkVal := view.GetData().Get(int(row))
		pkVec.Append(pkVal, false)
		rowID := objectio.NewRowid(blkID, row)
		rowIDVec.Append(*rowID, false)
	}
	bat := containers.NewBatch()
	bat.AddVector(catalog.AttrRowID, rowIDVec)
	bat.AddVector("pk", pkVec)
	name := objectio.MockObjectName()
	writer, err := blockio.NewBlockWriterNew(fs.Service, name, 0, nil)
	if err != nil {
		return
	}
	_, err = writer.WriteBatchWithOutIndex(containers.ToCNBatch(bat))
	if err != nil {
		return
	}
	blks, _, err := writer.Sync(context.Background())
	location = blockio.EncodeLocation(name, blks[0].GetExtent(), uint32(bat.Length()), blks[0].GetID())
	return
}
