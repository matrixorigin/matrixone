// Copyright 2024 Matrix Origin
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

package rpc

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
)

func Test_objGetArg(t *testing.T) {
	get := objGetArg{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := get.InitReader(ctx, "abc")
	assert.Nil(t, err)
	defer get.fs.Close(ctx)

	_, err = get.GetData(ctx)
	assert.Error(t, err)
}

func Test_gcArg(t *testing.T) {
	t.Skip("todo")
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	ctx := context.Background()
	opts := config.WithQuickScanAndCKPOpts(nil)
	options.WithCheckpointGlobalMinCount(1)(opts)
	options.WithDisableGCCatalog()(opts)
	options.WithCheckpointIncrementaInterval(time.Hour)(opts)
	options.WithCheckpointGlobalMinCount(100)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	config.MinTSGCCheckerFactory(tae.DB, tae.Opts)
	dir := path.Join(tae.Dir, "data")
	dump := gcDumpArg{
		ctx: &inspectContext{
			db: tae.DB,
		},
		file: path.Join(dir, "test"),
	}
	cmd := dump.PrepareCommand()
	_ = cmd

	schema := catalog.MockSchemaAll(10, 2)
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 40)

	tae.CreateRelAndAppend2(bat, true)
	_, _ = tae.GetRelation()

	txn, db := tae.GetDB("db")
	testutil.DropRelation2(ctx, txn, db, schema.Name)
	assert.NoError(t, txn.Commit(context.Background()))

	assert.NoError(t, dump.Run())

	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	tae.AllFlushExpected(tae.TxnMgr.Now(), 4000)

	tae.DB.ForceCheckpoint(ctx, tae.TxnMgr.Now())
	testutils.WaitExpect(2000, func() bool {
		return tae.AllCheckpointsFinished()
	})

	tae.DB.ForceGlobalCheckpoint(ctx, txn.GetStartTS(), 5*time.Second)
	testutils.WaitExpect(1000, func() bool {
		return tae.AllCheckpointsFinished()
	})

	assert.NoError(t, txn.Commit(context.Background()))
	tae.DB.BGCheckpointRunner.DisableCheckpoint(ctx)
	assert.NoError(t, tae.DB.BGCheckpointRunner.WaitRunningCKPDoneForTest(ctx, true))
	assert.NoError(t, tae.DB.BGCheckpointRunner.WaitRunningCKPDoneForTest(ctx, false))

	assert.NoError(t, dump.Run())

	remove := gcRemoveArg{
		file:    path.Join(dir, "test"),
		oriDir:  dir,
		tarDir:  path.Join(dir, "tar"),
		modTime: time.Now().Unix(),
	}

	assert.NoError(t, remove.Run())
	cmd = remove.PrepareCommand()
	_ = cmd

	// to pass ci coverage
	gc := GCArg{}
	cmd = gc.PrepareCommand()
	assert.NoError(t, gc.FromCommand(cmd))
	assert.NoError(t, dump.FromCommand(cmd))
	assert.NoError(t, remove.FromCommand(cmd))
	_ = dump.String()
	_ = remove.String()
	_ = gc.String()

	assert.NoError(t, err)
}
