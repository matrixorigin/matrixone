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

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
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
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer tae.Close()
	dir := path.Join(tae.Dir, "data")
	dump := gcDumpArg{
		ctx: &inspectContext{
			db: tae,
		},
		file: path.Join(dir, "test"),
	}

	txn, _ := tae.StartTxn(nil)
	database, _ := txn.CreateDatabase("db", "", "")
	schema := catalog.MockSchema(1, 0)
	schema.Extra.BlockMaxRows = 1000
	schema.Extra.ObjectMaxBlocks = 2
	rel, _ := database.CreateRelation(schema)
	tableMeta := rel.GetMeta().(*catalog.TableEntry)
	dataFactory := tables.NewDataFactory(tae.Runtime, tae.Dir)
	tableFactory := dataFactory.MakeTableFactory()
	table := tableFactory(tableMeta)
	handle := table.GetHandle(false)
	_, err := handle.GetAppender()
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrAppendableObjectNotFound))
	obj, _ := rel.CreateObject(false)
	id := obj.GetMeta().(*catalog.ObjectEntry).AsCommonID()
	appender := handle.SetAppender(id)
	assert.NotNil(t, appender)

	blkCnt := 3
	rows := schema.Extra.BlockMaxRows * uint32(blkCnt)
	_, _, toAppend, err := appender.PrepareAppend(false, rows, nil)
	assert.Equal(t, schema.Extra.BlockMaxRows, toAppend)
	assert.Nil(t, err)
	t.Log(toAppend)

	assert.NoError(t, dump.Run())

	assert.NoError(t, tae.ForceCheckpoint(ctx, tae.TxnMgr.Now(), 0))

	assert.NoError(t, dump.Run())

	remove := gcRemoveArg{
		file:    path.Join(dir, "test"),
		oriDir:  dir,
		tarDir:  path.Join(dir, "tar"),
		modTime: time.Now().Unix(),
	}

	assert.NoError(t, remove.Run())

	assert.NoError(t, err)
}
