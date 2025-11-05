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
	"fmt"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
)

func Test_storageUsageDetails(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer tae.Close()

	storage := &storageUsageHistoryArg{
		ctx: &inspectContext{
			db:   tae,
			resp: &cmd_util.InspectResp{},
		},
	}
	err := storageUsageDetails(storage)
	require.NoError(t, err)
}

func TestMergeCommand(t *testing.T) {
	handle := mockTAEHandle(context.Background(), t, &options.Options{})
	asyncTxn, err := handle.db.StartTxn(nil)
	require.NoError(t, err)

	ctx := context.Background()
	database, err := testutil.CreateDatabase2(ctx, asyncTxn, "db1")
	require.NoError(t, err)
	schema := catalog.MockSchema(2, 1)
	schema.Name = "test1"
	table, err := testutil.CreateRelation2(ctx, asyncTxn, database, schema)
	require.NoError(t, err)
	vector := containers.NewVector(types.T_varchar.ToType())
	{
		id := objectio.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&id, true, true, false)
		vector.Append(stats.Marshal(), false)

		id = objectio.NewObjectid()
		stats = objectio.NewObjectStatsWithObjectID(&id, false, true, false)
		zm := index.NewZM(types.T_int32, 0)
		v1 := int32(1)
		v2 := int32(2)
		index.UpdateZM(zm, types.EncodeInt32(&v1))
		index.UpdateZM(zm, types.EncodeInt32(&v2))
		objectio.SetObjectStatsSortKeyZoneMap(stats, zm)
		vector.Append(stats.Marshal(), false)

		id = objectio.NewObjectid()
		stats = objectio.NewObjectStatsWithObjectID(&id, false, true, false)
		zm = index.NewZM(types.T_int32, 0)
		v1 = int32(2)
		v2 = int32(3)
		index.UpdateZM(zm, types.EncodeInt32(&v1))
		index.UpdateZM(zm, types.EncodeInt32(&v2))
		objectio.SetObjectStatsSortKeyZoneMap(stats, zm)
		vector.Append(stats.Marshal(), false)
	}
	require.NoError(t, table.AddDataFiles(ctx, vector))
	require.NoError(t, asyncTxn.Commit(context.Background()))

	resp, err := handle.runInspectCmd("merge show")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: true")

	handle.runInspectCmd("merge switch off")
	resp, err = handle.runInspectCmd("merge show")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: false")

	handle.runInspectCmd("merge switch off -t db1.test1")
	resp, err = handle.runInspectCmd("merge show -t db1.test1")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: false")
	require.Contains(t, string(resp.Payload), "auto merge: false")
	require.Contains(t, string(resp.Payload), "Obj(s,c,u): 2-0-0")

	handle.runInspectCmd("merge switch on -t db1.test1")
	resp, err = handle.runInspectCmd("merge show -t db1.test1")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: false")
	require.Contains(t, string(resp.Payload), "auto merge: true")

	handle.runInspectCmd("merge switch on")
	resp, err = handle.runInspectCmd("merge show -t db1.test1")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: true")
	require.Contains(t, string(resp.Payload), "auto merge: true")

	resp, err = handle.runInspectCmd("merge switch notOnOrOff")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "invalid input")

	resp, err = handle.runInspectCmd("merge switch on off")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "invalid input")

	resp, err = handle.runInspectCmd("merge trigger -t db1.test1")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "trigger nothing")

	_, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind l0")
	require.NoError(t, err)

	_, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind ln")
	require.NoError(t, err)

	_, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind vacuum")
	require.NoError(t, err)

	_, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind tombstone")
	require.NoError(t, err)

	resp, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind xx")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "invalid input")

	_, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind tombstone --tombstone-oneshot --patch-expire 1h")
	require.NoError(t, err)

	_, err = handle.runInspectCmd("merge trigger -t db1.test1 --kind l0 --l0-oneshot --patch-expire 1h")
	require.NoError(t, err)
	resp, err = handle.runInspectCmd("merge trace -t 1.2")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "invalid input")

	resp, err = handle.runInspectCmd("merge trace on")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "specified")

	resp, err = handle.runInspectCmd("merge trace xx -t 1.2")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "invalid input")

	_, err = handle.runInspectCmd("merge trace on -t 1.2")
	require.NoError(t, err)

	_, err = handle.runInspectCmd("merge trace off -t 1.2")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "invalid input")

	// checkpoint related
	_, err = handle.runInspectCmd("inspect inmemory-ckp list")
	require.NoError(t, err)

	resp, err = handle.runInspectCmd("inspect storage-ckp list -name xxx")
	require.NoError(t, err)
	require.Contains(t, resp.Message, "online mode is not implemented")
}

func Test_CkpEntries(t *testing.T) {
	entries := NewCkpEntries(2)
	var (
		e1, e2 checkpoint.CheckpointEntry
	)
	entries.Add(&e1)
	entries.Add(&e2)

	require.Equal(t, 2, len(entries.Entries))
	require.Equal(t, 2, entries.Count)

	jsonStr, err := entries.ToJson()
	require.NoError(t, err)
	t.Logf("entries: %s", jsonStr)
}

func Test_storageCkpStatArg(t *testing.T) {
	cmd := new(cobra.Command)
	arg := new(storageCkpStatArg)
	err := arg.FromCommand(cmd)
	require.NoError(t, err)
	arg.dir = "ckp/"
	arg.name = "xxx"
	err = arg.Run()
	t.Logf("err: %v", err)
	require.Error(t, err)

	arg.name = "meta_0-0_1749279217089645000-1.ckp"
	err = arg.Run()
	t.Logf("err: %v", err)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")

	arg.name = "meta_0-0_1749279217089645000-1.ckp"
	err = arg.Run()
	t.Logf("err: %v", err)
	require.Error(t, err)

	err = arg.runOnline()
	require.Error(t, err)

	entries := make([]*checkpoint.CheckpointEntry, 0)
	entries = append(entries, new(checkpoint.CheckpointEntry))
	entries = append(entries, new(checkpoint.CheckpointEntry))
	arg.readEntries = func(
		ctx context.Context,
		sid string,
		dir string,
		name string,
		verbose int,
		onEachEntry func(entry *checkpoint.CheckpointEntry),
		mp *mpool.MPool,
		fs fileservice.FileService,
	) ([]*checkpoint.CheckpointEntry, error) {
		return entries, nil
	}
	tableID := uint64(100000)
	arg.getRanges = func(
		entry *checkpoint.CheckpointEntry,
	) ([]ckputil.TableRange, error) {
		var ranges []ckputil.TableRange
		ranges = append(ranges, ckputil.TableRange{
			TableID: tableID,
		})
		tableID++
		return ranges, nil
	}
	arg.tid = invalidId
	err = arg.Run()
	t.Logf("err: %v", err)
	t.Logf("resp.msg: %s", arg.String())
	require.NoError(t, err)

	arg.tid = 100000
	err = arg.Run()
	t.Logf("err: %v", err)
	t.Logf("resp.msg: %s", arg.String())
	require.NoError(t, err)
}

func Test_storageCkpListArg(t *testing.T) {
	cmd := new(cobra.Command)
	arg := new(storageCkpListArg)
	err := arg.FromCommand(cmd)
	require.NoError(t, err)
	arg.dir = "ckp/"
	arg.name = "xxx"
	err = arg.Run()
	require.Error(t, err)

	arg.name = "meta_0-0_1749279217089645000-1.ckp"
	err = arg.Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")

	entries := make([]*checkpoint.CheckpointEntry, 0)
	entries = append(entries, new(checkpoint.CheckpointEntry))
	entries = append(entries, new(checkpoint.CheckpointEntry))
	arg.readEntries = func(
		ctx context.Context,
		sid string,
		dir string,
		name string,
		verbose int,
		onEachEntry func(entry *checkpoint.CheckpointEntry),
		mp *mpool.MPool,
		fs fileservice.FileService,
	) ([]*checkpoint.CheckpointEntry, error) {
		return entries, nil
	}
	err = arg.Run()
	require.NoError(t, err)
	t.Logf("resp.msg: %s", arg.String())
}

func Test_inspectArgs(t *testing.T) {
	arg := new(moObjStatArg)
	err := arg.InitReader(context.Background(), "xxx")
	require.NoError(t, err)

	arg2 := new(storageCkpArg)
	t.Logf("arg2: %s", arg2.String())
	err = arg2.FromCommand(nil)
	require.NoError(t, err)
	err = arg2.Run()
	require.NoError(t, err)
}

func TestApplyTableData(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	opts.EnableApplyTableData = true
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	ioutil.Start("")
	mh := &mockHandle{
		m: mpool.MustNewZero(),
	}

	mh.Handle = &Handle{
		db: tae.DB,
	}

	colCount := 2
	schema := catalog.MockSchema(colCount, -1)
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 2)

	tae.CreateRelAndAppend2(bat, true)
	tae.DeleteAll(true)
	txn, table := tae.GetRelation()
	tableEntry := table.GetMeta().(*catalog.TableEntry)
	assert.NoError(t, txn.Commit(ctx))

	dumpTableCmd := fmt.Sprintf("dump-table -d %d -t %d", tableEntry.GetDB().ID, tableEntry.ID)

	_, err := mh.runInspectCmd(dumpTableCmd)
	require.NoError(t, err)

	dumpTableFS, err := tae.Runtime.TmpFS.GetOrCreateApp(
		&fileservice.AppConfig{
			Name: DumpTableDir,
			GCFn: GCDumpTableFiles,
		},
	)
	require.NoError(t, err)

	dirs := dumpTableFS.List(ctx, "")
	var dir string
	for entry, err := range dirs {
		assert.NoError(t, err)
		t.Log(entry.Name)
		dir = entry.Name
	}

	applyTableCmd := fmt.Sprintf("apply-table-data -d %v -t %v -o %s", "db2", "table2", dir)

	_, err = mh.runInspectCmd(applyTableCmd)
	require.NoError(t, err)
}

func TestApplyTableDataError(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	ioutil.Start("")
	mh := &mockHandle{
		m: mpool.MustNewZero(),
	}

	mh.Handle = &Handle{
		db: tae.DB,
	}

	colCount := 2
	schema := catalog.MockSchema(colCount, -1)
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 2)

	tae.CreateRelAndAppend2(bat, true)
	tae.DeleteAll(true)
	txn, table := tae.GetRelation()
	tableEntry := table.GetMeta().(*catalog.TableEntry)
	assert.NoError(t, txn.Commit(ctx))

	dumpTableCmd := fmt.Sprintf("dump-table -d %d -t %d", tableEntry.GetDB().ID, tableEntry.ID)

	_, err := mh.runInspectCmd(dumpTableCmd)
	require.NoError(t, err)

	dumpTableCmd2 := fmt.Sprintf("dump-table -d %d -t %d", tae.Catalog.NextDB(), tableEntry.ID)
	resp, err := mh.runInspectCmd(dumpTableCmd2)
	require.NoError(t, err)
	assert.True(t, strings.Contains(resp.Message, "get database by id"))
	t.Log(resp.Message)

	dumpTableCmd3 := fmt.Sprintf("dump-table -d %d -t %d", tableEntry.GetDB().ID, tae.Catalog.NextTable())
	resp, err = mh.runInspectCmd(dumpTableCmd3)
	require.NoError(t, err)
	assert.True(t, strings.Contains(resp.Message, "get table by id"))
	t.Log(resp.Message)

	dumpTableFS, err := tae.Runtime.TmpFS.GetOrCreateApp(
		&fileservice.AppConfig{
			Name: DumpTableDir,
			GCFn: GCDumpTableFiles,
		},
	)
	require.NoError(t, err)

	dirs := dumpTableFS.List(ctx, "")
	var dir string
	for entry, err := range dirs {
		assert.NoError(t, err)
		t.Log(entry.Name)
		dir = entry.Name
	}

	applyTableCmd := fmt.Sprintf("apply-table-data -d %v -t %v -o %s", "db2", "table2", dir)

	resp, err = mh.runInspectCmd(applyTableCmd)
	assert.True(t, strings.Contains(resp.Message, "apply table data is not enabled"))
	require.NoError(t, err)

	tae.Opts.EnableApplyTableData = true
	resp, err = mh.runInspectCmd(applyTableCmd)
	t.Log(resp.Message)
	require.NoError(t, err)

	resp, err = mh.runInspectCmd(applyTableCmd)
	assert.True(t, strings.Contains(resp.Message, "table already exists"))
	require.NoError(t, err)
}
