// Copyright 2026 Matrix Origin
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

package frontend

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"net/url"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type testTableDumpObjectCopier struct {
	fileservice.FileService
	data   []byte
	copied bool
}

type testConcurrentTableDumpObjectCopier struct {
	fileservice.FileService
	data        []byte
	release     chan struct{}
	active      atomic.Int32
	maximum     atomic.Int32
	statRelease chan struct{}
	statActive  atomic.Int32
	statMaximum atomic.Int32
}

type testAmbiguousWriteFileService struct {
	fileservice.FileService
}

type tableDumpRequestContextKey struct{}

func (f *testAmbiguousWriteFileService) Write(ctx context.Context, vector fileservice.IOVector) error {
	if err := f.FileService.Write(ctx, vector); err != nil {
		return err
	}
	return errors.New("write response lost after destination was created")
}

func TestLockTableDumpLoadTargetsUsesRequestContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{Mode: txn.TxnMode_Pessimistic}).AnyTimes()
	ses.proc.Base.TxnOperator = txnOp
	eng := mock_frontend.NewMockEngine(ctrl)
	ses.txnHandler.storage = eng

	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableID(gomock.Any()).Return(uint64(42)).AnyTimes()
	rel.EXPECT().GetPrimaryKeys(gomock.Any()).Return([]*engine.Attribute{{
		Name: "id",
		Type: types.T_int64.ToType(),
	}}, nil)

	staleCtx, cancelStale := context.WithCancel(context.Background())
	cancelStale()
	ses.proc.Ctx = staleCtx
	requestCtx := context.WithValue(context.Background(), tableDumpRequestContextKey{}, "request")

	var locked []uint64
	stub := gostub.Stub(&lockTableForTableDump, func(
		ctx context.Context,
		_ engine.Engine,
		_ *process.Process,
		tableID uint64,
		_ types.Type,
		_ bool,
	) error {
		require.NoError(t, ctx.Err())
		require.Equal(t, "request", ctx.Value(tableDumpRequestContextKey{}))
		locked = append(locked, tableID)
		return nil
	})
	defer stub.Reset()

	err := lockTableDumpLoadTargets(requestCtx, ses, []tableDumpRelationRef{{relation: rel}}, true)
	require.NoError(t, err)
	require.Equal(t, []uint64{42, tableDumpObjectInstallLockTableID}, locked)
}

func (c *testConcurrentTableDumpObjectCopier) CopyObject(
	ctx context.Context,
	_ fileservice.FileService,
	_ string,
	dstPath string,
) (bool, error) {
	active := c.active.Add(1)
	defer c.active.Add(-1)
	for {
		maximum := c.maximum.Load()
		if active <= maximum || c.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-c.release:
	}
	err := c.FileService.Write(ctx, fileservice.IOVector{
		FilePath: dstPath,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(c.data)), Data: c.data}},
	})
	return true, err
}

func (c *testConcurrentTableDumpObjectCopier) StatFile(
	ctx context.Context,
	filePath string,
) (*fileservice.DirEntry, error) {
	if c.statRelease == nil {
		return c.FileService.StatFile(ctx, filePath)
	}
	active := c.statActive.Add(1)
	defer c.statActive.Add(-1)
	for {
		maximum := c.statMaximum.Load()
		if active <= maximum || c.statMaximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.statRelease:
	}
	return c.FileService.StatFile(ctx, filePath)
}

func (c *testConcurrentTableDumpObjectCopier) Read(ctx context.Context, vector *fileservice.IOVector) error {
	if c.statRelease == nil {
		return c.FileService.Read(ctx, vector)
	}
	active := c.statActive.Add(1)
	defer c.statActive.Add(-1)
	for {
		maximum := c.statMaximum.Load()
		if active <= maximum || c.statMaximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.statRelease:
	}
	return c.FileService.Read(ctx, vector)
}

func (c *testTableDumpObjectCopier) CopyObject(
	ctx context.Context,
	_ fileservice.FileService,
	_ string,
	dstPath string,
) (bool, error) {
	c.copied = true
	err := c.FileService.Write(ctx, fileservice.IOVector{
		FilePath: dstPath,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(c.data)), Data: c.data}},
	})
	return true, err
}

func TestOpenLocalTableDump(t *testing.T) {
	_, err := openLocalTableDump("relative/path")
	require.Error(t, err)
	_, err = openLocalTableDump("s3://bucket/path")
	require.Error(t, err)

	fs, err := openLocalTableDump("file://" + t.TempDir())
	require.NoError(t, err)
	fs.Close(context.Background())
}

func TestOpenStageTableDump(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	root := t.TempDir()
	ses.proc.GetStageCache().Set("fixture", stage.StageDef{
		Id: 1, Name: "fixture", Url: &url.URL{Scheme: stage.FILE_PROTOCOL, Path: root},
	})

	fs, closeFS, err := openTableDumpFS(context.Background(), ses, "stage://fixture/tpch100g/lineitem")
	require.NoError(t, err)
	defer closeFS()
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: tableDumpReadyName,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte("1")}},
	}))
	_, err = fs.StatFile(context.Background(), tableDumpReadyName)
	require.NoError(t, err)
	_, _, err = openTableDumpFS(context.Background(), ses, "relative/path")
	require.Error(t, err)
}

func TestResolveTableName(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()

	_, _, err := resolveTableName(ses, nil)
	require.Error(t, err)
	_, _, err = resolveTableName(ses, &tree.TableName{})
	require.Error(t, err)

	table := tree.NewTableName("orders", tree.ObjectNamePrefix{}, nil)
	_, _, err = resolveTableName(ses, table)
	require.Error(t, err)
	ses.SetDatabaseName("tpch")
	dbName, tableName, err := resolveTableName(ses, table)
	require.NoError(t, err)
	require.Equal(t, "tpch", dbName)
	require.Equal(t, "orders", tableName)

	table.ExplicitSchema = true
	table.SchemaName = "archive"
	dbName, tableName, err = resolveTableName(ses, table)
	require.NoError(t, err)
	require.Equal(t, "archive", dbName)
	require.Equal(t, "orders", tableName)
}

func TestTableDumpPrivileges(t *testing.T) {
	table := tree.NewTableName("orders", tree.ObjectNamePrefix{SchemaName: "tpch", ExplicitSchema: true}, nil)
	dump := &tree.DumpTable{Table: table}
	dumpPriv := determinePrivilegeSetOfStatement(dump)
	require.Equal(t, objectTypeNone, dumpPriv.objType)
	require.Equal(t, privilegeKindSpecial, dumpPriv.kind)
	require.Equal(t, specialTagAdmin, dumpPriv.special)
	require.True(t, dumpPriv.writeDatabaseAndTableDirectly)
	require.Equal(t, []string{"tpch"}, dumpPriv.writeDatabaseTargets)
	require.Empty(t, dumpPriv.entries)

	ctrl := gomock.NewController(t)
	ses := newSes(dumpPriv, ctrl)
	defer ses.Close()
	allowed, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(context.Background(), ses, dump)
	require.NoError(t, err)
	require.True(t, allowed)
	ses.GetTenantInfo().SetDefaultRole("readonly")
	allowed, _, err = authenticateUserCanExecuteStatementWithObjectTypeNone(context.Background(), ses, dump)
	require.NoError(t, err)
	require.False(t, allowed)
	ses.SetTenantInfo(&TenantInfo{Tenant: "tenant", DefaultRole: accountAdminRoleName})
	allowed, _, err = authenticateUserCanExecuteStatementWithObjectTypeNone(context.Background(), ses, dump)
	require.NoError(t, err)
	require.True(t, allowed)

	loadPriv := determinePrivilegeSetOfStatement(&tree.LoadTable{Table: table})
	require.Equal(t, objectTypeNone, loadPriv.objType)
	require.Equal(t, privilegeKindSpecial, loadPriv.kind)
	require.Equal(t, specialTagAdmin, loadPriv.special)
	require.True(t, loadPriv.writeDatabaseAndTableDirectly)
	require.Equal(t, []string{"tpch"}, loadPriv.writeDatabaseTargets)
	require.Empty(t, loadPriv.entries)
	load := &tree.LoadTable{Table: table}
	allowed, _, err = authenticateUserCanExecuteStatementWithObjectTypeNone(context.Background(), ses, load)
	require.NoError(t, err)
	require.True(t, allowed)
	ses.GetTenantInfo().SetDefaultRole("readonly")
	allowed, _, err = authenticateUserCanExecuteStatementWithObjectTypeNone(context.Background(), ses, load)
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestExecInFrontendRoutesTableDumpAndLoad(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	for _, stmt := range []tree.Statement{&tree.DumpTable{}, &tree.LoadTable{}} {
		_, err := execInFrontend(ses, &ExecCtx{reqCtx: context.Background(), stmt: stmt})
		require.Error(t, err)
	}
}

func TestGetTableDumpRelations(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	master := mock_frontend.NewMockRelation(ctrl)
	indexRel := mock_frontend.NewMockRelation(ctrl)
	ses.txnHandler.storage = eng
	eng.EXPECT().Database(gomock.Any(), "tpch", gomock.Any()).Return(db, nil).AnyTimes()

	masterDef := &plan.TableDef{
		Name: "orders",
		Cols: []*plan.ColDef{{
			Name: "o_orderkey", Typ: plan.Type{Id: int32(types.T_int64)},
			Default: &plan.Default{},
		}},
		Indexes: []*plan.IndexDef{{IndexName: "idx", IndexTableName: "__idx", IndexAlgoTableType: "regular"}},
	}
	master.EXPECT().GetTableDef(gomock.Any()).Return(masterDef).AnyTimes()
	master.EXPECT().GetTableName().Return("orders")
	db.EXPECT().Relation(gomock.Any(), "__idx", nil).Return(indexRel, nil)
	indexRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		Name: "__idx",
		Cols: []*plan.ColDef{{
			Name: "index_key", Typ: plan.Type{Id: int32(types.T_varchar)}, Default: &plan.Default{},
		}},
	})

	refs, err := getTableDumpRelations(context.Background(), ses, "tpch", master)
	require.NoError(t, err)
	require.Len(t, refs, 2)
	require.Equal(t, "main", refs[0].Role)
	require.Equal(t, "index", refs[1].Role)
	require.Equal(t, "idx", refs[1].IndexName)
	require.NotEmpty(t, refs[0].SchemaHash)
	require.NotEmpty(t, refs[1].SchemaHash)
	require.Equal(t, tableDumpRelationKey("index", "idx", "regular"), "index\x00idx\x00regular")
}

func TestValidateTableDumpRelationsRejectsHiddenAutoIncrement(t *testing.T) {
	ctrl := gomock.NewController(t)
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		Name: "__fulltext", Cols: []*plan.ColDef{{
			Name: catalog.FakePrimaryKeyColName,
			Typ:  plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}},
	})
	err := validateTableDumpRelations(context.Background(), []tableDumpRelationRef{{relation: rel}})
	require.ErrorContains(t, err, "auto-increment")
}

func TestGetTableForDumpErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	_, _, _, err := getTableForDump(context.Background(), ses, nil)
	require.Error(t, err)

	ses.SetDatabaseName("tpch")
	eng := mock_frontend.NewMockEngine(ctrl)
	ses.txnHandler.storage = eng
	eng.EXPECT().Database(gomock.Any(), "tpch", gomock.Any()).Return(nil, errors.New("database failed"))
	_, _, _, err = getTableForDump(context.Background(), ses, tree.NewTableName("orders", tree.ObjectNamePrefix{}, nil))
	require.Error(t, err)

	db := mock_frontend.NewMockDatabase(ctrl)
	eng.EXPECT().Database(gomock.Any(), "tpch", gomock.Any()).Return(db, nil)
	db.EXPECT().Relation(gomock.Any(), "orders", nil).Return(nil, errors.New("relation failed"))
	_, _, _, err = getTableForDump(context.Background(), ses, tree.NewTableName("orders", tree.ObjectNamePrefix{}, nil))
	require.Error(t, err)

	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(nil)
	_, err = getTableDumpRelations(context.Background(), ses, "tpch", rel)
	require.Error(t, err)

	def := &plan.TableDef{
		Name: "orders",
		Cols: []*plan.ColDef{{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}}},
	}
	rel = mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def)
	eng.EXPECT().Database(gomock.Any(), "tpch", gomock.Any()).Return(nil, errors.New("database failed"))
	_, err = getTableDumpRelations(context.Background(), ses, "tpch", rel)
	require.Error(t, err)

	def.Indexes = []*plan.IndexDef{{IndexName: "idx", IndexTableName: "__idx"}}
	rel = mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def)
	rel.EXPECT().GetTableName().Return("orders")
	db = mock_frontend.NewMockDatabase(ctrl)
	eng.EXPECT().Database(gomock.Any(), "tpch", gomock.Any()).Return(db, nil)
	db.EXPECT().Relation(gomock.Any(), "__idx", nil).Return(nil, errors.New("index relation failed"))
	_, err = getTableDumpRelations(context.Background(), ses, "tpch", rel)
	require.Error(t, err)
}

func TestGetTableDumpRelationsRejectsDuplicateMapping(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	indexRel := mock_frontend.NewMockRelation(ctrl)
	ses.txnHandler.storage = eng
	eng.EXPECT().Database(gomock.Any(), "tpch", gomock.Any()).Return(db, nil)
	db.EXPECT().Relation(gomock.Any(), "__idx", nil).Return(indexRel, nil)
	indexRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		Name: "__idx",
		Cols: []*plan.ColDef{{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}}},
	})
	rel.EXPECT().GetTableName().Return("orders")
	rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		Name: "orders",
		Cols: []*plan.ColDef{{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}}},
		Indexes: []*plan.IndexDef{
			{IndexName: "idx", IndexTableName: "__idx", IndexAlgoTableType: "regular"},
			{IndexName: "idx", IndexTableName: "__idx", IndexAlgoTableType: "regular"},
		},
	})
	_, err := getTableDumpRelations(context.Background(), ses, "tpch", rel)
	require.Error(t, err)
}

func newTableDumpTestObject(t *testing.T, tombstone bool) tableDumpObject {
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
	return tableDumpObject{
		Name: stats.ObjectName().String(), Stats: append([]byte(nil), stats.Marshal()...), Tombstone: tombstone,
	}
}

func writePhysicalTableDumpTestObject(
	t *testing.T,
	fs fileservice.FileService,
) tableDumpObject {
	t.Helper()
	ctx := context.Background()
	id := objectio.NewObjectid()
	objectName := objectio.BuildObjectNameWithObjectID(&id)
	name := objectName.String()
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.SetAttributes([]string{"a"})
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(42), false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	writer, err := objectio.NewObjectWriter(objectName, fs, 0, nil, nil)
	require.NoError(t, err)
	_, err = writer.Write(bat)
	require.NoError(t, err)
	writer.WriteObjectMeta(ctx, 1, nil)
	_, err = writer.WriteEnd(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	size, hash, err := hashTableDumpFile(ctx, fs, name)
	require.NoError(t, err)
	return tableDumpObject{
		Name: name, Stats: append([]byte(nil), stats.Marshal()...), Size: size, SHA256: hash,
	}
}

func TestLoadPhysicalTableDumpStatsRejectsManifestForgery(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewLocalETLFS("objects", t.TempDir())
	require.NoError(t, err)
	item := writePhysicalTableDumpTestObject(t, fs)
	targetDef := &plan.TableDef{
		Name:          "target",
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "a"},
		Name2ColIndex: map[string]int32{"a": 0},
		Cols: []*plan.ColDef{{
			Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64)},
		}},
	}
	schema, err := buildTableDumpPhysicalSchema(targetDef)
	require.NoError(t, err)

	stats, blocks, err := loadPhysicalTableDumpStats(ctx, fs, item, mpool.MustNewZero(), schema)
	require.NoError(t, err)
	require.Equal(t, uint32(1), blocks)
	require.Equal(t, objectio.ObjectStats(item.Stats), stats)
	tombstoneItem := item
	tombstoneItem.Tombstone = true
	_, _, err = loadPhysicalTableDumpStats(ctx, fs, tombstoneItem, mpool.MustNewZero(), schema)
	require.ErrorContains(t, err, "tombstone object")

	incompatibleDef := *targetDef
	incompatibleDef.Cols = []*plan.ColDef{{
		Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_varchar)},
	}}
	incompatibleSchema, err := buildTableDumpPhysicalSchema(&incompatibleDef)
	require.NoError(t, err)
	_, _, err = loadPhysicalTableDumpStats(ctx, fs, item, mpool.MustNewZero(), incompatibleSchema)
	require.ErrorContains(t, err, "target expects")

	forged := item
	forged.Stats = append([]byte(nil), item.Stats...)
	forgedStats := objectio.ObjectStats(forged.Stats)
	require.NoError(t, objectio.SetObjectStatsBlkCnt(&forgedStats, math.MaxUint16))
	forged.Stats = append(forged.Stats[:0], forgedStats.Marshal()...)
	_, _, err = loadPhysicalTableDumpStats(ctx, fs, forged, mpool.MustNewZero(), schema)
	require.ErrorContains(t, err, "do not match physical metadata")

	objects := []tableDumpObject{item}
	var total atomic.Uint64
	total.Store(tableDumpMaxBlocks)
	err = validatePhysicalTableDumpObjectsImpl(ctx, fs, objects, mpool.MustNewZero(), &total, targetDef)
	require.ErrorContains(t, err, "more than 1000000 blocks")

	header := objectio.BuildHeader()
	header.SetExtent(objectio.NewExtent(0, objectio.HeaderSize, tableDumpMaxObjectMeta+1, tableDumpMaxObjectMeta+1))
	badItem := newTableDumpTestObject(t, false)
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: badItem.Name,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(header)), Data: header}},
	}))
	_, _, err = loadPhysicalTableDumpStats(ctx, fs, badItem, mpool.MustNewZero(), schema)
	require.ErrorContains(t, err, "invalid metadata extent")
}

func TestSubmitTableDumpObjects(t *testing.T) {
	ctrl := gomock.NewController(t)
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
	}).Times(1)
	rel.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, bat any) error {
		require.NotNil(t, bat)
		return nil
	}).Times(1)
	rel.EXPECT().Delete(gomock.Any(), gomock.Any(), "").DoAndReturn(func(ctx context.Context, bat any, _ string) error {
		require.Equal(t, true, ctx.Value(defines.SkipTransferKey{}))
		require.NotNil(t, bat)
		return nil
	}).Times(1)

	objects := []tableDumpObject{newTableDumpTestObject(t, false), newTableDumpTestObject(t, true)}
	submitted, err := submitTableDumpObjects(context.Background(), rel, objects, mpool.MustNewZero())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{objects[0].Name, objects[1].Name}, submitted)
}

func TestSubmitTableDumpObjectsRejectsInvalidMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	rel := mock_frontend.NewMockRelation(ctrl)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(nil).AnyTimes()
	mp := mpool.MustNewZero()

	_, err := submitTableDumpObjects(context.Background(), rel, []tableDumpObject{{Name: "bad", Stats: []byte("short")}}, mp)
	require.Error(t, err)
	item := newTableDumpTestObject(t, false)
	item.Name = "wrong-name"
	_, err = submitTableDumpObjects(context.Background(), rel, []tableDumpObject{item}, mp)
	require.Error(t, err)
	item = newTableDumpTestObject(t, false)
	stats := objectio.ObjectStats(item.Stats)
	objectio.WithAppendable()(&stats)
	item.Stats = stats.Marshal()
	_, err = submitTableDumpObjects(context.Background(), rel, []tableDumpObject{item}, mp)
	require.Error(t, err)
}

func TestHandleLoadTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetDatabaseName("tpch")

	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	workspace := newTestWorkspace()
	var targetsLocked atomic.Bool
	ses.txnHandler.storage = eng
	ses.txnHandler.txnOp = txnOp
	eng.EXPECT().Database(gomock.Any(), "tpch", txnOp).Return(db, nil).Times(2)
	db.EXPECT().Relation(gomock.Any(), "orders", nil).Return(rel, nil)

	def := &plan.TableDef{
		Name: "orders",
		Cols: []*plan.ColDef{{
			Name: "o_orderkey", Typ: plan.Type{Id: int32(types.T_int64)},
			Default: &plan.Default{},
		}},
	}
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
	rel.EXPECT().GetTableName().Return("orders")
	rel.EXPECT().Rows(gomock.Any()).DoAndReturn(func(context.Context) (uint64, error) {
		require.True(t, targetsLocked.Load())
		return 0, nil
	})
	rel.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(workspace).AnyTimes()

	object := newTableDumpTestObject(t, false)
	content := []byte("immutable object fixture")
	object.Size = int64(len(content))
	object.SHA256 = fmt.Sprintf("%x", sha256.Sum256(content))
	object.FixturePath = path.Join("objects", object.Name)
	dumpRoot := t.TempDir()
	dumpFS, err := fileservice.NewLocalETLFS("dump", dumpRoot)
	require.NoError(t, err)
	require.NoError(t, dumpFS.Write(context.Background(), fileservice.IOVector{
		FilePath: object.FixturePath,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: object.Size, Data: content}},
	}))
	require.NoError(t, dumpFS.Write(context.Background(), fileservice.IOVector{
		FilePath: tableDumpReadyName,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte("1")}},
	}))
	schemaHash, err := tableSchemaHash(def)
	require.NoError(t, err)
	require.NoError(t, writeTableDumpJSON(context.Background(), dumpFS, tableDumpManifestName, &tableDumpManifest{
		Version: tableDumpFormatVersion, SourceDatabase: "tpch", SourceTable: "orders",
		SchemaHash: schemaHash,
		Relations:  []tableDumpRelation{{Role: "main", SourceTable: "orders", SchemaHash: schemaHash, Objects: []tableDumpObject{object}}},
	}))

	targetFS, err := fileservice.NewLocalETLFS("target", t.TempDir())
	require.NoError(t, err)
	stub := gostub.Stub(&GetObjectFSProvider, func(*Session) (fileservice.FileService, error) {
		return targetFS, nil
	})
	defer stub.Reset()
	lockStub := gostub.Stub(&lockTableDumpLoadTargets, func(_ context.Context, _ *Session, refs []tableDumpRelationRef, install bool) error {
		require.Len(t, refs, 1)
		require.True(t, install)
		targetsLocked.Store(true)
		return nil
	})
	defer lockStub.Reset()
	physicalStub := gostub.Stub(&validatePhysicalTableDumpObjects, func(
		context.Context, fileservice.FileService, []tableDumpObject, *mpool.MPool, *atomic.Uint64, *plan.TableDef,
	) error {
		return nil
	})
	defer physicalStub.Reset()

	stmt := &tree.LoadTable{Table: tree.NewTableName("orders", tree.ObjectNamePrefix{}, nil), Path: dumpRoot}
	require.NoError(t, handleLoadTable(context.Background(), ses, stmt))
	require.Equal(t, []string{object.Name}, workspace.protectedCloneFiles)
	require.Equal(t, []string{object.Name}, workspace.trackedLoadFiles)
	require.NoError(t, verifyTableDumpObject(context.Background(), targetFS, object.Name, object.Size, object.SHA256))
}

func TestHandleDumpTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.SetDatabaseName("tpch")

	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	reader := mock_frontend.NewMockReader(ctrl)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	workspace := newTestWorkspace()
	ses.txnHandler.storage = eng
	ses.txnHandler.txnOp = txnOp
	eng.EXPECT().Database(gomock.Any(), "tpch", txnOp).Return(db, nil).Times(2)
	db.EXPECT().Relation(gomock.Any(), "orders", nil).Return(rel, nil)

	def := &plan.TableDef{
		Name: "orders",
		Cols: []*plan.ColDef{{
			Name: "o_orderkey", Typ: plan.Type{Id: int32(types.T_int64)},
			Default: &plan.Default{},
		}},
	}
	rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
	rel.EXPECT().GetTableName().Return("orders")
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(workspace).AnyTimes()

	object := newTableDumpTestObject(t, false)
	reader.EXPECT().Read(gomock.Any(), nil, nil, gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, bat *batch.Batch) (bool, error) {
			if len(bat.Vecs) == 2 {
				require.NoError(t, vector.AppendBytes(bat.Vecs[1], object.Stats, false, mp))
			}
			return false, nil
		},
	).Times(2)
	reader.EXPECT().Close().Return(nil)
	readerStub := gostub.Stub(&newImmutableTableMetaReader, func(context.Context, engine.Relation, int) (engine.Reader, error) {
		return reader, nil
	})
	defer readerStub.Reset()

	sourceFS, err := fileservice.NewLocalETLFS("source", t.TempDir())
	require.NoError(t, err)
	content := []byte("immutable object fixture")
	require.NoError(t, sourceFS.Write(context.Background(), fileservice.IOVector{
		FilePath: object.Name,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
	}))
	fsStub := gostub.Stub(&GetObjectFSProvider, func(*Session) (fileservice.FileService, error) {
		return sourceFS, nil
	})
	defer fsStub.Reset()

	dumpRoot := t.TempDir()
	stmt := &tree.DumpTable{Table: tree.NewTableName("orders", tree.ObjectNamePrefix{}, nil), Path: dumpRoot}
	require.NoError(t, handleDumpTable(context.Background(), ses, stmt))
	dumpFS, err := fileservice.NewLocalETLFS("dump", dumpRoot)
	require.NoError(t, err)
	manifest, err := readTableDumpManifest(context.Background(), dumpFS)
	require.NoError(t, err)
	require.Len(t, manifest.Relations, 1)
	require.Len(t, manifest.Relations[0].Objects, 1)
	require.Equal(t, object.Name, manifest.Relations[0].Objects[0].Name)
	_, err = dumpFS.StatFile(context.Background(), tableDumpReadyName)
	require.NoError(t, err)
}

func TestHandleDumpTableRejectsUnsupportedSchemas(t *testing.T) {
	for _, tc := range []struct {
		name string
		def  *plan.TableDef
	}{
		{name: "partition", def: &plan.TableDef{Name: "orders", Partition: &plan.Partition{}}},
		{name: "auto-increment", def: &plan.TableDef{Name: "orders", Cols: []*plan.ColDef{{Typ: plan.Type{AutoIncr: true}}}}},
		{name: "foreign-key", def: &plan.TableDef{Name: "orders", Fkeys: []*plan.ForeignKeyDef{{Name: "fk_parent"}}}},
		{name: "referenced", def: &plan.TableDef{Name: "orders", RefChildTbls: []uint64{42}}},
		{name: "temporary", def: &plan.TableDef{Name: "orders", IsTemporary: true}},
		{name: "external", def: &plan.TableDef{Name: "orders", TableType: catalog.SystemExternalRel}},
		{name: "source", def: &plan.TableDef{Name: "orders", TableType: catalog.SystemSourceRel}},
		{name: "view", def: &plan.TableDef{Name: "orders", TableType: catalog.SystemViewRel}},
		{name: "sequence", def: &plan.TableDef{Name: "orders", TableType: catalog.SystemSequenceRel}},
		{name: "transient", def: &plan.TableDef{Name: "orders", TableType: catalog.SystemTransientRel}},
		{name: "view-sql", def: &plan.TableDef{Name: "orders", ViewSql: &plan.ViewDef{}}},
		{name: "table-function", def: &plan.TableDef{Name: "orders", TblFunc: &plan.TableFunction{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			defer ses.Close()
			ses.SetDatabaseName("tpch")
			eng := mock_frontend.NewMockEngine(ctrl)
			db := mock_frontend.NewMockDatabase(ctrl)
			rel := mock_frontend.NewMockRelation(ctrl)
			ses.txnHandler.storage = eng
			eng.EXPECT().Database(gomock.Any(), "tpch", gomock.Any()).Return(db, nil)
			db.EXPECT().Relation(gomock.Any(), "orders", nil).Return(rel, nil)
			rel.EXPECT().GetTableDef(gomock.Any()).Return(tc.def)
			err := handleDumpTable(context.Background(), ses, &tree.DumpTable{
				Table: tree.NewTableName("orders", tree.ObjectNamePrefix{}, nil), Path: t.TempDir(),
			})
			require.Error(t, err)
		})
	}
}

func TestHandleLoadTableRejectsNonObjectBackedRelations(t *testing.T) {
	for _, tc := range []struct {
		name string
		def  *plan.TableDef
	}{
		{name: "temporary", def: &plan.TableDef{Name: "orders", IsTemporary: true}},
		{name: "external", def: &plan.TableDef{Name: "orders", TableType: catalog.SystemExternalRel}},
		{name: "source", def: &plan.TableDef{Name: "orders", TableType: catalog.SystemSourceRel}},
		{name: "view", def: &plan.TableDef{Name: "orders", TableType: catalog.SystemViewRel}},
		{name: "sequence", def: &plan.TableDef{Name: "orders", TableType: catalog.SystemSequenceRel}},
		{name: "transient", def: &plan.TableDef{Name: "orders", TableType: catalog.SystemTransientRel}},
		{name: "view-sql", def: &plan.TableDef{Name: "orders", ViewSql: &plan.ViewDef{}}},
		{name: "table-function", def: &plan.TableDef{Name: "orders", TblFunc: &plan.TableFunction{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			defer ses.Close()
			ses.SetDatabaseName("tpch")
			eng := mock_frontend.NewMockEngine(ctrl)
			db := mock_frontend.NewMockDatabase(ctrl)
			rel := mock_frontend.NewMockRelation(ctrl)
			ses.txnHandler.storage = eng
			eng.EXPECT().Database(gomock.Any(), "tpch", gomock.Any()).Return(db, nil)
			db.EXPECT().Relation(gomock.Any(), "orders", nil).Return(rel, nil)
			rel.EXPECT().GetTableDef(gomock.Any()).Return(tc.def)

			err := handleLoadTable(context.Background(), ses, &tree.LoadTable{
				Table: tree.NewTableName("orders", tree.ObjectNamePrefix{}, nil),
				Path:  t.TempDir(),
			})
			require.Error(t, err)
		})
	}
}

func TestHandleLoadTableRejectsInvalidFixture(t *testing.T) {
	for _, tc := range []struct {
		name          string
		writeReady    bool
		wrongHash     bool
		emptyTopology bool
		foreignKey    bool
	}{
		{name: "missing-ready"},
		{name: "schema-mismatch", writeReady: true, wrongHash: true},
		{name: "topology-mismatch", writeReady: true, emptyTopology: true},
		{name: "foreign-key", writeReady: true, foreignKey: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ses := newTestSession(t, ctrl)
			defer ses.Close()
			ses.SetDatabaseName("tpch")
			eng := mock_frontend.NewMockEngine(ctrl)
			db := mock_frontend.NewMockDatabase(ctrl)
			rel := mock_frontend.NewMockRelation(ctrl)
			ses.txnHandler.storage = eng
			eng.EXPECT().Database(gomock.Any(), "tpch", gomock.Any()).Return(db, nil).AnyTimes()
			db.EXPECT().Relation(gomock.Any(), "orders", nil).Return(rel, nil)
			def := &plan.TableDef{Name: "orders", Cols: []*plan.ColDef{{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}, Default: &plan.Default{}}}}
			if tc.foreignKey {
				def.Fkeys = []*plan.ForeignKeyDef{{Name: "fk_parent"}}
			}
			rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
			rel.EXPECT().GetTableName().Return("orders").AnyTimes()
			hash, err := tableSchemaHash(def)
			require.NoError(t, err)
			manifest := &tableDumpManifest{Version: tableDumpFormatVersion, SchemaHash: hash}
			if tc.wrongHash {
				manifest.SchemaHash = "wrong"
			} else if !tc.emptyTopology {
				manifest.Relations = []tableDumpRelation{{Role: "main", SchemaHash: hash}}
			}
			root := t.TempDir()
			fs, err := fileservice.NewLocalETLFS("fixture", root)
			require.NoError(t, err)
			require.NoError(t, writeTableDumpJSON(context.Background(), fs, tableDumpManifestName, manifest))
			if tc.writeReady {
				require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{FilePath: tableDumpReadyName, Entries: []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte("1")}}}))
			}
			err = handleLoadTable(context.Background(), ses, &tree.LoadTable{
				Table: tree.NewTableName("orders", tree.ObjectNamePrefix{}, nil), Path: root,
			})
			require.Error(t, err)
		})
	}
}

func TestTableDumpManifestAndCopy(t *testing.T) {
	ctx := context.Background()
	src, err := fileservice.NewLocalETLFS("src", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	fixture, err := fileservice.NewLocalETLFS("fixture", t.TempDir())
	require.NoError(t, err)

	content := []byte("native object bytes")
	require.NoError(t, src.Write(ctx, fileservice.IOVector{
		FilePath: "obj", Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
	}))
	size, hash, err := copyTableDumpFile(ctx, src, fixture, "obj", "objects/obj")
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), size)

	manifest := &tableDumpManifest{Version: tableDumpFormatVersion, Relations: []tableDumpRelation{{
		Role: "main", Objects: []tableDumpObject{{
			Name: "obj", Size: size, SHA256: hash, FixturePath: "objects/obj",
		}},
	}}}
	require.NoError(t, writeTableDumpJSON(ctx, fixture, tableDumpManifestName, manifest))
	read, err := readTableDumpManifest(ctx, fixture)
	require.NoError(t, err)
	require.Equal(t, manifest.Relations, read.Relations)
	created, err := installTableDumpObject(ctx, fixture, dst, read.Relations[0].Objects[0])
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, verifyTableDumpObject(ctx, dst, "obj", size, hash))
	created, err = installTableDumpObject(ctx, fixture, dst, read.Relations[0].Objects[0])
	require.NoError(t, err)
	require.False(t, created)

	serverSideDst, err := fileservice.NewLocalETLFS("server-side-dst", t.TempDir())
	require.NoError(t, err)
	serverSideCopier := &testTableDumpObjectCopier{FileService: serverSideDst, data: content}
	created, err = installTableDumpObject(ctx, fixture, serverSideCopier, read.Relations[0].Objects[0])
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, verifyTableDumpObject(ctx, serverSideCopier, "obj", size, hash))

	badDst, err := fileservice.NewLocalETLFS("bad-dst", t.TempDir())
	require.NoError(t, err)
	badCopier := &testTableDumpObjectCopier{FileService: badDst, data: []byte("corrupt object byte")}
	created, err = installTableDumpObject(ctx, fixture, badCopier, read.Relations[0].Objects[0])
	require.Error(t, err)
	require.True(t, created)
	_, err = badDst.StatFile(ctx, "obj")
	require.NoError(t, err)
}

func TestReadTableDumpManifestRejectsInvalidInput(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewLocalETLFS("fixture", t.TempDir())
	require.NoError(t, err)
	_, err = readTableDumpManifest(ctx, fs)
	require.Error(t, err)

	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: tableDumpManifestName,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte("{")}},
	}))
	_, err = readTableDumpManifest(ctx, fs)
	require.Error(t, err)
	require.NoError(t, fs.Delete(ctx, tableDumpManifestName))

	require.NoError(t, writeTableDumpJSON(ctx, fs, tableDumpManifestName, &tableDumpManifest{Version: tableDumpFormatVersion + 1}))
	_, err = readTableDumpManifest(ctx, fs)
	require.Error(t, err)
	require.Error(t, writeTableDumpJSON(ctx, fs, "bad.json", make(chan int)))
}

func TestDecodeTableDumpManifestBoundsCollectionsBeforeMaterializing(t *testing.T) {
	emptyObjects := func(count int) string {
		var builder strings.Builder
		builder.Grow(count*3 + 2)
		builder.WriteByte('[')
		for i := range count {
			if i != 0 {
				builder.WriteByte(',')
			}
			builder.WriteString("{}")
		}
		builder.WriteByte(']')
		return builder.String()
	}

	_, err := decodeTableDumpManifest([]byte(fmt.Sprintf(
		`{"version":%d,"relations":%s}`,
		tableDumpFormatVersion,
		emptyObjects(tableDumpMaxRelations+1),
	)))
	require.ErrorContains(t, err, "more than 4096 relations")

	_, err = decodeTableDumpManifest([]byte(fmt.Sprintf(
		`{"version":%d,"relations":[{"objects":%s}]}`,
		tableDumpFormatVersion,
		emptyObjects(tableDumpMaxObjects+1),
	)))
	require.ErrorContains(t, err, "more than 250000 objects")
}

func TestDecodeTableDumpManifestUnknownAndMalformedFields(t *testing.T) {
	manifest, err := decodeTableDumpManifest([]byte(`{
		"version": 1,
		"source_database": "db",
		"source_table": "src",
		"create_sql": "create table src (a int)",
		"schema_hash": "main",
		"metadata_only": true,
		"unknown_scalar": 42,
		"unknown_nested": {"array": [1, {"deeper": [true, null]}]},
		"relations": [{
			"role": "main",
			"index_name": "",
			"index_algo_table_type": "",
			"source_table": "src",
			"schema_hash": "relation",
			"unknown_relation_field": [{"ignored": true}],
			"objects": [{"name": "object"}]
		}]
	}`))
	require.NoError(t, err)
	require.Equal(t, "db", manifest.SourceDatabase)
	require.Equal(t, "src", manifest.SourceTable)
	require.True(t, manifest.MetadataOnly)
	require.Len(t, manifest.Relations, 1)
	require.Len(t, manifest.Relations[0].Objects, 1)

	for _, tc := range []struct {
		name string
		data string
	}{
		{name: "not-object", data: `[]`},
		{name: "invalid-field-type", data: `{"version":"one"}`},
		{name: "relations-not-array", data: `{"relations":{}}`},
		{name: "relation-not-object", data: `{"relations":[[]]}`},
		{name: "objects-not-array", data: `{"relations":[{"objects":{}}]}`},
		{name: "invalid-object", data: `{"relations":[{"objects":[1]}]}`},
		{name: "trailing-data", data: `{} {}`},
		{name: "truncated-unknown-value", data: `{"unknown":[1,`},
		{name: "truncated-relations", data: `{"relations":[`},
		{name: "truncated-relation", data: `{"relations":[{"role":"main"`},
		{name: "truncated-objects", data: `{"relations":[{"objects":[`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeTableDumpManifest([]byte(tc.data))
			require.Error(t, err)
		})
	}

	manifest, err = decodeTableDumpManifest([]byte(`{"relations":null}`))
	require.NoError(t, err)
	require.Nil(t, manifest.Relations)
	manifest, err = decodeTableDumpManifest([]byte(`{"relations":[{"objects":null}]}`))
	require.NoError(t, err)
	require.Nil(t, manifest.Relations[0].Objects)
}

func TestCopyTableDumpFilePrefersObjectCopier(t *testing.T) {
	ctx := context.Background()
	src, err := fileservice.NewLocalETLFS("src", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	content := []byte("provider-side object bytes")
	require.NoError(t, src.Write(ctx, fileservice.IOVector{
		FilePath: "obj", Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
	}))
	copier := &testTableDumpObjectCopier{FileService: dst, data: content}

	size, hash, err := copyTableDumpFile(ctx, src, copier, "obj", "objects/obj")
	require.NoError(t, err)
	require.True(t, copier.copied)
	require.Equal(t, int64(len(content)), size)
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(content)), hash)
	require.NoError(t, verifyTableDumpObject(ctx, copier, "objects/obj", size, hash))
}

func TestVerifyTableDumpObjectRejectsMissingAndMismatchedData(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewLocalETLFS("objects", t.TempDir())
	require.NoError(t, err)
	content := []byte("object bytes")
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: "object",
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
	}))

	require.NoError(t, verifyTableDumpObject(ctx, fs, "object", int64(len(content)), ""))
	require.Error(t, verifyTableDumpObject(ctx, fs, "object", int64(len(content)+1), ""))
	require.Error(t, verifyTableDumpObject(ctx, fs, "object", int64(len(content)), strings.Repeat("0", sha256.Size*2)))
	require.Error(t, verifyTableDumpObject(ctx, fs, "missing", 0, ""))
	require.Error(t, verifyTableDumpObject(ctx, fs, "missing", 0, strings.Repeat("0", sha256.Size*2)))

	_, _, err = hashTableDumpFile(ctx, fs, "missing")
	require.Error(t, err)
	_, _, err = copyTableDumpFile(ctx, fs, fs, "missing", "destination")
	require.Error(t, err)
	_, _, err = streamTableDumpFile(ctx, fs, fs, "missing", "destination")
	require.Error(t, err)
}

func TestCopyTableDumpFileRejectsServerSideSizeMismatch(t *testing.T) {
	ctx := context.Background()
	src, err := fileservice.NewLocalETLFS("src", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	require.NoError(t, src.Write(ctx, fileservice.IOVector{
		FilePath: "obj", Entries: []fileservice.IOEntry{{Offset: 0, Size: 3, Data: []byte("src")}},
	}))
	copier := &testTableDumpObjectCopier{FileService: dst, data: []byte("different size")}
	_, _, err = copyTableDumpFile(ctx, src, copier, "obj", "objects/obj")
	require.Error(t, err)
}

func TestCopyTableDumpFileTracksAmbiguousWriteOwnership(t *testing.T) {
	ctx := context.Background()
	src, err := fileservice.NewLocalETLFS("src", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	content := []byte("object bytes")
	require.NoError(t, src.Write(ctx, fileservice.IOVector{
		FilePath: "obj", Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
	}))

	_, _, err = copyTableDumpFile(
		ctx, src, &testAmbiguousWriteFileService{FileService: dst}, "obj", "objects/obj",
	)
	require.Error(t, err)
	_, err = dst.StatFile(ctx, "objects/obj")
	require.NoError(t, err)
}

func TestCopyTableDumpObjectsUsesBoundedConcurrency(t *testing.T) {
	ctx := context.Background()
	src, err := fileservice.NewLocalETLFS("src", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	content := []byte("provider-side object bytes")
	objects := make([]tableDumpObject, fileservice.DefaultObjectCopyConcurrency*2)
	for i := range objects {
		name := fmt.Sprintf("obj-%d", i)
		require.NoError(t, src.Write(ctx, fileservice.IOVector{
			FilePath: name,
			Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
		}))
		objects[i] = tableDumpObject{Name: name, FixturePath: path.Join("objects", name)}
	}
	copier := &testConcurrentTableDumpObjectCopier{
		FileService: dst,
		data:        content,
		release:     make(chan struct{}),
	}
	type copyResult struct {
		written []string
		err     error
	}
	resultCh := make(chan copyResult, 1)
	go func() {
		written, err := copyTableDumpObjects(ctx, src, copier, objects)
		resultCh <- copyResult{written: written, err: err}
	}()
	require.Eventually(t, func() bool {
		return copier.maximum.Load() == fileservice.DefaultObjectCopyConcurrency
	}, 5*time.Second, 10*time.Millisecond)
	close(copier.release)
	result := <-resultCh
	require.NoError(t, result.err)
	require.Len(t, result.written, len(objects))
	require.Equal(t, int32(fileservice.DefaultObjectCopyConcurrency), copier.maximum.Load())
	for i := range objects {
		require.Equal(t, int64(len(content)), objects[i].Size)
		require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(content)), objects[i].SHA256)
	}
}

func TestCopyTableDumpObjectsValidatesWithBoundedConcurrency(t *testing.T) {
	ctx := context.Background()
	src, err := fileservice.NewLocalETLFS("src", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	content := []byte("provider-side object bytes")
	objects := make([]tableDumpObject, fileservice.DefaultObjectCopyConcurrency*2)
	for i := range objects {
		name := fmt.Sprintf("obj-%d", i)
		require.NoError(t, src.Write(ctx, fileservice.IOVector{
			FilePath: name,
			Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
		}))
		objects[i] = tableDumpObject{Name: name, FixturePath: path.Join("objects", name)}
	}
	copyRelease := make(chan struct{})
	close(copyRelease)
	copier := &testConcurrentTableDumpObjectCopier{
		FileService: dst,
		data:        content,
		release:     copyRelease,
		statRelease: make(chan struct{}),
	}
	released := false
	defer func() {
		if !released {
			close(copier.statRelease)
		}
	}()
	type copyResult struct {
		written []string
		err     error
	}
	resultCh := make(chan copyResult, 1)
	go func() {
		written, err := copyTableDumpObjects(ctx, src, copier, objects)
		resultCh <- copyResult{written: written, err: err}
	}()
	require.Eventually(t, func() bool {
		return copier.statMaximum.Load() == fileservice.DefaultObjectCopyConcurrency
	}, 5*time.Second, 10*time.Millisecond)
	close(copier.statRelease)
	released = true
	result := <-resultCh
	require.NoError(t, result.err)
	require.Len(t, result.written, len(objects))
	require.Equal(t, int32(fileservice.DefaultObjectCopyConcurrency), copier.statMaximum.Load())
	for i := range objects {
		require.Equal(t, int64(len(content)), objects[i].Size)
		require.Equal(t, fmt.Sprintf("%x", sha256.Sum256(content)), objects[i].SHA256)
	}
}

func TestInstallTableDumpObjectsUsesBoundedConcurrency(t *testing.T) {
	ctx := context.Background()
	fixture, err := fileservice.NewLocalETLFS("fixture", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	content := []byte("provider-side object bytes")
	objects := make([]tableDumpObject, fileservice.DefaultObjectCopyConcurrency*2)
	for i := range objects {
		name := fmt.Sprintf("obj-%d", i)
		fixturePath := path.Join("objects", name)
		require.NoError(t, fixture.Write(ctx, fileservice.IOVector{
			FilePath: fixturePath,
			Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
		}))
		objects[i] = tableDumpObject{Name: name, FixturePath: fixturePath, Size: int64(len(content))}
	}
	copier := &testConcurrentTableDumpObjectCopier{
		FileService: dst,
		data:        content,
		release:     make(chan struct{}),
	}
	released := false
	defer func() {
		if !released {
			close(copier.release)
		}
	}()
	resultCh := make(chan error, 1)
	var tracked atomic.Int32
	go func() {
		resultCh <- installTableDumpObjects(ctx, fixture, copier, objects, func(string) {
			tracked.Add(1)
		})
	}()
	require.Eventually(t, func() bool {
		return copier.maximum.Load() == fileservice.DefaultObjectCopyConcurrency
	}, 5*time.Second, 10*time.Millisecond)
	close(copier.release)
	released = true
	require.NoError(t, <-resultCh)
	require.Equal(t, int32(fileservice.DefaultObjectCopyConcurrency), copier.maximum.Load())
	require.Equal(t, int32(len(objects)), tracked.Load())
	for i := range objects {
		require.NoError(t, verifyTableDumpObject(ctx, copier, objects[i].Name, objects[i].Size, ""))
	}
}

func TestInstallTableDumpObjectsCancelsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fixture, err := fileservice.NewLocalETLFS("fixture", t.TempDir())
	require.NoError(t, err)
	dst, err := fileservice.NewLocalETLFS("dst", t.TempDir())
	require.NoError(t, err)
	content := []byte("provider-side object bytes")
	objects := make([]tableDumpObject, fileservice.DefaultObjectCopyConcurrency*2)
	for i := range objects {
		name := fmt.Sprintf("obj-%d", i)
		fixturePath := path.Join("objects", name)
		require.NoError(t, fixture.Write(ctx, fileservice.IOVector{
			FilePath: fixturePath,
			Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(content)), Data: content}},
		}))
		objects[i] = tableDumpObject{Name: name, FixturePath: fixturePath, Size: int64(len(content))}
	}
	copier := &testConcurrentTableDumpObjectCopier{
		FileService: dst,
		data:        content,
		release:     make(chan struct{}),
	}
	resultCh := make(chan error, 1)
	var tracked atomic.Int32
	go func() {
		resultCh <- installTableDumpObjects(ctx, fixture, copier, objects, func(string) {
			tracked.Add(1)
		})
	}()
	require.Eventually(t, func() bool {
		return copier.maximum.Load() == fileservice.DefaultObjectCopyConcurrency
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-resultCh, context.Canceled)
	require.Positive(t, tracked.Load(), "ambiguous partial copies must be tracked for rollback")
}

func TestTableSchemaHashIgnoresIdentity(t *testing.T) {
	left := &plan.TableDef{TblId: 1, DbId: 2, DbName: "a", Name: "t", Createsql: "create table a.t(a int)", Cols: []*plan.ColDef{{Name: "a"}}}
	right := &plan.TableDef{TblId: 9, DbId: 8, DbName: "b", Name: "t2", Createsql: "create table b.t2(a int)", Cols: []*plan.ColDef{{Name: "a"}}}
	leftHash, err := tableSchemaHash(left)
	require.NoError(t, err)
	rightHash, err := tableSchemaHash(right)
	require.NoError(t, err)
	require.Equal(t, leftHash, rightHash)

	right.Createsql = "create table b.t2(b int)"
	rightHash, err = tableSchemaHash(right)
	require.NoError(t, err)
	require.NotEqual(t, leftHash, rightHash)
}

func TestTableSchemaHashExpandsCreateLike(t *testing.T) {
	column := func(typ types.T) *plan.ColDef {
		return &plan.ColDef{
			Name: "a",
			Typ:  plan.Type{Id: int32(typ)},
			Default: &plan.Default{
				NullAbility: false,
			},
		}
	}
	left := &plan.TableDef{Name: "src", Createsql: "create table src(a bigint)", Cols: []*plan.ColDef{column(types.T_int64)}}
	right := &plan.TableDef{Name: "dst", Createsql: "create table dst like src", Cols: []*plan.ColDef{column(types.T_int64)}}
	leftHash, err := tableSchemaHash(left)
	require.NoError(t, err)
	rightHash, err := tableSchemaHash(right)
	require.NoError(t, err)
	require.Equal(t, leftHash, rightHash)

	right.Cols[0] = column(types.T_int32)
	rightHash, err = tableSchemaHash(right)
	require.NoError(t, err)
	require.NotEqual(t, leftHash, rightHash)
}

func TestTableSchemaHashFallback(t *testing.T) {
	def := &plan.TableDef{
		TblId: 10, DbId: 20, DbName: "db", Name: "table", OriginalName: "old",
		Cols: []*plan.ColDef{{Name: "a"}},
	}
	got, err := tableSchemaHash(def)
	require.NoError(t, err)
	require.NotEmpty(t, got)

	clone := *def
	clone.TblId = 99
	clone.DbId = 88
	clone.DbName = "other"
	clone.Name = "renamed"
	clone.OriginalName = "different"
	want, err := tableSchemaHash(&clone)
	require.NoError(t, err)
	require.Equal(t, want, got)

	_, err = tableSchemaHash(nil)
	require.Error(t, err)
	_, err = tableSchemaHash(&plan.TableDef{Createsql: "select 1", Cols: []*plan.ColDef{{Name: "a"}}})
	require.Error(t, err)
	_, err = tableSchemaHash(&plan.TableDef{Createsql: "not valid sql", Cols: []*plan.ColDef{{Name: "a"}}})
	require.Error(t, err)
}
