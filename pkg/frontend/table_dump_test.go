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
	"errors"
	"fmt"
	"net/url"
	"path"
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

func (f *testAmbiguousWriteFileService) Write(ctx context.Context, vector fileservice.IOVector) error {
	if err := f.FileService.Write(ctx, vector); err != nil {
		return err
	}
	return errors.New("write response lost after destination was created")
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
	for _, tc := range []struct {
		stmt tree.Statement
		want PrivilegeType
	}{
		{stmt: &tree.DumpTable{Table: table}, want: PrivilegeTypeSelect},
		{stmt: &tree.LoadTable{Table: table}, want: PrivilegeTypeInsert},
	} {
		priv := determinePrivilegeSetOfStatement(tc.stmt)
		require.Equal(t, objectTypeTable, priv.objType)
		require.True(t, priv.writeDatabaseAndTableDirectly)
		require.Equal(t, []string{"tpch"}, priv.writeDatabaseTargets)
		require.Len(t, priv.entries, 3)
		require.Equal(t, tc.want, priv.entries[0].privilegeId)
		require.Equal(t, "tpch", priv.entries[0].databaseName)
	}
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

	refs, err := getTableDumpRelations(context.Background(), ses, "tpch", master)
	require.NoError(t, err)
	require.Len(t, refs, 2)
	require.Equal(t, "main", refs[0].Role)
	require.Equal(t, "index", refs[1].Role)
	require.Equal(t, "idx", refs[1].IndexName)
	require.NotEmpty(t, refs[0].SchemaHash)
	require.Equal(t, tableDumpRelationKey("index", "idx", "regular"), "index\x00idx\x00regular")
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
	rel.EXPECT().Rows(gomock.Any()).Return(uint64(0), nil)
	rel.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(workspace).AnyTimes()

	object := newTableDumpTestObject(t, false)
	content := []byte("immutable object fixture")
	object.Size = int64(len(content))
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

	stmt := &tree.LoadTable{Table: tree.NewTableName("orders", tree.ObjectNamePrefix{}, nil), Path: dumpRoot}
	require.NoError(t, handleLoadTable(context.Background(), ses, stmt))
	require.Equal(t, []string{object.Name}, workspace.protectedCloneFiles)
	require.NoError(t, verifyTableDumpObject(context.Background(), targetFS, object.Name, object.Size, ""))
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
	for _, def := range []*plan.TableDef{
		{Name: "orders", Partition: &plan.Partition{}},
		{Name: "orders", Cols: []*plan.ColDef{{Typ: plan.Type{AutoIncr: true}}}},
	} {
		t.Run(fmt.Sprintf("%p", def), func(t *testing.T) {
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
			rel.EXPECT().GetTableDef(gomock.Any()).Return(def)
			err := handleDumpTable(context.Background(), ses, &tree.DumpTable{
				Table: tree.NewTableName("orders", tree.ObjectNamePrefix{}, nil), Path: t.TempDir(),
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
	}{
		{name: "missing-ready"},
		{name: "schema-mismatch", writeReady: true, wrongHash: true},
		{name: "topology-mismatch", writeReady: true, emptyTopology: true},
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
	err = installTableDumpObject(ctx, fixture, dst, read.Relations[0].Objects[0])
	require.NoError(t, err)
	require.NoError(t, verifyTableDumpObject(ctx, dst, "obj", size, hash))

	serverSideDst, err := fileservice.NewLocalETLFS("server-side-dst", t.TempDir())
	require.NoError(t, err)
	serverSideCopier := &testTableDumpObjectCopier{FileService: serverSideDst, data: content}
	err = installTableDumpObject(ctx, fixture, serverSideCopier, read.Relations[0].Objects[0])
	require.NoError(t, err)
	require.NoError(t, verifyTableDumpObject(ctx, serverSideCopier, "obj", size, hash))

	badDst, err := fileservice.NewLocalETLFS("bad-dst", t.TempDir())
	require.NoError(t, err)
	badCopier := &testTableDumpObjectCopier{FileService: badDst, data: []byte("corrupt object byte")}
	err = installTableDumpObject(ctx, fixture, badCopier, read.Relations[0].Objects[0])
	require.Error(t, err)
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
	require.Empty(t, hash)
	require.NoError(t, verifyTableDumpObject(ctx, copier, "objects/obj", size, hash))
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
		require.Empty(t, objects[i].SHA256)
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
	go func() {
		resultCh <- installTableDumpObjects(ctx, fixture, copier, objects)
	}()
	require.Eventually(t, func() bool {
		return copier.maximum.Load() == fileservice.DefaultObjectCopyConcurrency
	}, 5*time.Second, 10*time.Millisecond)
	close(copier.release)
	released = true
	require.NoError(t, <-resultCh)
	require.Equal(t, int32(fileservice.DefaultObjectCopyConcurrency), copier.maximum.Load())
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
	go func() {
		resultCh <- installTableDumpObjects(ctx, fixture, copier, objects)
	}()
	require.Eventually(t, func() bool {
		return copier.maximum.Load() == fileservice.DefaultObjectCopyConcurrency
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-resultCh, context.Canceled)
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
