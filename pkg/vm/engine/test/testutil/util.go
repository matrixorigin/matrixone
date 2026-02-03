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

package testutil

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

func GetDefaultTestPath(module string, t *testing.T) string {
	usr, _ := user.Current()
	dirName := fmt.Sprintf("%s-ut-workspace", usr.Username)
	return filepath.Join("/tmp", dirName, module, t.Name())
}

func MakeDefaultTestPath(module string, t *testing.T) string {
	path := GetDefaultTestPath(module, t)
	err := os.MkdirAll(path, os.FileMode(0755))
	assert.Nil(t, err)
	return path
}

type TestDisttaeEngineOptions func(*TestDisttaeEngine)

func WithDisttaeEngineMPool(mp *mpool.MPool) TestDisttaeEngineOptions {
	return func(e *TestDisttaeEngine) {
		e.mp = mp
	}
}
func WithDisttaeEngineInsertEntryMaxCount(v int) TestDisttaeEngineOptions {
	return func(e *TestDisttaeEngine) {
		e.insertEntryMaxCount = v
	}
}
func WithDisttaeEngineCommitWorkspaceThreshold(v uint64) TestDisttaeEngineOptions {
	return func(e *TestDisttaeEngine) {
		e.commitWorkspaceThreshold = v
	}
}
func WithDisttaeEngineWriteWorkspaceThreshold(v uint64) TestDisttaeEngineOptions {
	return func(e *TestDisttaeEngine) {
		e.writeWorkspaceThreshold = v
	}
}
func WithDisttaeEngineQuota(v uint64) TestDisttaeEngineOptions {
	return func(e *TestDisttaeEngine) {
		e.quota = v
	}
}

func CreateEngines(
	ctx context.Context,
	opts TestOptions,
	t *testing.T,
	funcOpts ...TestDisttaeEngineOptions,
) (
	disttaeEngine *TestDisttaeEngine,
	taeEngine *TestTxnStorage,
	rpcAgent *MockRPCAgent,
	mp *mpool.MPool,
) {

	if v := ctx.Value(defines.TenantIDKey{}); v == nil {
		panic("cannot find account id in ctx")
	}

	_, ok := ctx.Deadline()
	if ok {
		panic("context should not have deadline")
	}

	var err error

	rpcAgent = NewMockLogtailAgent()

	rootDir := GetDefaultTestPath("engine_test", t)
	os.RemoveAll(rootDir)

	s3Op, err := getS3SharedFileServiceOption(ctx, rootDir)
	require.NoError(t, err)

	if opts.TaeEngineOptions == nil {
		opts.TaeEngineOptions = &options.Options{}
	}

	opts.TaeEngineOptions.Fs = s3Op.Fs

	taeDir := path.Join(rootDir, "tae")

	taeEngine, err = NewTestTAEEngine(ctx, taeDir, t, rpcAgent, opts.TaeEngineOptions)
	require.Nil(t, err)

	disttaeEngine, err = NewTestDisttaeEngine(ctx, taeEngine.GetDB().Runtime.Fs, rpcAgent, taeEngine, funcOpts...)
	require.Nil(t, err)

	mp = disttaeEngine.mp
	disttaeEngine.rootDir = rootDir

	return
}

func GetDefaultTNShard() metadata.TNShard {
	return metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{
			ShardID:    0,
			LogShardID: 1,
		},
		ReplicaID: 0x2f,
		Address:   "echo to test",
	}
}

func EngineTableDefBySchema(schema *catalog.Schema) ([]engine.TableDef, error) {
	var defs = make([]engine.TableDef, 0)
	for idx := range schema.ColDefs {
		if schema.ColDefs[idx].Name == catalog2.Row_ID {
			continue
		}

		defs = append(defs, &engine.AttributeDef{
			Attr: engine.Attribute{
				Type:          schema.ColDefs[idx].Type,
				IsRowId:       schema.ColDefs[idx].Name == catalog2.Row_ID,
				Name:          schema.ColDefs[idx].Name,
				ID:            uint64(schema.ColDefs[idx].Idx),
				Primary:       schema.ColDefs[idx].IsPrimary(),
				IsHidden:      schema.ColDefs[idx].IsHidden(),
				Seqnum:        schema.ColDefs[idx].SeqNum,
				ClusterBy:     schema.ColDefs[idx].ClusterBy,
				AutoIncrement: schema.ColDefs[idx].AutoIncrement,
			},
		})
	}

	if schema.Constraint != nil {
		var con engine.ConstraintDef
		err := con.UnmarshalBinary(schema.Constraint)
		if err != nil {
			return nil, err
		}

		// If schema has PK but ConstraintDef doesn't contain PrimaryKeyDef, add it
		// This happens when using fake PK (pkIdx == -1 in MockSchemaAll)
		hasPKConstraint := false
		for _, ct := range con.Cts {
			if _, ok := ct.(*engine.PrimaryKeyDef); ok {
				hasPKConstraint = true
				break
			}
		}

		if !hasPKConstraint {
			if pkCol := schema.GetPrimaryKey(); pkCol != nil {
				pkConstraint := &engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						PkeyColName: pkCol.Name,
						Names:       []string{pkCol.Name},
					},
				}
				con.Cts = append(con.Cts, pkConstraint)
			}
		}

		defs = append(defs, &con)
	}

	return defs, nil
}

func EngineDefAddIndex(defs []engine.TableDef, idxColName string) []engine.TableDef {
	indexes := []*plan.IndexDef{
		{
			IndexName:          "hnsw_idx",
			TableExist:         true,
			IndexAlgo:          catalog2.MoIndexHnswAlgo.ToString(),
			IndexAlgoTableType: catalog2.Hnsw_TblType_Metadata,
			IndexTableName:     "meta_tbl",
			Parts:              []string{idxColName},
			IndexAlgoParams:    `{"m":"16","ef_construction":"200","ef_search":"100","op_type":"vector_l2_ops"}`,
		},
		{
			IndexName:          "hnsw_idx",
			TableExist:         true,
			IndexAlgo:          catalog2.MoIndexHnswAlgo.ToString(),
			IndexAlgoTableType: catalog2.Hnsw_TblType_Storage,
			IndexTableName:     "storage_tbl",
			Parts:              []string{idxColName},
			IndexAlgoParams:    `{"m":"16","ef_construction":"200","ef_search":"100","op_type":"vector_l2_ops"}`,
		},
	}
	for _, def := range defs {
		if con, ok := def.(*engine.ConstraintDef); ok {
			con.Cts = append(con.Cts, &engine.IndexDef{
				Indexes: indexes,
			})
		}
	}
	return defs
}

func PlanTableDefBySchema(schema *catalog.Schema, tableId uint64, databaseName string) plan.TableDef {
	tblDef := plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{},
	}

	tblDef.Name = schema.Name
	tblDef.TblId = tableId

	for idx := range schema.ColDefs {
		tblDef.Cols = append(tblDef.Cols, &plan.ColDef{
			ColId:     uint64(schema.ColDefs[idx].Idx),
			Name:      schema.ColDefs[idx].Name,
			Hidden:    schema.ColDefs[idx].Hidden,
			NotNull:   !schema.ColDefs[idx].Nullable(),
			TblName:   schema.Name,
			DbName:    databaseName,
			ClusterBy: schema.ColDefs[idx].ClusterBy,
			Primary:   schema.ColDefs[idx].IsPrimary(),
			Pkidx:     int32(schema.GetPrimaryKey().Idx),
			Typ: plan.Type{
				Id:          int32(schema.ColDefs[idx].Type.Oid),
				NotNullable: !schema.ColDefs[idx].Nullable(),
				Width:       schema.ColDefs[idx].Type.Oid.ToType().Width,
			},
			Seqnum: uint32(schema.ColDefs[idx].Idx),
		})
	}

	tblDef.Pkey.PkeyColName = schema.GetPrimaryKey().Name
	tblDef.Pkey.PkeyColId = uint64(schema.GetPrimaryKey().Idx)
	tblDef.Pkey.Names = append(tblDef.Pkey.Names, schema.GetPrimaryKey().Name)
	tblDef.Pkey.CompPkeyCol = nil
	tblDef.Pkey.Cols = append(tblDef.Pkey.Cols, uint64(schema.GetPrimaryKey().Idx))

	tblDef.Name2ColIndex = make(map[string]int32)
	for idx := range schema.ColDefs {
		tblDef.Name2ColIndex[schema.ColDefs[idx].Name] = int32(schema.ColDefs[idx].Idx)
	}

	return tblDef
}

func NewDefaultTableReader(
	ctx context.Context,
	rel engine.Relation,
	expr *plan.Expr,
	mp *mpool.MPool,
	ranges engine.RelData,
	snapshotTS timestamp.Timestamp,
	e *disttae.Engine,
	txnOffset int,
) (engine.Reader, error) {

	source, err := disttae.BuildLocalDataSource(ctx, rel, ranges, txnOffset)
	if err != nil {
		return nil, err
	}

	return readutil.NewReader(
		ctx,
		mp,
		e.PackerPool(),
		e.FS(),
		rel.GetTableDef(ctx),
		snapshotTS,
		expr,
		source,
		readutil.GetThresholdForReader(1),
		engine.FilterHint{Must: true},
	)
}

type EnginePack struct {
	D       *TestDisttaeEngine
	T       *TestTxnStorage
	R       *MockRPCAgent
	Mp      *mpool.MPool
	Ctx     context.Context
	t       *testing.T
	cancelF func()
}

func InitEnginePack(opts TestOptions, t *testing.T) *EnginePack {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(0))
	pack := &EnginePack{
		t: t,
	}
	pack.D, pack.T, pack.R, pack.Mp = CreateEngines(ctx, opts, t, opts.DisttaeOptions...)
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	ctx, c := context.WithTimeoutCause(ctx, timeout, moerr.CauseInitEnginePack)
	pack.Ctx = ctx
	pack.cancelF = func() {
		c()
		cancel()
	}
	return pack
}

func (p *EnginePack) Close() {
	p.D.Close(p.Ctx)
	p.T.Close(true)
	p.R.Close()
	p.cancelF()
}

func (p *EnginePack) StartCNTxn(opts ...client.TxnOption) client.TxnOperator {
	op, err := p.D.NewTxnOperator(p.Ctx, p.D.Now(), opts...)
	require.NoError(p.t, err)
	require.NoError(p.t, p.D.Engine.New(p.Ctx, op))
	return op
}

func (p *EnginePack) CreateDB(txnop client.TxnOperator, dbname string) engine.Database {
	err := p.D.Engine.Create(p.Ctx, dbname, txnop)
	require.NoError(p.t, err)
	db, err := p.D.Engine.Database(p.Ctx, dbname, txnop)
	require.NoError(p.t, err)
	return db
}

func (p *EnginePack) CreateDBAndTable(txnop client.TxnOperator, dbname string, schema *catalog.Schema) (engine.Database, engine.Relation) {
	db, rels := p.CreateDBAndTables(txnop, dbname, schema)
	return db, rels[0]
}

func (p *EnginePack) CreateDBAndTables(txnop client.TxnOperator, dbname string, schema ...*catalog.Schema) (engine.Database, []engine.Relation) {
	db := p.CreateDB(txnop, dbname)
	rels := make([]engine.Relation, 0, len(schema))
	for _, s := range schema {
		defs, err := catalog.SchemaToDefs(s)
		require.NoError(p.t, err)
		require.NoError(p.t, db.Create(p.Ctx, s.Name, defs))
		tbl, err := db.Relation(p.Ctx, s.Name, nil)
		require.NoError(p.t, err)
		rels = append(rels, tbl)
	}
	return db, rels
}

func (p *EnginePack) CreateTableInDB(txnop client.TxnOperator, dbname string, schema *catalog.Schema) engine.Relation {
	db, err := p.D.Engine.Database(p.Ctx, dbname, txnop)
	require.NoError(p.t, err)
	defs, err := catalog.SchemaToDefs(schema)
	require.NoError(p.t, err)
	require.NoError(p.t, db.Create(p.Ctx, schema.Name, defs))
	tbl, err := db.Relation(p.Ctx, schema.Name, nil)
	require.NoError(p.t, err)
	return tbl
}

func (p *EnginePack) DeleteTableInDB(txnop client.TxnOperator, dbname, tblname string) {
	db, err := p.D.Engine.Database(p.Ctx, dbname, txnop)
	require.NoError(p.t, err)
	require.NoError(p.t, db.Delete(p.Ctx, tblname))
}

func EmptyBatchFromSchema(schema *catalog.Schema, sels ...int) *batch.Batch {
	if len(sels) == 0 {
		ret := batch.NewWithSize(len(schema.ColDefs))
		for i, col := range schema.ColDefs {
			vec := vector.NewVec(col.Type.Oid.ToType())
			ret.Vecs[i] = vec
			ret.Attrs = append(ret.Attrs, col.Name)
		}
		return ret
	}
	ret := batch.NewWithSize(len(sels))
	for i, sel := range sels {
		col := schema.ColDefs[sel]
		vec := vector.NewVec(col.Type.Oid.ToType())
		ret.Vecs[i] = vec
		ret.Attrs = append(ret.Attrs, col.Name)
	}
	return ret
}

func TxnRanges(
	ctx context.Context,
	txn client.TxnOperator,
	relation engine.Relation,
	exprs []*plan.Expr,
) (engine.RelData, error) {
	rangesParam := engine.RangesParam{
		BlockFilters:   exprs,
		PreAllocBlocks: 2,
		TxnOffset:      txn.GetWorkspace().GetSnapshotWriteOffset(),
		Policy:         engine.Policy_CollectAllData,
	}
	return relation.Ranges(ctx, rangesParam)
}

func GetRelationReader(
	ctx context.Context,
	e *TestDisttaeEngine,
	txn client.TxnOperator,
	relation engine.Relation,
	exprs []*plan.Expr,
	mp *mpool.MPool,
	t *testing.T,
) (reader engine.Reader, err error) {
	ranges, err := TxnRanges(ctx, txn, relation, exprs)
	require.NoError(t, err)
	var expr *plan.Expr
	if len(exprs) > 0 {
		expr = exprs[0]
	}
	reader, err = NewDefaultTableReader(
		ctx,
		relation,
		expr,
		mp,
		ranges,
		txn.SnapshotTS(),
		e.Engine,
		txn.GetWorkspace().GetSnapshotWriteOffset())
	require.NoError(t, err)
	return
}

func GetTableTxnReader(
	ctx context.Context,
	e *TestDisttaeEngine,
	dbName, tableName string,
	exprs []*plan.Expr,
	mp *mpool.MPool,
	t *testing.T,
) (
	txn client.TxnOperator,
	relation engine.Relation,
	reader engine.Reader,
	err error,
) {
	_, relation, txn, err = e.GetTable(ctx, dbName, tableName)
	require.NoError(t, err)
	ranges, err := TxnRanges(ctx, txn, relation, exprs)
	require.NoError(t, err)
	var expr *plan.Expr
	if len(exprs) > 0 {
		expr = exprs[0]
	}
	reader, err = NewDefaultTableReader(
		ctx,
		relation,
		expr,
		mp,
		ranges,
		txn.SnapshotTS(),
		e.Engine,
		txn.GetWorkspace().GetSnapshotWriteOffset())
	require.NoError(t, err)
	return
}

func WriteToRelation(
	ctx context.Context,
	txn client.TxnOperator,
	relation engine.Relation,
	bat *batch.Batch,
	isDelete, toEndStatement bool,
) (err error) {
	txn.GetWorkspace().StartStatement()
	if isDelete {
		err = relation.Delete(ctx, bat, catalog2.Row_ID)
	} else {
		err = relation.Write(ctx, bat)
	}
	if err == nil && toEndStatement {
		EndThisStatement(ctx, txn)
	}
	return
}

func EndThisStatement(
	ctx context.Context,
	txn client.TxnOperator,
) (err error) {
	err = txn.GetWorkspace().IncrStatementID(ctx, false)
	txn.GetWorkspace().EndStatement()
	txn.GetWorkspace().UpdateSnapshotWriteOffset()

	return
}

func MakeTxnHeartbeatMonkeyJob(
	e *TestTxnStorage,
	opInterval time.Duration,
) *tasks.CancelableJob {
	taeDB := e.GetDB()
	return tasks.NewCancelableCronJob(
		"txn-heartbeat-monkey",
		opInterval,
		func(ctx context.Context) {
			if v := rand.Intn(100); v > 50 {
				taeDB.StopTxnHeartbeat()
			} else {
				taeDB.ResetTxnHeartbeat()
			}
		},
		false,
		1,
	)
}
