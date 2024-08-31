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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	testutil2 "github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
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

func RemoveDefaultTestPath(module string, t *testing.T) {
	path := GetDefaultTestPath(module, t)
	os.RemoveAll(path)
}

func InitTestEnv(module string, t *testing.T) string {
	RemoveDefaultTestPath(module, t)
	return MakeDefaultTestPath(module, t)
}

type TestDisttaeEngineOptions func(*TestDisttaeEngine)

func WithDisttaeEngineMPool(mp *mpool.MPool) TestDisttaeEngineOptions {
	return func(e *TestDisttaeEngine) {
		e.mp = mp
	}
}

func CreateEngines(
	ctx context.Context,
	opts TestOptions,
	t *testing.T,
	options ...TestDisttaeEngineOptions,
) (
	disttaeEngine *TestDisttaeEngine,
	taeEngine *TestTxnStorage,
	rpcAgent *MockRPCAgent,
	mp *mpool.MPool,
) {

	if v := ctx.Value(defines.TenantIDKey{}); v == nil {
		panic("cannot find account id in ctx")
	}

	var err error

	rpcAgent = NewMockLogtailAgent()

	taeEngine, err = NewTestTAEEngine(ctx, "partition_state", t, rpcAgent, opts.TaeEngineOptions)
	require.Nil(t, err)

	disttaeEngine, err = NewTestDisttaeEngine(ctx, taeEngine.GetDB().Runtime.Fs.Service, rpcAgent, taeEngine, options...)
	require.Nil(t, err)

	mp = disttaeEngine.mp

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

		defs = append(defs, &con)
	}

	return defs, nil
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
	databaseName string,
	schema *catalog.Schema,
	expr *plan.Expr,
	mp *mpool.MPool,
	ranges engine.RelData,
	snapshotTS timestamp.Timestamp,
	e *disttae.Engine,
	txnOffset int,
) (engine.Reader, error) {

	tblDef := PlanTableDefBySchema(schema, rel.GetTableID(ctx), databaseName)

	source, err := disttae.BuildLocalDataSource(ctx, rel, ranges, txnOffset)
	if err != nil {
		return nil, err
	}

	return disttae.NewReader(
		ctx,
		testutil2.NewProcessWithMPool("", mp),
		e,
		&tblDef,
		snapshotTS,
		expr,
		source,
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
	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(0))
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	pack := &EnginePack{
		Ctx:     ctx,
		t:       t,
		cancelF: cancel,
	}
	pack.D, pack.T, pack.R, pack.Mp = CreateEngines(pack.Ctx, opts, t)
	return pack
}

func (p *EnginePack) Close() {
	p.cancelF()
	p.D.Close(p.Ctx)
	p.T.Close(true)
	p.R.Close()
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

func EmptyBatchFromSchema(schema *catalog.Schema) *batch.Batch {
	ret := batch.NewWithSize(len(schema.ColDefs))
	for i, col := range schema.ColDefs {
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
	return relation.Ranges(ctx, exprs, txn.GetWorkspace().GetSnapshotWriteOffset())
}
