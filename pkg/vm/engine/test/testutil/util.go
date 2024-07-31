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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func CreateEngines(ctx context.Context, opts TestOptions,
	t *testing.T) (disttaeEngine *TestDisttaeEngine, taeEngine *TestTxnStorage,
	rpcAgent *MockRPCAgent, mp *mpool.MPool) {

	if v := ctx.Value(defines.TenantIDKey{}); v == nil {
		panic("cannot find account id in ctx")
	}

	var err error

	mp, err = mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)

	rpcAgent = NewMockLogtailAgent()

	taeEngine, err = NewTestTAEEngine(ctx, "partition_state", t, rpcAgent, opts.TaeEngineOptions)
	require.Nil(t, err)

	disttaeEngine, err = NewTestDisttaeEngine(ctx, mp, taeEngine.GetDB().Runtime.Fs.Service, rpcAgent, taeEngine)
	require.Nil(t, err)

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

func (p *EnginePack) CreateDBAndTables(txnop client.TxnOperator, dbname string, schema ...*catalog2.Schema) (engine.Database, []engine.Relation) {
	db := p.CreateDB(txnop, dbname)
	rels := make([]engine.Relation, 0, len(schema))
	for _, s := range schema {
		defs, err := catalog2.SchemaToDefs(s)
		require.NoError(p.t, err)
		require.NoError(p.t, db.Create(p.Ctx, s.Name, defs))
		tbl, err := db.Relation(p.Ctx, s.Name, nil)
		require.NoError(p.t, err)
		rels = append(rels, tbl)
	}
	return db, rels
}

func (p *EnginePack) CreateTableInDB(txnop client.TxnOperator, dbname string, schema *catalog2.Schema) engine.Relation {
	db, err := p.D.Engine.Database(p.Ctx, dbname, txnop)
	require.NoError(p.t, err)
	defs, err := catalog2.SchemaToDefs(schema)
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
