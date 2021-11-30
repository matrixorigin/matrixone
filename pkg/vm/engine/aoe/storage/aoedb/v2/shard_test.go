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

package aoedb

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
	"github.com/stretchr/testify/assert"
)

type mockShard struct {
	sm.ClosedState
	sm.StateMachine
	gen      *shard.MockShardIndexGenerator
	database *metadata.Database
	queue    sm.Queue
	inst     *DB
}

type requestCtx struct {
	sync.WaitGroup
	err     error
	result  interface{}
	request interface{}
}

func newCtx() *requestCtx {
	ctx := new(requestCtx)
	ctx.Add(1)
	return ctx
}

func (ctx *requestCtx) setDone(err error, result interface{}) {
	ctx.err = err
	ctx.result = result
	ctx.Done()
}

func newMockShard(inst *DB, gen *shard.MockIndexAllocator) *mockShard {
	s := &mockShard{
		inst: inst,
	}
	var err error
	dbName := strconv.FormatUint(uint64(time.Now().UnixNano()), 10)
	ctx := &CreateDBCtx{
		DB: dbName,
	}
	s.database, err = inst.CreateDatabase(ctx)
	if err != nil {
		panic(err)
	}
	s.gen = gen.Shard(s.database.GetShardId())
	wg := new(sync.WaitGroup)
	s.queue = sm.NewWaitableQueue(1000, 1, s, wg, nil, nil, s.onItems)
	s.queue.Start()
	return s
}

func (s *mockShard) Stop() {
	s.queue.Stop()
}

func (s *mockShard) sendRequest(ctx *requestCtx) {
	if ctx.request == nil {
		panic(ctx)
	}
	_, err := s.queue.Enqueue(ctx)
	if err != nil {
		ctx.Done()
	}
}

func (s *mockShard) onItems(items ...interface{}) {
	item := items[0]
	ctx := item.(*requestCtx)
	switch r := ctx.request.(type) {
	case *metadata.Schema:
		err := s.createTable(r)
		ctx.setDone(err, nil)
	case *DropTableCtx:
		err := s.dropTable(r)
		ctx.setDone(err, nil)
	case *AppendCtx:
		err := s.insert(r)
		ctx.setDone(err, nil)
	default:
		panic(r)
	}
}

func (s *mockShard) createTable(schema *metadata.Schema) error {
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(s.database, s.gen.Host),
		Schema:        schema,
	}
	_, err := s.inst.CreateTable(createCtx)
	return err
}

func (s *mockShard) dropTable(ctx *DropTableCtx) error {
	ctx.DB = s.database.Name
	ctx.Id = s.gen.Alloc()
	ctx.Size = 1
	_, err := s.inst.DropTable(ctx)
	return err
}

func (s *mockShard) insert(ctx *AppendCtx) error {
	ctx.DB = s.database.Name
	ctx.Id = s.gen.Alloc()
	ctx.Size = 1
	err := s.inst.Append(ctx)
	return err
}

func (s *mockShard) getSafeId() uint64 {
	return s.database.GetCheckpointId()
}

type mockClient struct {
	t       *testing.T
	schemas []*metadata.Schema
	router  map[string]int
	shards  []*mockShard
	bats    []*batch.Batch
}

func newClient(t *testing.T, shards []*mockShard, schemas []*metadata.Schema,
	bats []*batch.Batch) *mockClient {
	router := make(map[string]int)
	for i, info := range schemas {
		routed := i % len(shards)
		router[info.Name] = routed
	}
	return &mockClient{
		t:       t,
		schemas: schemas,
		shards:  shards,
		router:  router,
		bats:    bats,
	}
}

func (cli *mockClient) routing(name string) *mockShard {
	shardPos := cli.router[name]
	shard := cli.shards[shardPos]
	// cli.t.Logf("table-%s routed to shard-%d", info.Name, shard.id)
	return shard
}

func (cli *mockClient) insert(pos int) error {
	info := cli.schemas[pos]
	shard := cli.routing(info.Name)
	ctx := newCtx()
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(len(cli.bats))
	appendCtx := new(AppendCtx)
	appendCtx.Table = info.Name
	appendCtx.Data = cli.bats[n]
	ctx.request = appendCtx
	shard.sendRequest(ctx)
	ctx.Wait()
	return ctx.err
}

func (cli *mockClient) dropTable(pos int) error {
	info := cli.schemas[pos]
	shard := cli.routing(info.Name)
	ctx := newCtx()
	ctx.request = &DropTableCtx{
		Table: info.Name,
	}
	shard.sendRequest(ctx)
	ctx.Wait()
	return ctx.err
}

func (cli *mockClient) createTable(pos int) error {
	info := cli.schemas[pos]
	shard := cli.routing(info.Name)
	ctx := newCtx()
	ctx.request = info
	shard.sendRequest(ctx)
	ctx.Wait()
	return ctx.err
}

func TestShard1(t *testing.T) {
	tableCnt := 20
	schemas := make([]*metadata.Schema, tableCnt)
	for i := 0; i < tableCnt; i++ {
		schemas[i] = metadata.MockSchema(20)
		schemas[i].Name = fmt.Sprintf("mock-%d", i)
	}
	initTestEnv(t)

	inst, gen, _ := initTestDB2(t)

	shardCnt := 8
	shards := make([]*mockShard, shardCnt)
	for i := 0; i < shardCnt; i++ {
		shards[i] = newMockShard(inst, gen)
	}

	var wg sync.WaitGroup
	clients := make([]*mockClient, 2)
	for i, _ := range clients {
		clients[i] = newClient(t, shards, schemas[i*tableCnt/2:(i+1)*tableCnt/2], nil)
		wg.Add(1)
		go func(cli *mockClient) {
			defer wg.Done()
			for pos := 0; pos < len(cli.schemas); pos++ {
				err := cli.createTable(pos)
				assert.Nil(t, err)
			}
			for pos := 0; pos < len(cli.schemas); pos++ {
				err := cli.dropTable(pos)
				assert.Nil(t, err)
			}
		}(clients[i])
	}

	wg.Wait()
	for _, shard := range shards {
		testutils.WaitExpect(200, func() bool {
			return shard.gen.Get() == shard.getSafeId()
		})
		assert.Equal(t, shard.gen.Get(), shard.getSafeId())
		t.Logf("shard-%d safeid %d, logid-%d", shard.gen.ShardId, shard.getSafeId(), shard.gen.Get())
		shard.Stop()
	}
	inst.Close()
}

func TestShard2(t *testing.T) {
	// Create 10 Table [0,1,2,3,4,5]
	// Insert To 10 Table [0,1,2,3,4]
	// Drop 10 Table
	tableCnt := 10
	schemas := make([]*metadata.Schema, tableCnt)
	for i := 0; i < tableCnt; i++ {
		schemas[i] = metadata.MockSchema(20)
	}

	initTestEnv(t)
	inst, gen, _ := initTestDB2(t)

	shardCnt := 4
	shards := make([]*mockShard, shardCnt)
	for i := 0; i < shardCnt; i++ {
		shards[i] = newMockShard(inst, gen)
	}

	batches := make([]*batch.Batch, 10)
	for i, _ := range batches {
		step := inst.Store.Catalog.Cfg.BlockMaxRows / 10
		rows := (uint64(i) + 1) * 2 * step
		batches[i] = mock.MockBatch(schemas[0].Types(), rows)
	}

	var wg sync.WaitGroup
	clients := make([]*mockClient, 2)
	for i, _ := range clients {
		clients[i] = newClient(t, shards, schemas[i*tableCnt/2:(i+1)*tableCnt/2], batches)
		wg.Add(1)
		go func(cli *mockClient) {
			defer wg.Done()
			for pos := 0; pos < len(cli.schemas); pos++ {
				err := cli.createTable(pos)
				assert.Nil(t, err)
			}
			for n := 0; n < 2; n++ {
				for pos := 0; pos < len(cli.schemas); pos++ {
					err := cli.insert(pos)
					assert.Nil(t, err)
				}
			}
			for pos := 0; pos < len(cli.schemas); pos++ {
				err := cli.dropTable(pos)
				assert.Nil(t, err)
			}
		}(clients[i])
	}

	wg.Wait()
	for _, shard := range shards {
		testutils.WaitExpect(400, func() bool {
			return shard.gen.Get() == shard.getSafeId()
		})
		assert.Equal(t, shard.gen.Get(), shard.getSafeId())
		t.Logf("shard-%d safeid %d, logid-%d", shard.gen.ShardId, shard.getSafeId(), shard.gen.Get())
		shard.Stop()
	}

	total := 0
	for _, shard := range shards {
		view := shard.database.View(0)
		assert.Empty(t, view.Database.TableSet)
		view = shard.database.View(shard.getSafeId())
		total += len(view.Database.TableSet)
	}
	assert.Equal(t, 0, total)
	for _, shard := range shards {
		testutils.WaitExpect(400, func() bool {
			return shard.gen.Get() == shard.getSafeId()
		})
	}
	dbCompacts := 0
	tblCompacts := 0
	dbListener := new(metadata.BaseDatabaseListener)
	dbListener.DatabaseCompactedFn = func(database *metadata.Database) {
		dbCompacts++
	}
	tblListener := new(metadata.BaseTableListener)
	tblListener.TableCompactedFn = func(t *metadata.Table) {
		tblCompacts++
	}
	inst.Store.Catalog.Compact(dbListener, tblListener)
	assert.Equal(t, 0, dbCompacts)
	assert.Equal(t, tableCnt, tblCompacts)

	for _, shard := range shards {
		ctx := CreateDBMutationCtx(shard.database, gen)
		_, err := inst.DropDatabase(ctx)
		assert.Nil(t, err)
	}
	for _, shard := range shards {
		testutils.WaitExpect(400, func() bool {
			return (shard.gen.Get() == shard.getSafeId()) && (shard.database.IsHardDeleted())
		})
		assert.Equal(t, shard.gen.Get(), shard.getSafeId())
	}
	dbCompacts = 0
	tblCompacts = 0
	inst.Store.Catalog.Compact(dbListener, tblListener)
	assert.Equal(t, len(shards), dbCompacts)
	assert.Equal(t, 0, tblCompacts)

	t.Log(inst.Store.Catalog.IndexWal.String())

	inst.Close()
}

func TestShard3(t *testing.T) {
	initTestEnv(t)
	inst, gen, _ := initTestDBWithOptions(t, defaultDBPath, emptyDBName, uint64(40000), uint64(8), nil, wal.BrokerRole)
	defer inst.Close()
	t.Log(inst.Opts.CacheCfg.InsertCapacity / 1024 / 1024)
	tableCnt := 10
	schemas := make([]*metadata.Schema, tableCnt)
	for i := 0; i < tableCnt; i++ {
		schemas[i] = metadata.MockSchema(10)
	}

	shardCnt := 4
	shards := make([]*mockShard, shardCnt)
	for i := 0; i < shardCnt; i++ {
		shards[i] = newMockShard(inst, gen)
	}

	batches := make([]*batch.Batch, 10)
	for i, _ := range batches {
		step := inst.Store.Catalog.Cfg.BlockMaxRows / 4
		rows := (uint64(i) + 1) * step
		batches[i] = mock.MockBatch(schemas[0].Types(), rows)
	}

	var wg sync.WaitGroup
	clients := make([]*mockClient, 2)
	for i, _ := range clients {
		clients[i] = newClient(t, shards, schemas[i*tableCnt/2:(i+1)*tableCnt/2], batches)
		wg.Add(1)
		go func(cli *mockClient) {
			defer wg.Done()
			for pos := 0; pos < len(cli.schemas); pos++ {
				err := cli.createTable(pos)
				assert.Nil(t, err)
			}
			for n := 0; n < 2; n++ {
				for pos := 0; pos < len(cli.schemas); pos++ {
					err := cli.insert(pos)
					assert.Nil(t, err)
				}
			}
			// for pos := 0; pos < len(cli.schemas); pos++ {
			// 	err := cli.dropTable(pos)
			// 	assert.Nil(t, err)
			// }
		}(clients[i])
	}
	wg.Wait()
	prepareSnapshotPath(defaultSnapshotPath, t)
	costs := make(map[uint64]time.Duration)
	for _, shard := range shards {
		now := time.Now()
		ctx := &CreateSnapshotCtx{
			DB:   shard.database.Name,
			Path: getSnapshotPath(defaultSnapshotPath, t),
			Sync: true,
		}
		idx, err := inst.CreateSnapshot(ctx)
		assert.Nil(t, err)
		assert.Equal(t, shard.getSafeId(), idx)
		costs[shard.database.Id] = time.Since(now)
	}
	for id, cost := range costs {
		t.Logf("Create snapshot %d takes: %s", id, cost)
	}
}
