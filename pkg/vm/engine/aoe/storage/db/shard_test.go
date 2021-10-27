package db

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/stretchr/testify/assert"
)

type mockShard struct {
	sm.ClosedState
	sm.StateMachine
	id      uint64
	idAlloc common.IdAlloctor
	queue   sm.Queue
	inst    *DB
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

func newMockShard(shardId uint64, inst *DB) *mockShard {
	s := &mockShard{
		id:   shardId,
		inst: inst,
	}
	wg := new(sync.WaitGroup)
	s.queue = sm.NewWaitableQueue(1000, 1, s, wg, nil, nil, s.onItems)
	s.queue.Start()
	return s
}

func (s *mockShard) Stop() {
	s.queue.Stop()
}

func (s *mockShard) sendRequest(ctx *requestCtx) {
	_, err := s.queue.Enqueue(ctx)
	if err != nil {
		ctx.Done()
	}
}

func (s *mockShard) onItems(items ...interface{}) {
	item := items[0]
	ctx := item.(*requestCtx)
	switch r := ctx.request.(type) {
	case *aoe.TableInfo:
		err := s.createTable(r)
		ctx.setDone(err, nil)
	case *dbi.DropTableCtx:
		err := s.dropTable(r)
		ctx.setDone(err, nil)
	case *dbi.AppendCtx:
		err := s.insert(r)
		ctx.setDone(err, nil)
	default:
		panic("")
	}
}

func (s *mockShard) createTable(info *aoe.TableInfo) error {
	_, err := s.inst.CreateTable(info, dbi.TableOpCtx{
		TableName: info.Name,
		OpIndex:   s.idAlloc.Alloc(),
		ShardId:   s.id,
	})
	return err
}

func (s *mockShard) dropTable(ctx *dbi.DropTableCtx) error {
	ctx.ShardId = s.id
	ctx.OpIndex = s.idAlloc.Alloc()
	_, err := s.inst.DropTable(*ctx)
	return err
}

func (s *mockShard) insert(ctx *dbi.AppendCtx) error {
	ctx.ShardId = s.id
	ctx.OpIndex = s.idAlloc.Alloc()
	ctx.OpSize = 1
	err := s.inst.Append(*ctx)
	return err
}

func (s *mockShard) getSafeId() uint64 {
	return s.inst.Wal.GetShardCheckpointId(s.id)
}

type mockClient struct {
	t      *testing.T
	infos  []*aoe.TableInfo
	router map[string]int
	shards []*mockShard
	bats   []*batch.Batch
}

func newClient(t *testing.T, shards []*mockShard, infos []*aoe.TableInfo,
	bats []*batch.Batch) *mockClient {
	router := make(map[string]int)
	for i, info := range infos {
		routed := i % len(shards)
		router[info.Name] = routed
	}
	return &mockClient{
		t:      t,
		infos:  infos,
		shards: shards,
		router: router,
		bats:   bats,
	}
}

func (cli *mockClient) routing(name string) *mockShard {
	shardPos := cli.router[name]
	shard := cli.shards[shardPos]
	// cli.t.Logf("table-%s routed to shard-%d", info.Name, shard.id)
	return shard
}

func (cli *mockClient) insert(pos int) error {
	info := cli.infos[pos]
	shard := cli.routing(info.Name)
	ctx := newCtx()
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(len(cli.bats))
	ctx.request = &dbi.AppendCtx{TableName: info.Name, Data: cli.bats[n]}
	shard.sendRequest(ctx)
	ctx.Wait()
	return ctx.err
}

func (cli *mockClient) dropTable(pos int) error {
	info := cli.infos[pos]
	shard := cli.routing(info.Name)
	ctx := newCtx()
	ctx.request = &dbi.DropTableCtx{TableName: info.Name}
	shard.sendRequest(ctx)
	ctx.Wait()
	return ctx.err
}

func (cli *mockClient) createTable(pos int) error {
	info := cli.infos[pos]
	shard := cli.routing(info.Name)
	ctx := newCtx()
	ctx.request = info
	shard.sendRequest(ctx)
	ctx.Wait()
	return ctx.err
}

func TestShard1(t *testing.T) {
	tableCnt := 20
	tableInfos := make([]*aoe.TableInfo, tableCnt)
	for i := 0; i < tableCnt; i++ {
		tableInfos[i] = adaptor.MockTableInfo(20)
		tableInfos[i].Name = fmt.Sprintf("mock-%d", i)
	}

	initDBTest()
	inst := initDB(wal.BrokerRole)

	shardCnt := 8
	shards := make([]*mockShard, shardCnt)
	for i := 0; i < shardCnt; i++ {
		shards[i] = newMockShard(uint64(i), inst)
	}

	var wg sync.WaitGroup
	clients := make([]*mockClient, 2)
	for i, _ := range clients {
		clients[i] = newClient(t, shards, tableInfos[i*tableCnt/2:(i+1)*tableCnt/2], nil)
		wg.Add(1)
		go func(cli *mockClient) {
			defer wg.Done()
			for pos := 0; pos < len(cli.infos); pos++ {
				err := cli.createTable(pos)
				assert.Nil(t, err)
			}
			for pos := 0; pos < len(cli.infos); pos++ {
				err := cli.dropTable(pos)
				assert.Nil(t, err)
			}
		}(clients[i])
	}

	wg.Wait()
	for _, shard := range shards {
		testutils.WaitExpect(100, func() bool {
			return shard.idAlloc.Get() == shard.getSafeId()
		})
		assert.Equal(t, shard.idAlloc.Get(), shard.getSafeId())
		t.Logf("shard-%d safeid %d, logid-%d", shard.id, shard.getSafeId(), shard.idAlloc.Get())
		shard.Stop()
	}
	inst.Close()
}

func TestShard2(t *testing.T) {
	// Create 10 Table [0,1,2,3,4,5]
	// Insert To 10 Table [0,1,2,3,4]
	// Drop 10 Table
	tableCnt := 10
	tableInfos := make([]*aoe.TableInfo, tableCnt)
	for i := 0; i < tableCnt; i++ {
		tableInfos[i] = adaptor.MockTableInfo(20)
		tableInfos[i].Name = fmt.Sprintf("mock-%d", i)
	}

	initDBTest()
	inst := initDB(wal.BrokerRole)

	shardCnt := 4
	shards := make([]*mockShard, shardCnt)
	for i := 0; i < shardCnt; i++ {
		shards[i] = newMockShard(uint64(i), inst)
	}

	batches := make([]*batch.Batch, 10)
	for i, _ := range batches {
		step := inst.Store.Catalog.Cfg.BlockMaxRows / 10
		rows := (uint64(i) + 1) * 2 * step
		batches[i] = mock.MockBatch(adaptor.TableInfoToSchema(inst.Store.Catalog, tableInfos[0]).Types(), rows)
	}

	var wg sync.WaitGroup
	clients := make([]*mockClient, 2)
	for i, _ := range clients {
		clients[i] = newClient(t, shards, tableInfos[i*tableCnt/2:(i+1)*tableCnt/2], batches)
		wg.Add(1)
		go func(cli *mockClient) {
			defer wg.Done()
			for pos := 0; pos < len(cli.infos); pos++ {
				err := cli.createTable(pos)
				assert.Nil(t, err)
			}
			for n := 0; n < 2; n++ {
				for pos := 0; pos < len(cli.infos); pos++ {
					err := cli.insert(pos)
					assert.Nil(t, err)
				}
			}
			for pos := 0; pos < len(cli.infos); pos++ {
				err := cli.dropTable(pos)
				assert.Nil(t, err)
			}
		}(clients[i])
	}

	wg.Wait()
	for _, shard := range shards {
		testutils.WaitExpect(40, func() bool {
			return shard.idAlloc.Get() == shard.getSafeId()
		})
		// assert.Equal(t, shard.idAlloc.Get(), shard.getSafeId())
		t.Logf("shard-%d safeid %d, logid-%d", shard.id, shard.getSafeId(), shard.idAlloc.Get())
		shard.Stop()
	}
	inst.Close()
}
