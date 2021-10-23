package db

import (
	"fmt"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/logstore/sm"
	"matrixone/pkg/vm/engine/aoe/storage/testutils"
	"sync"
	"testing"

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

func (s *mockShard) getSafeId() uint64 {
	id, _ := s.inst.Wal.GetShardSafeId(s.id)
	return id
}

type mockClient struct {
	t      *testing.T
	infos  []*aoe.TableInfo
	router map[string]int
	shards []*mockShard
}

func newClient(t *testing.T, shards []*mockShard, infos []*aoe.TableInfo) *mockClient {
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
	}
}

func (cli *mockClient) createTable(pos int) error {
	info := cli.infos[pos]
	shardPos := cli.router[info.Name]
	shard := cli.shards[shardPos]
	// cli.t.Logf("table-%s routed to shard-%d", info.Name, shard.id)
	ctx := newCtx()
	ctx.request = info
	shard.sendRequest(ctx)
	ctx.Wait()
	return ctx.err
}

func TestShard(t *testing.T) {
	// Create 10 Table [0,1,2,3,4,5,6,7,8,9]
	// Insert To 5 Table [0,1,2,3,4]
	// Drop 2 Table [4,5]
	// Insert to 4 Table [3,6,7,8]
	// Drop 2 Table [0,8]
	// Create 2 Table [4, 5]
	tableCnt := 10
	tableInfos := make([]*aoe.TableInfo, tableCnt)
	for i := 0; i < tableCnt; i++ {
		tableInfos[i] = adaptor.MockTableInfo(20)
		tableInfos[i].Name = fmt.Sprintf("mock-%d", i)
	}

	initDBTest()
	inst := initDB(storage.NORMAL_FT, false)

	shardCnt := 4
	shards := make([]*mockShard, shardCnt)
	for i := 0; i < shardCnt; i++ {
		shards[i] = newMockShard(uint64(i), inst)
	}

	var wg sync.WaitGroup
	clients := make([]*mockClient, 2)
	for i, _ := range clients {
		clients[i] = newClient(t, shards, tableInfos[i*tableCnt/2:(i+1)*tableCnt/2])
		wg.Add(1)
		go func(cli *mockClient) {
			defer wg.Done()
			for pos := 0; pos < len(cli.infos); pos++ {
				err := cli.createTable(pos)
				assert.Nil(t, err)
			}

		}(clients[i])
	}

	wg.Wait()
	for _, shard := range shards {
		testutils.WaitExpect(20, func() bool {
			return shard.idAlloc.Get() == shard.getSafeId()
		})
		assert.Equal(t, shard.idAlloc.Get(), shard.getSafeId())
		// t.Logf("shard-%d safeid %d, logid-%d", shard.id, shard.getSafeId(), shard.idAlloc.Get())
		shard.Stop()
	}
	inst.Close()
}
