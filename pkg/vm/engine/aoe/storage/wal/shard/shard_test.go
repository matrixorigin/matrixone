package shard

import (
	"math/rand"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/wal"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

var (
	logSizes = []int{2, 4, 6, 8, 10}
	logCaps  = []int{1, 2, 3, 4, 5}
	mockId   = uint64(0)
)

func nextMockId() uint64 {
	return atomic.AddUint64(&mockId, uint64(1)) - 1
}

type mockLogBatch struct {
	indice []*LogIndex
}

type mockProducer struct {
	shardId uint64
	id      uint64
}

func (mp *mockProducer) nextLogBatch() *mockLogBatch {
	id := mp.id
	mp.id++
	logSize := logSizes[int(id)%len(logSizes)]
	logCap := logCaps[int(id)%len(logCaps)]
	b := &mockLogBatch{
		indice: make([]*LogIndex, logSize),
	}
	for i := 0; i < logSize; i++ {
		idx := &LogIndex{
			Id: IndexId{
				Id:     mp.id,
				Offset: uint32(i),
				Size:   uint32(logSize),
			},
			Capacity: uint64(logCap),
		}
		b.indice[i] = idx
	}
	return b
}

type mockConsumer struct {
	shardId uint64
	snippet *Snippet
}

func newMockConsumer(shardId uint64) *mockConsumer {
	c := &mockConsumer{
		snippet: NewSnippet(shardId, nextMockId(), uint32(0)),
		shardId: shardId,
	}
	return c
}

func (mc *mockConsumer) consume(index *LogIndex) {
	index.Count = index.Capacity
	mc.snippet.Append(index)
}

func (mc *mockConsumer) reset() {
	mc.snippet = NewSnippet(mc.shardId, nextMockId(), uint32(0))
}

func TestSequence(t *testing.T) {
	producer := mockProducer{}
	cnt := 3
	consumers := make([]*mockConsumer, cnt)
	for i, _ := range consumers {
		consumers[i] = newMockConsumer(uint64(1))
	}
	ii := 0
	for i := 0; i < 100; i++ {
		logBat := producer.nextLogBatch()
		for _, index := range logBat.indice {
			ic := ii % cnt
			consumers[ic].consume(index)
			ii++
		}
	}

	groups := make([]*snippets, cnt)

	for i, c := range consumers {
		r := c.snippet.CompletedRange(nil, nil)
		assert.Equal(t, uint64(0), r.Left)
		assert.Equal(t, uint64(200)-1, r.Right)
		t.Logf("seq %d range start %d, end %d", c.snippet.id, r.Left, r.Right)
		groups[i] = newSnippets(c.snippet.id)
		groups[i].Append(c.snippet)
	}

	for i, _ := range consumers {
		consumers[i] = newMockConsumer(uint64(1))
	}
	for i := 0; i < 100; i++ {
		logBat := producer.nextLogBatch()
		for _, index := range logBat.indice {
			ic := ii % cnt
			consumers[ic].consume(index)
			ii++
		}
	}

	for i, c := range consumers {
		groups[i].Append(c.snippet)
	}
	for _, group := range groups {
		total := 0
		fn := func(*IndexId) {
			total++
		}
		group.ForEach(fn)
		assert.Equal(t, 400, total)
	}
}

func TestShardProxy(t *testing.T) {
	shardProxy := newProxy(nextMockId(), nil)
	producer := mockProducer{}
	cnt := 5
	consumers := make([]*mockConsumer, cnt)
	for i, _ := range consumers {
		consumers[i] = newMockConsumer(uint64(1))
	}
	ii := 0
	for i := 0; i < 10; i++ {
		logBat := producer.nextLogBatch()
		for _, index := range logBat.indice {
			ic := ii % cnt
			consumers[ic].consume(index)
			ii++
			idx := &(*index)
			shardProxy.LogIndex(idx)
		}
	}
	for _, c := range consumers {
		shardProxy.AppendSnippet(c.snippet)
		c.reset()
		shardProxy.Checkpoint()
	}

	for i := 0; i < 10000; i++ {
		logBat := producer.nextLogBatch()
		for _, index := range logBat.indice {
			ic := ii % cnt
			consumers[ic].consume(index)
			ii++
			idx := &(*index)
			shardProxy.LogIndex(idx)
		}
	}
	for _, c := range consumers {
		shardProxy.AppendSnippet(c.snippet)
		shardProxy.Checkpoint()
	}
	now := time.Now()
	shardProxy.Checkpoint()
	t.Logf("safe id: %d, %s", shardProxy.GetSafeId(), time.Since(now))
}

func TestShardManager(t *testing.T) {
	mgr := NewManager()
	var wg sync.WaitGroup
	pool, _ := ants.NewPool(8)

	ff := func(shardId uint64) func() {
		return func() {
			defer wg.Done()
			producer := mockProducer{shardId: shardId}
			consumer := newMockConsumer(shardId)
			for k := 0; k < 2; k++ {
				for i := 0; i < 2000; i++ {
					bat := producer.nextLogBatch()
					for _, index := range bat.indice {
						entry := wal.GetEntry(common.NextGlobalSeqNum())
						idx := *index
						idx.ShardId = producer.shardId
						entry.Payload = &idx
						err := mgr.EnqueueEntry(entry)
						assert.Nil(t, err)
						entry.WaitDone()
						entry.Free()
						consumer.consume(index)
					}
				}
				mgr.EnqueueSnippet(consumer.snippet)
				consumer.reset()
			}
		}
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		pool.Submit(ff(uint64(i)))
	}
	wg.Wait()
	mgr.Close()
}

func TestProxy2(t *testing.T) {
	mgr := NewManager()
	defer mgr.Close()
	var indice []*LogIndex
	for i := 1; i < 20; i += 4 {
		index := &LogIndex{
			Id: IndexId{
				Id:   uint64(i),
				Size: uint32(1),
			},
		}
		indice = append(indice, index)
		entry, err := mgr.Log(index)
		entry.WaitDone()
		entry.Free()
		assert.Nil(t, err)
	}
	s0, err := mgr.GetShard(0)
	assert.Nil(t, err)
	assert.Equal(t, uint64(5), s0.mask.GetCardinality())
	assert.Equal(t, uint64(12), s0.stopmask.GetCardinality())
	t.Log(s0.mask.String())
	t.Log(s0.stopmask.String())
	mgr.Checkpoint(indice[0])
	mgr.Checkpoint(indice[1])
	time.Sleep(time.Duration(1) * time.Millisecond)
	t.Log(s0.mask.String())
	t.Log(s0.stopmask.String())
	assert.Equal(t, uint64(5), s0.GetSafeId())
	mgr.Checkpoint(indice[2])
	time.Sleep(time.Duration(1) * time.Millisecond)
	assert.Equal(t, uint64(9), s0.GetSafeId())
	mgr.Checkpoint(indice[3])
	time.Sleep(time.Duration(1) * time.Millisecond)
	assert.Equal(t, uint64(13), s0.GetSafeId())
	mgr.Checkpoint(indice[4])
	time.Sleep(time.Duration(1) * time.Millisecond)
	assert.Equal(t, uint64(17), s0.GetSafeId())
}

func TestProxy3(t *testing.T) {
	mgr := NewManager()
	defer mgr.Close()
	var indice []*LogIndex
	rand.Seed(time.Now().UnixNano())
	produce := func() {
		cnt := 10000
		j := 0
		// for i := 1; j < cnt; i += 1 {
		for i := 1; j < cnt; i += rand.Intn(100) + 1 {
			index := &LogIndex{
				Id: IndexId{
					Id:   uint64(i),
					Size: uint32(1),
				},
			}
			indice = append(indice, index)
			entry, err := mgr.Log(index)
			entry.WaitDone()
			entry.Free()
			assert.Nil(t, err)
			j++
		}
		t.Log(indice[cnt-1].String())
		// t.Log(mgr.String())
	}

	consume := func() {
		for i := len(indice) - 1; i >= 0; i-- {
			mgr.Checkpoint(indice[i])
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
		s, err := mgr.GetShard(uint64(0))
		assert.Nil(t, err)
		assert.Equal(t, indice[len(indice)-1].Id.Id, s.GetSafeId())
	}

	produce()
	consume()
}
