// Copyright 2026 Matrix Origin
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

package fulltext2

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestTailPoolTransfersOnlyCurrentState(t *testing.T) {
	p := &tailPool{states: make(map[string]*tailState)}
	first := newTailState(1, 10, 10, []*Segment{NewSegment("tail-10", 0)}, map[any]int64{"old": 10})
	second := newTailState(1, 11, 11, []*Segment{NewSegment("tail-11", 0)}, map[any]int64{"old": 10, "new": 11})
	index := "db.store"
	p.mu.Lock()
	p.states[index] = first
	p.mu.Unlock()

	second.segments = append(append([]*segmentLease(nil), first.segments...), second.segments...)
	views, deletes, ok := p.installDeltaAndAcquire(index, first, second)
	require.True(t, ok)
	require.Len(t, views, 2)
	require.Equal(t, int64(10), deletes["old"])
	require.Equal(t, int64(11), deletes["new"])
	for _, view := range views {
		view.Free()
	}

	p.clear(index)
	require.Nil(t, first.segments[0].template)
	require.Nil(t, second.segments[1].template)
}

func TestTailPoolRejectsStaleDeltaInstall(t *testing.T) {
	p := &tailPool{states: make(map[string]*tailState)}
	index := "db.store"
	first := newTailState(1, 10, 10, []*Segment{NewSegment("tail-10", 0)}, nil)
	other := newTailState(1, 12, 12, []*Segment{NewSegment("tail-12", 0)}, nil)
	state := newTailState(1, 11, 11, []*Segment{NewSegment("tail-11", 0)}, nil)
	state.segments = append(append([]*segmentLease(nil), first.segments...), state.segments...)
	p.mu.Lock()
	p.states[index] = other
	p.mu.Unlock()

	views, _, ok := p.installDeltaAndAcquire(index, first, state)
	require.False(t, ok)
	require.Nil(t, views)
	for _, lease := range state.segments[1:] {
		lease.retire()
	}
	p.clear(index)
}

func TestMergeTailDeletesInitializesInsertOnlyDelta(t *testing.T) {
	merged := mergeTailDeletes(nil, map[any]int64{"new": 11})
	require.Equal(t, map[any]int64{"new": int64(11)}, merged)
}

func TestTailPoolBoundsIndexesAndEvictsIdle(t *testing.T) {
	p := &tailPool{
		states:     make(map[string]*tailState),
		maxEntries: 1,
		maxBytes:   1 << 20,
		idleTTL:    time.Hour,
	}
	for i, index := range []string{"db.one", "db.two"} {
		state := newTailState(1, int64(i), int64(i), []*Segment{NewSegment(index, 0)}, nil)
		views, _ := p.installAndAcquire(index, state)
		for _, view := range views {
			view.Free()
		}
	}
	require.LessOrEqual(t, len(p.states), 1)

	p.idleTTL = time.Nanosecond
	p.evict(time.Now().Add(time.Second))
	require.Empty(t, p.states)
	p.clearAll()
}

func TestTailPoolByteBoundCountsDeserializedArchiveAndDeletePayload(t *testing.T) {
	b := NewBuilder("tail", int32(types.T_int64))
	words := make([]string, 100)
	for i := range words {
		words[i] = "repeated"
	}
	for doc := int64(0); doc < 100; doc++ {
		feed(t, b, doc, words...)
	}
	seg, err := b.Finish()
	require.NoError(t, err)
	archive, err := seg.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("tail", bytes.NewReader(archive))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = loaded.dict.Close()
		loaded.Free()
	})

	require.Equal(t, int64(len(archive)), loaded.ownedBytes)
	deleteKey := strings.Repeat("varchar-pk-", 128)
	state := newTailState(1, 1, 1, []*Segment{loaded}, map[any]int64{deleteKey: 1})
	require.GreaterOrEqual(t, state.bytes, int64(len(archive)))
	require.GreaterOrEqual(t, state.bytes, int64(len(archive)+len(deleteKey)+32))
	state.segments[0].retire()
}

// A replacement on another CN has no local invalidation callback. The durable
// bounds must invalidate retained tail leases even when no base model changes.
func TestTailReuseDistinguishesReplacementFromAppend(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()
	key := cfg.DbName + "." + cfg.IndexTable
	loadedTailPool.clear(key)
	t.Cleanup(func() { loadedTailPool.clear(key) })
	makeChunk := func(id, pk int64, word string) TailChunk {
		b := NewBuilder("tail", int32(types.T_int64))
		feed(t, b, pk, word)
		seg, err := b.Finish()
		require.NoError(t, err)
		data, err := FrameSegment(seg)
		require.NoError(t, err)
		return TailChunk{ChunkId: id, Data: data}
	}
	chunks := make([]TailChunk, 1, 2)
	chunks[0] = makeChunk(1, 1, "apple")
	var dataSQL string
	afterRE := regexp.MustCompile(`chunk_id > ([0-9]+)`)
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "LENGTH(") {
			return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 1)}}, nil
		}
		dataSQL = sql
		after := int64(-1)
		if m := afterRE.FindStringSubmatch(sql); len(m) > 0 {
			after, _ = strconv.ParseInt(m[1], 10, 64)
		}
		var visible []TailChunk
		for _, c := range chunks {
			if c.ChunkId > after {
				visible = append(visible, c)
			}
		}
		return executor.Result{Mp: mp, Batches: []*batch.Batch{tailChunkBatch(mp, visible)}}, nil
	})
	search := func(segs []*Segment, deletes map[any]int64, term string) []any {
		rows, err := NewIndex(segs, deletes).SearchText([]byte(term), tokenizer.NewSimpleTokenizer(), TfIdf, 10, nil)
		require.NoError(t, err)
		return pkslice(rows)
	}
	load := func(first, last int64) ([]*Segment, map[any]int64) {
		segs, d, _, err := loadTailWithReuse(sp, cfg, 0, first, last, nil)
		require.NoError(t, err)
		return segs, d
	}
	old, oldDeletes := load(1, 1)
	defer freeSegs(old)
	require.Equal(t, []any{int64(1)}, search(old, oldDeletes, "apple"))
	chunks = append(chunks, makeChunk(2, 2, "banana"))
	segs, d := load(1, 2)
	require.Contains(t, dataSQL, "chunk_id > 1", "ordinary append reads only its delta")
	require.Equal(t, []any{int64(1)}, search(segs, d, "apple"))
	require.Equal(t, []any{int64(2)}, search(segs, d, "banana"))
	freeSegs(segs)
	chunks = []TailChunk{makeChunk(5, 5, "cherry")}
	segs, d = load(5, 5)
	require.NotRegexp(t, afterRE, dataSQL, "a same-base partial replacement must reload")
	require.Empty(t, search(segs, d, "apple"))
	require.Equal(t, []any{int64(5)}, search(segs, d, "cherry"))
	freeSegs(segs)
	require.Equal(t, []any{int64(1)}, search(old, oldDeletes, "apple"), "old readers retain their own snapshot leases")
	chunks = []TailChunk{{ChunkId: 7, Data: cuvscdc.FrameCdcChunk(nil, nil, 0, 0, 0)}}
	segs, d = load(7, 7)
	require.Empty(t, search(segs, d, "cherry"))
	freeSegs(segs)
	chunks = append(chunks, makeChunk(9, 9, "date"))
	segs, d = load(7, 9)
	require.Contains(t, dataSQL, "chunk_id > 7")
	require.Equal(t, []any{int64(9)}, search(segs, d, "date"))
	freeSegs(segs)
	// Prefix removal is conservative: changed minimum with an unchanged maximum
	// still reloads, rather than retaining data outside the observed snapshot.
	chunks = chunks[1:]
	segs, d = load(9, 9)
	require.NotRegexp(t, afterRE, dataSQL)
	require.Equal(t, []any{int64(9)}, search(segs, d, "date"))
	freeSegs(segs)
	require.Equal(t, int64(9), loadedTailPool.state(key).minChunk, fmt.Sprint(dataSQL))
}
