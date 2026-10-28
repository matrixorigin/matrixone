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

package indexfile

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func md5Hex(b []byte) string {
	s := md5.Sum(b)
	return hex.EncodeToString(s[:])
}

func testRef(data []byte) Ref {
	return Ref{DB: "db", Table: "idx", IndexID: "idx:1:0", Tag: 0, Size: int64(len(data)), Checksum: md5Hex(data)}
}

func chunksOf(data []byte, first int64) map[int64][]byte {
	out := make(map[int64][]byte)
	for i := int64(0); i*ChunkSize < int64(len(data)); i++ {
		end := min((i+1)*ChunkSize, int64(len(data)))
		out[first+i] = data[i*ChunkSize : end]
	}
	return out
}

func randomBytes(n int, seed int64) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func TestAssembler(t *testing.T) {
	data := randomBytes(3*ChunkSize+100, 1)
	ref := testRef(data)
	ref.FirstChunk = 7
	chunks := chunksOf(data, 7)

	newAsm := func() (*assembler, *os.File) {
		f, err := os.CreateTemp(t.TempDir(), "asm")
		require.NoError(t, err)
		t.Cleanup(func() { f.Close() })
		a, err := newAssembler(f, &ref)
		require.NoError(t, err)
		return a, f
	}

	a, f := newAsm()
	for _, id := range []int64{9, 7, 10, 8} {
		require.NoError(t, a.add(id, chunks[id]))
	}
	require.NoError(t, a.add(6, []byte("outside")))
	require.NoError(t, a.add(11, []byte("outside")))
	require.NoError(t, a.finish())
	got, err := io.ReadAll(io.NewSectionReader(f, 0, ref.Size))
	require.NoError(t, err)
	require.Equal(t, data, got)

	a, _ = newAsm()
	require.NoError(t, a.add(7, chunks[7]))
	require.ErrorContains(t, a.add(7, chunks[7]), "duplicate chunk 7")

	a, _ = newAsm()
	require.ErrorContains(t, a.add(8, chunks[8][:10]), "chunk 8 has 10 bytes")
	require.ErrorContains(t, a.add(10, append(chunks[10], 0)), "chunk 10 has 101 bytes")

	a, _ = newAsm()
	require.NoError(t, a.add(7, chunks[7]))
	require.NoError(t, a.add(10, chunks[10]))
	require.ErrorContains(t, a.finish(), "missing chunk 8")

	empty := Ref{DB: "db", Table: "t", IndexID: "e", Size: 0, Checksum: md5Hex(nil)}
	f, err = os.CreateTemp(t.TempDir(), "asm")
	require.NoError(t, err)
	defer f.Close()
	a, err = newAssembler(f, &empty)
	require.NoError(t, err)
	require.NoError(t, a.finish())

	exact := randomBytes(2*ChunkSize, 2)
	exactRef := testRef(exact)
	require.Equal(t, int64(2), exactRef.numChunks())
	f, err = os.CreateTemp(t.TempDir(), "asm")
	require.NoError(t, err)
	defer f.Close()
	a, err = newAssembler(f, &exactRef)
	require.NoError(t, err)
	require.ErrorContains(t, a.add(1, exact[ChunkSize:2*ChunkSize-1]), "chunk 1 has 65535 bytes")
	require.NoError(t, a.add(1, exact[ChunkSize:]))
	require.NoError(t, a.add(0, exact[:ChunkSize]))
	require.NoError(t, a.finish())
	got, err = io.ReadAll(io.NewSectionReader(f, 0, exactRef.Size))
	require.NoError(t, err)
	require.Equal(t, exact, got)
}

func TestManifestAndNames(t *testing.T) {
	b := encodeManifest(12345, 0xdeadbeef)
	size, crc, err := decodeManifest(b)
	require.NoError(t, err)
	require.Equal(t, int64(12345), size)
	require.Equal(t, uint32(0xdeadbeef), crc)
	for _, bad := range [][]byte{nil, b[:len(b)-1], append([]byte("XXXX"), b[4:]...), append(b, 0)} {
		_, _, err := decodeManifest(bad)
		require.Error(t, err)
	}

	ref := Ref{IndexID: "a/b:1", Tag: 1, FirstChunk: 5, Size: 70000, Checksum: "0a1b2c3d"}
	require.Equal(t, "42/"+hex.EncodeToString([]byte("a/b:1"))+"/1/5-70000/0a1b2c3d", entryName(42, &ref))
	ref.Checksum = "../x"
	require.Equal(t, "42/"+hex.EncodeToString([]byte("a/b:1"))+"/1/5-70000/x"+hex.EncodeToString([]byte("../x")), entryName(42, &ref))

	// distinct checksums never share a name, compared case-insensitively as on a
	// case-insensitive file system
	sums := []string{"!", "x21", "X21", "Ab", "aB", "ab", "AB", "x", "xx", "78", "0a1b2c3d", md5Hex([]byte("v"))}
	seen := make(map[string]string)
	for _, sum := range sums {
		name := strings.ToLower(pathSafe(sum))
		other, dup := seen[name]
		require.False(t, dup, "%q and %q share %q", sum, other, name)
		seen[name] = sum
	}
	require.Equal(t, md5Hex([]byte("v")), pathSafe(md5Hex([]byte("v"))), "a hex MD5 keeps its name")

	for _, r := range []Ref{
		{Table: "t", IndexID: "i", Checksum: "c"},
		{DB: "d", Table: "t", IndexID: "i", Size: -1, Checksum: "c"},
		{DB: "d", Table: "t", IndexID: "i", FirstChunk: -1, Checksum: "c"},
		{DB: "d", Table: "t", IndexID: "i"},
	} {
		require.Error(t, r.validate())
	}
}

func TestKeyedLocks(t *testing.T) {
	k := keyedLocks{m: make(map[string]*keyedLock)}
	unlock, err := k.lock(context.Background(), "a")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = k.lock(ctx, "a")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	other, err := k.lock(context.Background(), "b")
	require.NoError(t, err, "different keys do not block each other")
	other()

	var inside, maxInside atomic.Int32
	var wg sync.WaitGroup
	unlock()
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := k.lock(context.Background(), "a")
			if err != nil {
				return
			}
			n := inside.Add(1)
			for {
				m := maxInside.Load()
				if n <= m || maxInside.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			inside.Add(-1)
			u()
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), maxInside.Load())
	require.Empty(t, k.m, "unused entries are removed")
}

// chunkRow is one row of the storage table as the fake reader returns it.
type chunkRow struct {
	indexID string
	chunkID int64
	data    []byte
	tag     int64
}

type fakeReader struct {
	rows  []chunkRow
	batch int
	pos   int
}

func (r *fakeReader) Close() error                         { return nil }
func (r *fakeReader) SetOrderBy([]*plan.OrderBySpec)       {}
func (r *fakeReader) GetOrderBy() []*plan.OrderBySpec      { return nil }
func (r *fakeReader) SetIndexParam(*plan.IndexReaderParam) {}
func (r *fakeReader) SetFilterZM(objectio.ZoneMap)         {}
func (r *fakeReader) Read(_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, bat *batch.Batch) (bool, error) {
	if r.pos >= len(r.rows) {
		return true, nil
	}
	end := min(r.pos+r.batch, len(r.rows))
	for _, row := range r.rows[r.pos:end] {
		if err := vector.AppendBytes(bat.Vecs[0], []byte(row.indexID), false, mp); err != nil {
			return false, err
		}
		if err := vector.AppendFixed(bat.Vecs[1], row.chunkID, false, mp); err != nil {
			return false, err
		}
		if err := vector.AppendBytes(bat.Vecs[2], row.data, false, mp); err != nil {
			return false, err
		}
		if err := vector.AppendFixed(bat.Vecs[3], row.tag, false, mp); err != nil {
			return false, err
		}
	}
	bat.SetRowCount(end - r.pos)
	r.pos = end
	return false, nil
}

var _ engine.Reader = (*fakeReader)(nil)

func storageTableDef() *plan.TableDef {
	cols := []*plan.ColDef{
		{Name: catalog.IndexStorage_TblCol_Index_Id, Typ: plan.Type{Id: int32(types.T_varchar), Width: 128}},
		{Name: catalog.IndexStorage_TblCol_Chunk_Id, Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: catalog.IndexStorage_TblCol_Data, Typ: plan.Type{Id: int32(types.T_blob), Width: 65536}},
		{Name: catalog.IndexStorage_TblCol_Tag, Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: catalog.CPrimaryKeyColName, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
	}
	def := &plan.TableDef{Name: "idx", Cols: cols, Name2ColIndex: make(map[string]int32)}
	for i, c := range cols {
		def.Name2ColIndex[c.Name] = int32(i)
	}
	return def
}

// fakeTable answers the engine calls Open makes and counts the scans.
type fakeTable struct {
	tableID uint64
	rows    func() []chunkRow
	scans   atomic.Int32
	filters chan *plan.Expr
}

func newEnv(t *testing.T, ctrl *gomock.Controller, tbl *fakeTable, cache *fileservice.IndexFileCache) *Env {
	proc := testutil.NewProcess(t)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	txn := mock_frontend.NewMockTxnOperator(ctrl)
	ws := mock_frontend.NewMockWorkspace(ctrl)

	eng.EXPECT().Database(gomock.Any(), "db", txn).Return(db, nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), "idx", proc).Return(rel, nil).AnyTimes()
	rel.EXPECT().GetTableID(gomock.Any()).Return(tbl.tableID).AnyTimes()
	rel.EXPECT().GetTableDef(gomock.Any()).Return(storageTableDef()).AnyTimes()
	txn.EXPECT().GetWorkspace().Return(ws).AnyTimes()
	txn.EXPECT().Txn().Return(pbtxn.TxnMeta{ID: []byte("txn-1")}).AnyTimes()
	ws.EXPECT().GetSnapshotWriteOffset().Return(0).AnyTimes()
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, p engine.RangesParam) (engine.RelData, error) {
			tbl.scans.Add(1)
			if tbl.filters != nil {
				select {
				case tbl.filters <- p.BlockFilters[0]:
				default:
				}
			}
			return nil, nil
		}).AnyTimes()
	rel.EXPECT().BuildReaders(gomock.Any(), proc, gomock.Any(), gomock.Any(), 1, 0, false, gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, any, *plan.Expr, engine.RelData, int, int, bool, engine.TombstoneApplyPolicy, engine.FilterHint) ([]engine.Reader, error) {
			return []engine.Reader{&fakeReader{rows: tbl.rows(), batch: 3}}, nil
		}).AnyTimes()
	return &Env{Engine: eng, Proc: proc, Txn: txn, Cache: cache}
}

func fileRows(ref Ref, data []byte, shuffleSeed int64) []chunkRow {
	chunks := chunksOf(data, ref.FirstChunk)
	rows := make([]chunkRow, 0, len(chunks)+2)
	for id, b := range chunks {
		rows = append(rows, chunkRow{indexID: ref.IndexID, chunkID: id, data: b, tag: ref.Tag})
	}
	// rows of other files and tags in the same key range
	rows = append(rows,
		chunkRow{indexID: ref.IndexID + "x", chunkID: ref.FirstChunk, data: []byte("other id"), tag: ref.Tag},
		chunkRow{indexID: ref.IndexID, chunkID: ref.FirstChunk, data: []byte("other tag"), tag: ref.Tag + 1},
	)
	rand.New(rand.NewSource(shuffleSeed)).Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	return rows
}

func newCache(t *testing.T, dir string) *fileservice.IndexFileCache {
	c, err := fileservice.NewIndexFileCache(context.Background(), dir, fscache.ConstCapacity(1<<30), "", fileservice.IndexFileRebuildPolicyDefault)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

func readFile(t *testing.T, f *File) []byte {
	b, err := io.ReadAll(io.NewSectionReader(f.File(), 0, f.Size()))
	require.NoError(t, err)
	return b
}

func resetVerified() {
	verified.mu.Lock()
	clear(verified.m)
	verified.mu.Unlock()
}

func TestOpenRebuildsThenHitsCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	data := randomBytes(5*ChunkSize+321, 3)
	ref := testRef(data)
	tbl := &fakeTable{tableID: 11, rows: func() []chunkRow { return fileRows(ref, data, 4) }, filters: make(chan *plan.Expr, 1)}
	env := newEnv(t, ctrl, tbl, newCache(t, t.TempDir()))
	ctx := context.Background()

	f, err := Open(ctx, env, ref)
	require.NoError(t, err)
	require.Equal(t, data, readFile(t, f))
	require.NoError(t, f.Close())
	require.Equal(t, int32(1), tbl.scans.Load())

	filter := <-tbl.filters
	fn := filter.GetF()
	require.Equal(t, "prefix_between", fn.Func.ObjName)
	require.Equal(t, string(serialKey(ref.IndexID, 0)), fn.Args[1].GetLit().GetSval())
	require.Equal(t, string(serialKey(ref.IndexID, 5)), fn.Args[2].GetLit().GetSval())

	f, err = Open(ctx, env, ref)
	require.NoError(t, err)
	require.Equal(t, data, readFile(t, f))
	require.NoError(t, f.Close())
	require.Equal(t, int32(1), tbl.scans.Load(), "a second open is served from the cache")

	resetVerified()
	f, err = Open(ctx, env, ref)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, int32(1), tbl.scans.Load(), "a full check of a good entry still hits")
}

// Two versions of a file that differ only in a custom checksum are distinct cache entries:
// opening the second rebuilds it instead of serving the first version's bytes.
func TestOpenDistinguishesCustomChecksums(t *testing.T) {
	ctrl := gomock.NewController(t)
	first, second := randomBytes(ChunkSize+9, 21), randomBytes(ChunkSize+9, 22)
	want := map[string][]byte{"!": first, "x21": second}
	ref := Ref{DB: "db", Table: "idx", IndexID: "idx:1:0", Size: int64(len(first)), Checksum: "!"}
	ref.Verify = func(f *os.File, size int64, checksum string) error {
		got, err := io.ReadAll(io.NewSectionReader(f, 0, size))
		if err != nil {
			return err
		}
		if string(got) != string(want[checksum]) {
			return moerr.NewInternalErrorNoCtxf("content is not version %q", checksum)
		}
		return nil
	}
	current := first
	tbl := &fakeTable{tableID: 23, rows: func() []chunkRow { return fileRows(ref, current, 24) }}
	env := newEnv(t, ctrl, tbl, newCache(t, t.TempDir()))
	ctx := context.Background()

	f, err := Open(ctx, env, ref)
	require.NoError(t, err)
	require.Equal(t, first, readFile(t, f))
	require.NoError(t, f.Close())

	current = second
	ref.Checksum = "x21"
	f, err = Open(ctx, env, ref)
	require.NoError(t, err)
	require.Equal(t, second, readFile(t, f))
	require.NoError(t, f.Close())
	require.Equal(t, int32(2), tbl.scans.Load(), "the second version is rebuilt, not read from the first's entry")
}

func TestOpenTailFrameRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	frame := randomBytes(2*ChunkSize+5, 5)
	ref := Ref{DB: "db", Table: "idx", IndexID: "cdc_tail", Tag: 1, FirstChunk: 40, Size: int64(len(frame)), Checksum: "frame"}
	var verifyCalls int
	ref.Verify = func(f *os.File, size int64, checksum string) error {
		verifyCalls++
		require.Equal(t, "frame", checksum)
		require.Equal(t, int64(len(frame)), size)
		return nil
	}
	rows := func() []chunkRow {
		r := fileRows(ref, frame, 6)
		// neighbouring frames of the same tail
		r = append(r, chunkRow{indexID: "cdc_tail", chunkID: 39, data: []byte("prev"), tag: 1},
			chunkRow{indexID: "cdc_tail", chunkID: 43, data: []byte("next"), tag: 1})
		return r
	}
	env := newEnv(t, ctrl, &fakeTable{tableID: 12, rows: rows}, newCache(t, t.TempDir()))
	f, err := Open(context.Background(), env, ref)
	require.NoError(t, err)
	defer f.Close()
	require.Equal(t, frame, readFile(t, f))
	require.Equal(t, 1, verifyCalls)
}

func TestOpenRejectsBadRows(t *testing.T) {
	data := randomBytes(3*ChunkSize, 7)
	ref := testRef(data)
	cases := map[string]func() []chunkRow{
		"missing chunk": func() []chunkRow {
			r := fileRows(ref, data, 8)
			out := r[:0]
			for _, row := range r {
				if !(row.indexID == ref.IndexID && row.tag == ref.Tag && row.chunkID == 1) {
					out = append(out, row)
				}
			}
			return out
		},
		"duplicate chunk": func() []chunkRow {
			return append(fileRows(ref, data, 9), chunkRow{indexID: ref.IndexID, chunkID: 2, data: data[2*ChunkSize:], tag: ref.Tag})
		},
		"corrupted chunk": func() []chunkRow {
			r := fileRows(ref, data, 10)
			for i := range r {
				if r[i].indexID == ref.IndexID && r[i].tag == ref.Tag && r[i].chunkID == 0 {
					b := append([]byte(nil), r[i].data...)
					b[100] ^= 1
					r[i].data = b
				}
			}
			return r
		},
	}
	for name, rows := range cases {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			dir := t.TempDir()
			env := newEnv(t, ctrl, &fakeTable{tableID: 13, rows: rows}, newCache(t, dir))
			_, err := Open(context.Background(), env, ref)
			require.Error(t, err)
			_, err = env.Cache.Open(context.Background(), entryName(13, &ref), dataFile)
			require.Error(t, err, "nothing is cached")
		})
	}
}

func TestOpenRebuildsCorruptedCacheEntry(t *testing.T) {
	ctrl := gomock.NewController(t)
	data := randomBytes(2*ChunkSize+7, 11)
	ref := testRef(data)
	tbl := &fakeTable{tableID: 14, rows: func() []chunkRow { return fileRows(ref, data, 12) }}
	cache := newCache(t, t.TempDir())
	env := newEnv(t, ctrl, tbl, cache)
	ctx := context.Background()

	f, err := Open(ctx, env, ref)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	files, err := cache.Open(ctx, entryName(14, &ref), dataFile)
	require.NoError(t, err)
	path := files[0].Name()
	files[0].Close()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	raw[10] ^= 1
	require.NoError(t, os.WriteFile(path, raw, 0644))

	f, err = Open(ctx, env, ref)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, int32(1), tbl.scans.Load(), "already verified in this process: length check only")

	resetVerified()
	f, err = Open(ctx, env, ref)
	require.NoError(t, err)
	require.Equal(t, data, readFile(t, f), "the first full check after start detects the corruption and rebuilds")
	require.NoError(t, f.Close())
	require.Equal(t, int32(2), tbl.scans.Load())
}

func TestOpenWithoutCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	data := randomBytes(ChunkSize+1, 13)
	ref := testRef(data)
	tbl := &fakeTable{tableID: 15, rows: func() []chunkRow { return fileRows(ref, data, 14) }}
	env := newEnv(t, ctrl, tbl, nil)
	for i := 1; i <= 2; i++ {
		f, err := Open(context.Background(), env, ref)
		require.NoError(t, err)
		require.Equal(t, data, readFile(t, f))
		require.NoError(t, f.Close())
		require.Equal(t, int32(i), tbl.scans.Load(), "without a cache every open rebuilds")
	}
}

func TestOpenConcurrentMissesRebuildOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	data := randomBytes(4*ChunkSize+9, 15)
	ref := testRef(data)
	tbl := &fakeTable{tableID: 16, rows: func() []chunkRow { return fileRows(ref, data, 16) }}
	env := newEnv(t, ctrl, tbl, newCache(t, t.TempDir()))

	var wg sync.WaitGroup
	errs := make([]error, 16)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f, err := Open(context.Background(), env, ref)
			if err == nil {
				if b, rerr := io.ReadAll(io.NewSectionReader(f.File(), 0, f.Size())); rerr != nil || !bytes.Equal(b, data) {
					err = io.ErrUnexpectedEOF
				}
				f.Close()
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), tbl.scans.Load(), "concurrent misses rebuild once")
}

func TestFDPathSurvivesEviction(t *testing.T) {
	ctrl := gomock.NewController(t)
	data := randomBytes(2*ChunkSize+3, 17)
	ref := testRef(data)
	tbl := &fakeTable{tableID: 18, rows: func() []chunkRow { return fileRows(ref, data, 18) }}
	env := newEnv(t, ctrl, tbl, newCache(t, t.TempDir()))

	f, err := Open(context.Background(), env, ref)
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, env.Cache.Delete(context.Background(), entryName(18, &ref)))
	_, err = env.Cache.Open(context.Background(), entryName(18, &ref), dataFile)
	require.Error(t, err, "the cache entry is gone")

	got, err := os.ReadFile(f.FDPath())
	require.NoError(t, err)
	require.Equal(t, data, got, "a library opening FDPath reads the file after eviction")
	// the layer reads by offset, so a shared descriptor offset does not disturb it
	require.Equal(t, data, readFile(t, f))
}

func TestWriteCachesUnderNameAndCleansUp(t *testing.T) {
	ctrl := gomock.NewController(t)
	dir := t.TempDir()
	cache := newCache(t, dir)
	tbl := &fakeTable{tableID: 19, rows: func() []chunkRow { return nil }}
	env := newEnv(t, ctrl, tbl, cache)
	ctx := context.Background()
	data := randomBytes(2*ChunkSize+9, 19)

	var token string
	ref := Ref{DB: "db", Table: "idx", IndexID: "cdc_tail", Tag: 1, FirstChunk: 7, Checksum: "frame0001"}
	size, sum, err := Write(ctx, env, ref, bytes.NewReader(data), func(sql string) error {
		require.Contains(t, sql, "FROM index_file_chunks('")
		token = sql[strings.Index(sql, "('")+2 : strings.Index(sql, "',")]
		src, err := LookupSource(token, []byte("txn-1"))
		require.NoError(t, err)
		buf := make([]byte, ChunkSize)
		last, err := src.ReadChunk(9, buf)
		require.NoError(t, err)
		require.Equal(t, data[2*ChunkSize:], last, "the last chunk is short")
		_, err = src.ReadChunk(10, buf)
		require.Error(t, err)
		_, err = src.ReadChunk(6, buf)
		require.Error(t, err)
		_, err = LookupSource(token, []byte("txn-2"))
		require.Error(t, err, "another transaction cannot use the source")
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), size)
	require.Equal(t, md5Hex(data), sum)
	_, err = LookupSource(token, []byte("txn-1"))
	require.Error(t, err, "the source is unregistered after Write")

	ref.Size = size
	f, err := Open(ctx, env, ref)
	require.NoError(t, err, "the caller's checksum names the cached file")
	require.Equal(t, data, readFile(t, f))
	require.NoError(t, f.Close())
	require.Equal(t, int32(0), tbl.scans.Load(), "the writing CN reads its file from the cache")

	boom := errors.New("insert failed")
	failed := Ref{DB: "db", Table: "idx", IndexID: "idx:2:0"}
	_, _, err = Write(ctx, env, failed, bytes.NewReader(data), func(sql string) error {
		token = sql[strings.Index(sql, "('")+2 : strings.Index(sql, "',")]
		return boom
	})
	require.ErrorIs(t, err, boom)
	_, err = LookupSource(token, []byte("txn-1"))
	require.Error(t, err, "the source is unregistered after a failed Write")
	failed.Size, failed.Checksum = size, sum
	_, err = cache.Open(ctx, entryName(19, &failed), dataFile)
	require.Error(t, err, "a failed write caches nothing")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, strings.HasSuffix(e.Name(), ".tmp"), "temp files are removed: %s", e.Name())
	}

	empty := Ref{DB: "db", Table: "idx", IndexID: "empty"}
	size, sum, err = Write(ctx, env, empty, bytes.NewReader(nil), func(string) error {
		t.Fatal("an empty file writes no rows")
		return nil
	})
	require.NoError(t, err)
	require.Zero(t, size)
	empty.Checksum = sum
	f, err = Open(ctx, env, empty)
	require.NoError(t, err)
	require.Zero(t, f.Size())
	require.NoError(t, f.Close())
}

func TestVerifiedSetIsBounded(t *testing.T) {
	resetVerified()
	defer resetVerified()
	for i := 0; i <= maxVerified; i++ {
		verified.add(strconv.Itoa(i))
	}
	verified.mu.Lock()
	n := len(verified.m)
	verified.mu.Unlock()
	require.Equal(t, 1, n, "the set is cleared when full")
	require.True(t, verified.has(strconv.Itoa(maxVerified)))
	verified.remove(strconv.Itoa(maxVerified))
	require.False(t, verified.has(strconv.Itoa(maxVerified)))
}

// failCacheStaging makes writes to staging files under dir fail with ENOSPC once
// allowed writes have succeeded.
func failCacheStaging(t *testing.T, dir string, allowed int32) {
	var n atomic.Int32
	beforeStagingWrite = func(f *os.File) error {
		if strings.HasPrefix(f.Name(), dir) && n.Add(1) > allowed {
			return syscall.ENOSPC
		}
		return nil
	}
	t.Cleanup(func() { beforeStagingWrite = nil })
}

func requireNoStagingFiles(t *testing.T, dir string) {
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, strings.HasSuffix(e.Name(), ".tmp"), "temp files are removed: %s", e.Name())
	}
}

func TestOpenFallsBackWhenCacheStagingFails(t *testing.T) {
	resetVerified()
	ctrl := gomock.NewController(t)
	dir := t.TempDir()
	cache := newCache(t, dir)
	data := randomBytes(3*ChunkSize+5, 20)
	ref := testRef(data)
	tbl := &fakeTable{tableID: 20, rows: func() []chunkRow { return fileRows(ref, data, 20) }}
	env := newEnv(t, ctrl, tbl, cache)
	failCacheStaging(t, dir, 1)

	f, err := Open(context.Background(), env, ref)
	require.NoError(t, err, "a full cache disk does not fail the read")
	require.Equal(t, data, readFile(t, f))
	require.NoError(t, f.Close())
	_, err = cache.Open(context.Background(), entryName(20, &ref), dataFile)
	require.Error(t, err, "the file is not cached")
	requireNoStagingFiles(t, dir)

	beforeStagingWrite = func(*os.File) error { return syscall.EIO }
	_, err = Open(context.Background(), env, ref)
	require.ErrorIs(t, err, syscall.EIO, "a failing private file is still an error")
}

// onceReader cannot be rewound: it reads src once in small pieces.
type onceReader struct{ src io.Reader }

func (r *onceReader) Read(p []byte) (int, error) {
	if len(p) > 1000 {
		p = p[:1000]
	}
	return r.src.Read(p)
}

func TestWriteFallsBackWhenCacheStagingFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	dir := t.TempDir()
	cache := newCache(t, dir)
	tbl := &fakeTable{tableID: 21, rows: func() []chunkRow { return nil }}
	env := newEnv(t, ctrl, tbl, cache)
	data := randomBytes(2*ChunkSize+9, 21)
	failCacheStaging(t, dir, 5)

	ref := Ref{DB: "db", Table: "idx", IndexID: "idx:3:0"}
	var got []byte
	size, sum, err := Write(context.Background(), env, ref, &onceReader{src: bytes.NewReader(data)}, func(sql string) error {
		token := sql[strings.Index(sql, "('")+2 : strings.Index(sql, "',")]
		src, err := LookupSource(token, []byte("txn-1"))
		require.NoError(t, err)
		for id := int64(0); id < 3; id++ {
			buf := make([]byte, ChunkSize)
			chunk, err := src.ReadChunk(id, buf)
			require.NoError(t, err)
			got = append(got, chunk...)
		}
		return nil
	})
	require.NoError(t, err, "a full cache disk does not fail the write")
	require.Equal(t, int64(len(data)), size)
	require.Equal(t, md5Hex(data), sum)
	require.Equal(t, data, got, "bytes staged before the failure are kept")
	ref.Size, ref.Checksum = size, sum
	_, err = cache.Open(context.Background(), entryName(21, &ref), dataFile)
	require.Error(t, err, "the file is not cached")
	requireNoStagingFiles(t, dir)

	boom := errors.New("source failed")
	_, _, err = Write(context.Background(), env, Ref{DB: "db", Table: "idx", IndexID: "idx:4:0"},
		io.MultiReader(bytes.NewReader(data[:ChunkSize]), iotestErrReader{boom}), func(string) error { return nil })
	require.ErrorIs(t, err, boom, "a source error is not hidden by the fallback")
}

type iotestErrReader struct{ err error }

func (r iotestErrReader) Read([]byte) (int, error) { return 0, r.err }
