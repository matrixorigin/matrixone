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

package objectio

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

type membershipCacheProbe struct {
	fscache.Data
	snapshots atomic.Int32
	releases  atomic.Int32
}

func (p *membershipCacheProbe) validatedVectorSnapshot() []byte {
	p.snapshots.Add(1)
	return p.Data.(validatedVectorCacheDataMarker).validatedVectorSnapshot()
}

func (p *membershipCacheProbe) validatedVectorBackingForScope() []byte {
	return p.Data.(validatedVectorCacheDataMarker).validatedVectorBackingForScope()
}

func (p *membershipCacheProbe) Release() {
	p.releases.Add(1)
	p.Data.Release()
}

type membershipProbeFS struct {
	fileservice.FileService
	mu     sync.Mutex
	probes []*membershipCacheProbe
}

func (f *membershipProbeFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	if err := f.FileService.Read(ctx, v); err != nil {
		return err
	}
	for i := range v.Entries {
		entry := &v.Entries[i]
		if _, ok := entry.CachedData.(validatedVectorCacheDataMarker); ok {
			probe := &membershipCacheProbe{Data: entry.CachedData}
			entry.CachedData = probe
			f.mu.Lock()
			f.probes = append(f.probes, probe)
			f.mu.Unlock()
		}
	}
	return nil
}

func (f *membershipProbeFS) assertReleased(t *testing.T, snapshots int32) {
	t.Helper()
	require.NotEmpty(t, f.probes)
	var got int32
	for _, probe := range f.probes {
		require.Equal(t, int32(1), probe.releases.Load())
		got += probe.snapshots.Load()
	}
	require.Equal(t, snapshots, got, "scoped membership must not clone cache-backed filter columns")
	f.probes = nil
}

func newMembershipTestFilter(t testing.TB, mp *mpool.MPool, kind string, ids []int64) docfilter.MembershipFilter {
	t.Helper()
	vec := vector.NewVec(types.T_int64.ToType())
	defer vec.Free(mp)
	require.NoError(t, vector.AppendFixedList(vec, ids, nil, mp))
	var member docfilter.MembershipFilter
	var err error
	switch kind {
	case "bitmap":
		var data []byte
		var ok bool
		data, ok, err = docfilter.BuildCbitmapBytes(vec)
		require.NoError(t, err)
		require.True(t, ok)
		member, err = docfilter.NewCbitmapFilter(data)
	case "sorted":
		var data []byte
		data, err = docfilter.BuildSorted64Bytes(vec)
		require.NoError(t, err)
		member, err = docfilter.NewSorted64Filter(data)
	case "roaring":
		var data []byte
		data, err = docfilter.BuildCRoaringBytes(vec)
		require.NoError(t, err)
		member, err = docfilter.NewCRoaringFilter(data)
	}
	require.NoError(t, err)
	t.Cleanup(member.Free)
	return member
}

func persistMembershipTest(t testing.TB, fs fileservice.FileService, mp *mpool.MPool) Location {
	t.Helper()
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_array_float32.ToType())
	defer bat.Clean(mp)
	keys := []string{"a0", "a1", "b0", "b1"}
	for row, value := range []float32{4, 1, 3, 2} {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(keys[row]), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(row+1), row == 2, mp))
		require.NoError(t, vector.AppendArray(bat.Vecs[2], []float32{value, 0}, false, mp))
	}
	bat.SetRowCount(4)
	id := NewObjectid()
	name := BuildObjectNameWithObjectID(&id)
	writer, err := NewObjectWriter(name, fs, 0, []uint16{0, 1, 2}, nil)
	require.NoError(t, err)
	_, err = writer.Write(bat)
	require.NoError(t, err)
	blocks, err := writer.WriteEnd(t.Context())
	require.NoError(t, err)
	return BuildLocation(name, blocks[0].GetExtent(), 4, 0)
}

func TestReadBlockByMembershipAndTopN(t *testing.T) {
	mp := newTopNTestMP(t)
	storage := newBlockTopNTestFS(t)
	location := persistMembershipTest(t, storage, mp)
	fs := &membershipProbeFS{FileService: storage}
	for _, kind := range []string{"bitmap", "sorted", "roaring"} {
		t.Run(kind, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				pk     *ReadFilterSearch
				ids    []int64
				rows   []int64
				dists  []float64
				input  []int64
				err    error
				cancel bool
			}{
				{name: "dense excludes null", ids: []int64{1, 2, 3, 4}, rows: []int64{1, 3}, dists: []float64{1, 4}, input: []int64{0, 1, 3}},
				{name: "prefix intersection", pk: NewReadFilterPrefixSearch(types.T_varchar, [][]byte{[]byte("a")}), ids: []int64{1, 2, 3, 4}, rows: []int64{0, 1}, dists: []float64{16, 1}, input: []int64{0, 1}},
				// PK hits {0,1}; membership hits {1,3}. Neither predicate alone
				// can produce the expected intersection {1}.
				{name: "partial intersection", pk: NewReadFilterPrefixSearch(types.T_varchar, [][]byte{[]byte("a")}), ids: []int64{2, 3, 4}, rows: []int64{1}, dists: []float64{1}, input: []int64{1}},
				// Both predicates match rows, but their intersection is empty.
				{name: "disjoint intersection", pk: NewReadFilterPrefixSearch(types.T_varchar, [][]byte{[]byte("a")}), ids: []int64{3, 4}, rows: []int64{}, dists: []float64{}, input: []int64{}},
				{name: "sparse", pk: NewReadFilterSearch(types.T_varchar, [][]byte{[]byte("b1")}), ids: []int64{1, 2, 3, 4}, rows: []int64{3}, dists: []float64{4}, input: []int64{3}},
				{name: "empty pk", pk: NewReadFilterSearch(types.T_varchar, [][]byte{[]byte("missing")}), ids: []int64{1, 2, 3, 4}, rows: []int64{}, dists: []float64{}, input: []int64{}},
				// Out-of-fixture IDs keep each filter at two or more entries,
				// as required by the bitmap builder, without adding result hits.
				{name: "membership only no hit", ids: []int64{8, 9}, rows: []int64{}, dists: []float64{}, input: []int64{}},
				{name: "membership only single hit", ids: []int64{4, 5}, rows: []int64{3}, dists: []float64{4}, input: []int64{3}},
				{name: "membership only null", ids: []int64{3, 5}, rows: []int64{}, dists: []float64{}, input: []int64{}},
				{name: "finalize error", ids: []int64{1, 2, 3, 4}, input: []int64{0, 1, 3}, err: errors.New("tombstone failure")},
				{name: "cancel after filter", ids: []int64{1, 2, 3, 4}, input: []int64{0, 1, 3}, cancel: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					member := newMembershipTestFilter(t, mp, kind, tc.ids)
					for _, sorted := range []bool{false, true} {
						t.Run(fmt.Sprintf("sorted=%t", sorted), func(t *testing.T) {
							ctx, cancel := context.WithCancel(t.Context())
							defer cancel()
							out := vector.NewVec(types.T_varchar.ToType())
							defer out.Free(mp)
							rows, distances, _, err := ReadBlockByMembershipAndTopN(
								ctx, []uint16{0, 1}, []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
								[]uint16{0}, []types.Type{types.T_varchar.ToType()}, []*vector.Vector{out},
								2, types.T_array_float32.ToType(), NewReadFilterMembership(tc.pk, member), sorted,
								func(rows []int64, input int) ([]int64, error) {
									require.Equal(t, 4, input)
									require.Equal(t, tc.input, rows)
									if tc.cancel {
										cancel()
										return nil, ctx.Err()
									}
									return rows, tc.err
								}, newTopNTestOp(2), fs, location, mp, fileservice.SkipFullFilePreloads,
							)
							if tc.err != nil || tc.cancel {
								if tc.cancel {
									require.ErrorIs(t, err, context.Canceled)
								} else {
									require.ErrorIs(t, err, tc.err)
								}
								require.Empty(t, rows)
								require.Zero(t, out.Length())
							} else {
								require.NoError(t, err)
								require.Equal(t, tc.rows, rows)
								require.Equal(t, tc.dists, distances)
								require.Equal(t, len(rows), out.Length())
								for i, row := range rows {
									require.Equal(t, []string{"a0", "a1", "b0", "b1"}[row], out.GetStringAt(i))
								}
							}
							fs.assertReleased(t, 0)
							require.True(t, member.Valid(), "the reader, not the block read, owns the membership filter")
						})
					}
				})
			}
		})
	}
}

func TestReadBlockBySearchAndTopNCallbackCannotMutateCache(t *testing.T) {
	mp := newTopNTestMP(t)
	storage := newBlockTopNTestFS(t)
	location := persistMembershipTest(t, storage, mp)
	fs := &membershipProbeFS{FileService: storage}
	for attempt := range 2 {
		out := vector.NewVec(types.T_varchar.ToType())
		func() {
			defer out.Free(mp)
			_, _, _, err := ReadBlockBySearchAndTopN(
				t.Context(), []uint16{0}, []types.Type{types.T_varchar.ToType()},
				[]uint16{0}, []types.Type{types.T_varchar.ToType()}, []*vector.Vector{out},
				2, types.T_array_float32.ToType(),
				func(vectors []vector.Vector) ([]int64, error) {
					require.Equal(t, "a0", vectors[0].GetStringAt(0))
					if attempt == 0 {
						vectors[0].GetBytesAt(0)[0] = 'X'
					}
					return []int64{0}, nil
				}, newTopNTestOp(1), fs, location, mp, fileservice.SkipFullFilePreloads,
			)
			require.NoError(t, err)
			require.Equal(t, "a0", out.GetStringAt(0), "output must come from unchanged sealed cache bytes")
		}()
		fs.assertReleased(t, 1)
	}
}

func TestReadFilterMembershipConcurrent(t *testing.T) {
	mp := newTopNTestMP(t)
	storage := newBlockTopNTestFS(t)
	location := persistMembershipTest(t, storage, mp)
	member := newMembershipTestFilter(t, mp, "bitmap", []int64{2, 4})
	search := NewReadFilterMembership(NewReadFilterPrefixSearch(types.T_varchar, [][]byte{[]byte("a")}), member)
	fs := &membershipProbeFS{FileService: storage}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			out := vector.NewVec(types.T_varchar.ToType())
			defer out.Free(mp)
			rows, distances, _, err := ReadBlockByMembershipAndTopN(
				t.Context(), []uint16{0, 1}, []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
				[]uint16{0}, []types.Type{types.T_varchar.ToType()}, []*vector.Vector{out},
				2, types.T_array_float32.ToType(), search, false,
				func(rows []int64, _ int) ([]int64, error) { return rows, nil },
				newTopNTestOp(2), fs, location, mp, fileservice.SkipFullFilePreloads,
			)
			if err != nil || out.Length() != 1 || out.GetStringAt(0) != "a1" || len(rows) != 1 || rows[0] != 1 || len(distances) != 1 || distances[0] != 1 {
				t.Errorf("concurrent membership: rows=%v distances=%v err=%v", rows, distances, err)
			}
		})
	}
	wg.Wait()
	fs.assertReleased(t, 0)
}

func TestReadFilterMembershipRejectsForeignImplementation(t *testing.T) {
	mp := newTopNTestMP(t)
	member := newMembershipTestFilter(t, mp, "bitmap", []int64{1, 2, 3, 4})
	type foreignFilter struct{ docfilter.MembershipFilter }
	require.Nil(t, NewReadFilterMembership(nil, &foreignFilter{member}))
	require.Nil(t, NewReadFilterMembership(nil, (*docfilter.CbitmapFilter)(nil)))
	_, _, _, err := ReadBlockByMembershipAndTopN(t.Context(), nil, nil, nil, nil, nil,
		0, types.Type{}, &ReadFilterMembership{}, false,
		func(rows []int64, _ int) ([]int64, error) { return rows, nil }, nil, nil, nil, mp, 0)
	require.ErrorContains(t, err, "nil exact membership")
}

func TestReadFilterMembershipSecondaryLengthMismatch(t *testing.T) {
	mp := newTopNTestMP(t)
	pk := vector.NewVec(types.T_varchar.ToType())
	defer pk.Free(mp)
	require.NoError(t, vector.AppendBytes(pk, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(pk, []byte("b"), false, mp))
	ids := vector.NewVec(types.T_int64.ToType())
	defer ids.Free(mp)
	require.NoError(t, vector.AppendFixed(ids, int64(1), false, mp))
	// Both IDs are outside the fixture; two entries satisfy the bitmap builder.
	member := newMembershipTestFilter(t, mp, "bitmap", []int64{98, 99})
	for _, tc := range []struct {
		name string
		pk   *ReadFilterSearch
		rows []int64
	}{
		{name: "membership only retains all rows", rows: []int64{0, 1}},
		{name: "pk predicate retains only its matches", pk: NewReadFilterSearch(types.T_varchar, [][]byte{[]byte("a")}), rows: []int64{0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			search := NewReadFilterMembership(tc.pk, member)
			// A mismatched secondary column must not be probed as though it
			// described the PK rows, nor silently turn a fail-open into no hits.
			require.Equal(t, tc.rows, search.search([]vector.Vector{*pk, *ids}, false))
		})
	}
}

func BenchmarkReadFilterMembershipBinding(b *testing.B) {
	// Both arms use the same membership search. This measures binding cost only,
	// not the end-to-end difference between legacy and scoped block consumers.
	mp := newTopNTestMP(b)
	pk := vector.NewVec(types.T_varchar.ToType())
	ids := vector.NewVec(types.T_int64.ToType())
	b.Cleanup(func() { pk.Free(mp); ids.Free(mp) })
	const rowCount = 8192
	idValues := make([]int64, rowCount)
	for row := range rowCount {
		idValues[row] = int64(row)
		require.NoError(b, vector.AppendBytes(pk, []byte(fmt.Sprintf("compound-prefix-%08d", row)), false, mp))
	}
	require.NoError(b, vector.AppendFixedList(ids, idValues, nil, mp))
	member := newMembershipTestFilter(b, mp, "bitmap", idValues)
	search := NewReadFilterMembership(NewReadFilterPrefixSearch(types.T_varchar, [][]byte{[]byte("compound-prefix-000000")}), member)
	data := make([]fscache.Data, 2)
	for i, vec := range []*vector.Vector{pk, ids} {
		payload, err := vec.MarshalBinary()
		require.NoError(b, err)
		encoded := append(EncodeIOEntryHeader(&IOEntryHeader{Type: IOET_ColData, Version: IOET_ColumnData_V2}), payload...)
		data[i], err = validateVectorCacheData(fileservice.NewBytes(encoded))
		require.NoError(b, err)
		b.Cleanup(data[i].Release)
	}
	for _, scoped := range []bool{false, true} {
		name := "snapshot"
		if scoped {
			name = "scoped"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var vectors [2]vector.Vector
				for i := range vectors {
					var err error
					if scoped {
						err = bindCachedVectorForScope(&vectors[i], data[i])
					} else {
						err = MustVectorToCached(&vectors[i], data[i])
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				rows := search.search(vectors[:], true)
				if len(rows) != 100 || rows[0] != 0 || rows[99] != 99 {
					b.Fatalf("unexpected membership rows: %v", rows)
				}
				for i := range vectors {
					vectors[i].Free(nil)
				}
			}
		})
	}
}
