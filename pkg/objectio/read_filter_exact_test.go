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
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func TestReadFilterExactUnsortedMembership(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	defer func() { require.Zero(t, mp.CurrNB()) }()
	values := [][]byte{
		[]byte("z"), nil, []byte("key-00007"), {0, 255, 0},
		bytes.Repeat([]byte("long"), 12), []byte("key-00000"),
		[]byte("missing"), []byte("key-00007"), {},
	}
	for _, oid := range []types.T{types.T_varchar, types.T_varbinary} {
		for _, count := range []int{0, 1, 2, 8, 9, 64} {
			t.Run(fmt.Sprintf("%s/keys=%d", oid, count), func(t *testing.T) {
				vec := vector.NewVec(oid.ToType())
				defer vec.Free(mp)
				needles := make([][]byte, count)
				for i := range needles {
					needles[i] = []byte(fmt.Sprintf("key-%05d", count-1-i))
				}
				if count > 8 {
					// Unordered, duplicate, empty, binary and out-of-line keys.
					needles[0], needles[1], needles[2] = nil, []byte{}, bytes.Clone(values[3])
					needles[3], needles[4] = bytes.Clone(values[4]), []byte("key-00007")
				}
				sourceValues := append([][]byte(nil), values...)
				// Exercise every needle, on both sides of the short linear prefix,
				// while retaining misses and duplicate source rows.
				sourceValues = append(sourceValues, needles...)
				for _, value := range sourceValues {
					require.NoError(t, vector.AppendBytes(vec, value, false, mp))
				}
				var want []int64
				for row, value := range sourceValues {
					for _, needle := range needles {
						if bytes.Equal(value, needle) {
							want = append(want, int64(row))
							break
						}
					}
				}
				search := NewReadFilterSearch(oid, needles)
				for _, needle := range needles {
					clear(needle) // The descriptor must own its search values.
				}
				require.Equal(t, want, search.search(vec, false))
				require.Equal(t, want, search.search(vec, false), "reuse must not consume or reorder search state")
			})
		}
	}
}

func TestReadFilterExactLengthOrdering(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	defer func() { require.Zero(t, mp.CurrNB()) }()
	for _, oid := range []types.T{types.T_varchar, types.T_varbinary} {
		for _, shape := range []string{"uniform", "increasing", "decreasing", "interleaved", "sparse", "empty"} {
			t.Run(fmt.Sprintf("%s/%s", oid, shape), func(t *testing.T) {
				count := 16
				if shape == "uniform" {
					count = 32
				} else if shape == "interleaved" || shape == "sparse" {
					count = 64
				}
				prefix := bytes.Repeat([]byte{'a'}, 32)
				needles := make([][]byte, count)
				members := make(map[string]struct{}, count)
				vec := vector.NewVec(oid.ToType())
				defer vec.Free(mp)
				for i := range needles {
					suffixLen := 0
					if shape == "increasing" {
						suffixLen = 2 * i
					} else if shape == "decreasing" {
						suffixLen = 2 * (count - i)
					} else if shape == "interleaved" {
						suffixLen = 2 * (i % 3)
					} else if shape == "sparse" && i >= 24 {
						suffixLen = 2 * i
					}
					if shape != "empty" {
						needles[i] = append([]byte(fmt.Sprintf("%s%02d", prefix, i)), bytes.Repeat([]byte{'x'}, suffixLen)...)
					}
					members[string(needles[i])] = struct{}{}
				}
				for i := count - 1; i >= 0; i-- {
					require.NoError(t, vector.AppendBytes(vec, needles[i], false, mp))
				}
				// Exact duplicates plus shorter, longer, in-range absent lengths,
				// same-length nonmembers and arbitrary binary bytes.
				for _, value := range [][]byte{nil, needles[count-1], prefix,
					append(bytes.Clone(prefix), []byte("99x")...),
					append(bytes.Clone(prefix), []byte("99")...),
					append(bytes.Clone(prefix), bytes.Repeat([]byte{'x'}, 13)...),
					append(bytes.Clone(prefix), bytes.Repeat([]byte{'x'}, 21)...),
					append(bytes.Clone(prefix), bytes.Repeat([]byte{'x'}, 20)...),
					bytes.Repeat([]byte{'a'}, 128), {0, 255}} {
					require.NoError(t, vector.AppendBytes(vec, value, false, mp))
				}
				search := NewReadFilterSearch(oid, needles)
				term := &search.terms[0]
				require.Equal(t, shape == "decreasing" || shape == "interleaved" || shape == "uniform" || shape == "sparse",
					term.exactTail != nil,
					"only allocate auxiliary state for reordering or large length groups")
				if shape == "uniform" || shape == "interleaved" || shape == "sparse" {
					require.NotEmpty(t, term.exactTail.members)
				}
				for _, needle := range needles {
					clear(needle)
				}
				combined := CombineReadFilterSearch(search, search)
				for _, sorted := range []bool{false, true} {
					t.Run(fmt.Sprintf("sorted=%t", sorted), func(t *testing.T) {
						if sorted {
							vec.InplaceSort()
						}
						var want []int64
						for row := 0; row < vec.Length(); row++ {
							if _, ok := members[string(vec.GetBytesAt(row))]; ok {
								want = append(want, int64(row))
							}
						}
						require.Equal(t, want, search.search(vec, sorted))
						require.Equal(t, want, combined.search(vec, sorted))
						payload, err := vec.MarshalBinary()
						require.NoError(t, err)
						encoded := append([]byte(nil), EncodeIOEntryHeader(&IOEntryHeader{
							Type: IOET_ColData, Version: IOET_ColumnData_V2,
						})...)
						encoded = append(encoded, payload...)
						data, err := validateVectorCacheData(fileservice.NewBytes(encoded))
						require.NoError(t, err)
						defer data.Release()
						probe := &validatedVectorBytesProbe{
							backing: data.(validatedVectorCacheDataMarker).validatedVectorBackingForScope(),
						}
						got, err := SearchCachedVector(fileservice.IOEntry{CachedData: probe}, combined, sorted)
						require.NoError(t, err)
						require.Equal(t, want, got)
						require.Zero(t, probe.bytesCalls.Load(), "search must not clone the sealed backing")
					})
				}
			})
		}
	}
}

// Include in-range misses and matches so a speedup cannot rely on all source
// values being outside the needle range. The sorted path is an unchanged control.
func BenchmarkReadFilterExactSearch(b *testing.B) {
	for _, count := range []int{1, 8, 9, 64, 1024, 4096} {
		for _, shape := range []string{"miss", "mixed", "first-hit", "prefix-hit", "sorted", "const"} {
			b.Run(fmt.Sprintf("keys=%d/%s", count, shape), func(b *testing.B) {
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				needles := make([][]byte, count)
				for i := range needles {
					needles[i] = []byte(fmt.Sprintf("key-%05d", 2*i))
				}
				search := NewReadFilterSearch(types.T_varchar, needles)
				var vec *vector.Vector
				const rows = 8192
				want := 0
				if shape == "const" {
					var err error
					vec, err = vector.NewConstBytes(types.T_varchar.ToType(), needles[count-1], rows, mp)
					require.NoError(b, err)
					want = rows
				} else {
					vec = vector.NewVec(types.T_varchar.ToType())
					for row := 0; row < rows; row++ {
						value := 2*((row*31)%count) + 1
						if shape == "first-hit" {
							value = 0
							want++
						} else if shape == "prefix-hit" {
							value = 2 * (row % min(count, 8))
							want++
						} else if shape != "miss" && row%4 == 0 {
							value--
							want++
						}
						require.NoError(b, vector.AppendBytes(vec, []byte(fmt.Sprintf("key-%05d", value)), false, mp))
					}
				}
				defer vec.Free(mp)
				if shape == "sorted" {
					vec.InplaceSort()
				}
				run := func(b *testing.B, find func() []int64) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if got := find(); len(got) != want {
							b.Fatalf("matched %d rows, want %d", len(got), want)
						}
					}
				}
				b.Run("current", func(b *testing.B) {
					run(b, func() []int64 { return search.search(vec, shape == "sorted") })
				})
				if shape == "miss" || shape == "mixed" || shape == "first-hit" || shape == "prefix-hit" {
					// Keep the previous algorithm in the same binary/run as a
					// control for machine load and compiler changes.
					linear := vector.VarlenLinearSearchOffsetByValFactory(needles)
					b.Run("linear", func(b *testing.B) {
						run(b, func() []int64 { return linear(vec) })
					})
				}
			})
		}
	}
}

func BenchmarkReadFilterExactKeyLengths(b *testing.B) {
	for _, count := range []int{9, 16, 64} {
		for _, prefixLen := range []int{16, 256, 2048} {
			for _, shape := range []string{"uniform", "gap", "present-length"} {
				b.Run(fmt.Sprintf("keys=%d/prefix=%d/%s", count, prefixLen, shape), func(b *testing.B) {
					mp := mpool.MustNewZero()
					defer mpool.DeleteMPool(mp)
					vec := vector.NewVec(types.T_varchar.ToType())
					defer vec.Free(mp)
					prefix := string(bytes.Repeat([]byte{'a'}, prefixLen))
					needles := make([][]byte, count)
					members := make(map[string]struct{}, count)
					for i := range needles {
						width := 8
						if shape == "present-length" {
							width = 4
						}
						needles[i] = []byte(fmt.Sprintf("%s%0*d", prefix, width, i))
						if shape == "gap" {
							needles[i] = append(needles[i], bytes.Repeat([]byte{'x'}, (i%2)*8)...)
						} else if shape == "present-length" {
							needles[i] = append(needles[i], bytes.Repeat([]byte{'x'}, 2*(i%5))...)
						}
						members[string(needles[i])] = struct{}{}
					}
					const rows = 1024
					var want []int64
					for row := 0; row < rows; row++ {
						width := 4
						if shape == "gap" {
							width = 12
						}
						value := []byte(fmt.Sprintf("%s%0*d", prefix, width, (row*31)%rows))
						if row < count {
							// Actual hits keep the block eligible and cover every
							// early-key and length-ordered-tail candidate.
							value = needles[count-1-row]
						}
						if _, ok := members[string(value)]; ok {
							want = append(want, int64(row))
						}
						require.NoError(b, vector.AppendBytes(vec, value, false, mp))
					}
					search := NewReadFilterSearch(types.T_varchar, needles)
					linear := vector.VarlenLinearSearchOffsetByValFactory(needles)
					run := func(b *testing.B, find func() []int64) {
						require.Equal(b, want, find())
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if got := find(); len(got) != len(want) {
								b.Fatalf("matched %d rows, want %d", len(got), len(want))
							}
						}
					}
					b.Run("current", func(b *testing.B) {
						run(b, func() []int64 { return search.search(vec, false) })
					})
					b.Run("linear", func(b *testing.B) { run(b, func() []int64 { return linear(vec) }) })
				})
			}
		}
	}
}

func BenchmarkReadFilterExactConstruction(b *testing.B) {
	for _, count := range []int{1, 16, 4096} {
		for _, mixed := range []bool{false, true} {
			for _, prefixLen := range []int{0, 2048} {
				b.Run(fmt.Sprintf("keys=%d/mixed=%t/prefix=%d", count, mixed, prefixLen), func(b *testing.B) {
					prefix := string(bytes.Repeat([]byte{'a'}, prefixLen))
					needles := make([][]byte, count)
					for i := range needles {
						width := 8
						if mixed {
							width += 2 * (i % 3)
						}
						needles[i] = []byte(fmt.Sprintf("%skey-%0*d", prefix, width, i))
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						search := NewReadFilterSearch(types.T_varchar, needles)
						if len(search.terms[0].values) != count {
							b.Fatal("lost search keys")
						}
					}
				})
			}
		}
	}
}

func BenchmarkReadFilterExactHotRanks(b *testing.B) {
	for _, count := range []int{9, 64, 4096} {
		ranks := []int{9}
		if count > 9 {
			ranks = append(ranks, 10, 17, count/2, count, 0)
		}
		for _, rank := range ranks {
			b.Run(fmt.Sprintf("keys=%d/rank=%d", count, rank), func(b *testing.B) {
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				prefix := bytes.Repeat([]byte{'a'}, 2048)
				needles := make([][]byte, count)
				for i := range needles {
					needles[i] = append(bytes.Clone(prefix), []byte(fmt.Sprintf("%08d", i))...)
				}
				search := NewReadFilterSearch(types.T_varchar, needles)
				vec := vector.NewVec(types.T_varchar.ToType())
				defer vec.Free(mp)
				want := make([]int64, 1024)
				for i := range want {
					want[i] = int64(i)
					key := rank - 1
					if rank == 0 {
						// Cycle multiple tail keys: no consecutive-value memo can
						// hide the cost of actually searching different hot keys.
						key = 8 + i%min(count-8, 32)
					}
					require.NoError(b, vector.AppendBytes(vec, needles[key], false, mp))
				}
				linear := vector.VarlenLinearSearchOffsetByValFactory(needles)
				run := func(b *testing.B, find func() []int64) {
					require.Equal(b, want, find())
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if got := find(); len(got) != len(want) {
							b.Fatal("incorrect membership")
						}
					}
				}
				b.Run("current", func(b *testing.B) { run(b, func() []int64 { return search.search(vec, false) }) })
				b.Run("linear", func(b *testing.B) { run(b, func() []int64 { return linear(vec) }) })
			})
		}
	}
}

func TestReadFilterExactMembershipDispatch(t *testing.T) {
	for _, size := range []int{types.VarlenaInlineSize, types.VarlenaInlineSize + 1} {
		for _, count := range []int{16, 17} {
			t.Run(fmt.Sprintf("bytes=%d/keys=%d", size, count), func(t *testing.T) {
				mp := mpool.MustNewZero()
				defer mpool.DeleteMPool(mp)
				vec := vector.NewVec(types.T_varbinary.ToType())
				defer vec.Free(mp)
				needles := make([][]byte, count)
				var want []int64
				for i := range needles {
					needles[i] = bytes.Repeat([]byte{'a'}, size)
					needles[i][size-1] = byte(i)
					require.NoError(t, vector.AppendBytes(vec, needles[i], false, mp))
					want = append(want, int64(i))
				}
				require.NoError(t, vector.AppendBytes(vec, bytes.Repeat([]byte{'z'}, size), false, mp))
				require.NoError(t, vector.AppendBytes(vec, needles[0][:size-1], false, mp))
				search := NewReadFilterSearch(types.T_varbinary, needles)
				indexed := search.terms[0].exactTail != nil && search.terms[0].exactTail.members != nil
				require.Equal(t, size > types.VarlenaInlineSize && count > 16, indexed)
				for _, value := range needles {
					clear(value)
				}
				require.Equal(t, want, search.search(vec, false))
			})
		}
	}
}

func TestReadFilterExactConcurrentReuse(t *testing.T) {
	needles := make([][]byte, 32)
	for i := range needles {
		needles[i] = []byte(fmt.Sprintf("owned-key-with-out-of-line-payload-%02d", i))
	}
	search := NewReadFilterSearch(types.T_varchar, needles)
	require.NotEmpty(t, search.terms[0].exactTail.members)
	combined := CombineReadFilterSearch(search, search)
	for _, value := range needles {
		clear(value)
	}
	for worker := 0; worker < 4; worker++ {
		t.Run(fmt.Sprintf("reader=%d", worker), func(t *testing.T) {
			t.Parallel()
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			vec := vector.NewVec(types.T_varchar.ToType())
			defer vec.Free(mp)
			want := make([]int64, 32)
			for i := range want {
				want[i] = int64(i)
				value := []byte(fmt.Sprintf("owned-key-with-out-of-line-payload-%02d", (i+worker)%32))
				require.NoError(t, vector.AppendBytes(vec, value, false, mp))
			}
			require.NoError(t, vector.AppendBytes(vec, []byte("owned-key-with-out-of-line-payload-99"), false, mp))
			for range 4 {
				require.Equal(t, want, combined.search(vec, false))
			}
		})
	}
}
