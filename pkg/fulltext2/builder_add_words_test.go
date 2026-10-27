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

package fulltext2

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// The reference deliberately calls the unchanged single-occurrence API.
func addWordsUsingAdd(b *Builder, words []WordPos, pk any) error {
	for _, w := range words {
		if err := b.Add(w.Word, w.Pos, pk); err != nil {
			return err
		}
	}
	return nil
}

func TestBuilderAddWordsParity(t *testing.T) {
	for _, pf := range []bool{false, true} {
		for _, bytePK := range []bool{false, true} {
			t.Run(fmt.Sprintf("positionFree=%t/bytePK=%t", pf, bytePK), func(t *testing.T) {
				pkType := types.T_int64
				var first, duplicate, second any = int64(7), int64(7), int64(8)
				if bytePK {
					pkType = types.T_varchar
					first, duplicate, second = []byte("row-a"), []byte("row-a"), []byte("row-b")
				}
				opts := []BuildOpt{WithIncludeTypes([]int32{int32(types.T_varchar)})}
				if pf {
					opts = append(opts, WithPositionFree())
				}
				old, batched := NewBuilder("rows", int32(pkType), opts...), NewBuilder("rows", int32(pkType), opts...)
				repeats := make([]WordPos, 257)
				for i := range repeats {
					repeats[i] = WordPos{Word: "repeat", Pos: int32(i * 7)}
				}
				for _, row := range []struct {
					pk    any
					words []WordPos
				}{
					{first, nil},
					{first, []WordPos{}},
					{first, []WordPos{{Word: ""}}},
					{first, []WordPos{{Word: "alpha", Pos: 0}, {Word: "", Pos: 6}, {Word: "ignored", Pos: 7}}},
					{duplicate, []WordPos{{Word: "beta", Pos: 6}, {Word: "alpha", Pos: 11}}},
					{second, repeats},
				} {
					wantErr := addWordsUsingAdd(old, row.words, row.pk)
					err := batched.AddWords(row.words, row.pk)
					if wantErr != nil {
						require.IsType(t, wantErr, err)
						require.EqualError(t, err, wantErr.Error())
					} else {
						require.NoError(t, err)
						if len(row.words) > 0 {
							old.SetInclude(row.pk, []any{[]byte("active")})
							batched.SetInclude(row.pk, []any{[]byte("active")})
						}
					}
					require.Equal(t, old.docs, batched.docs, "includes valid prefix after an error")
					require.Equal(t, old.ordMap, batched.ordMap)
					require.Equal(t, old.NumPostings(), batched.NumPostings())
					require.Equal(t, old.NumDataBytes(), batched.NumDataBytes())
					for _, caps := range [][2]int64{{1, 1000}, {100, 3}, {100, 4}} {
						require.Equal(t, ReachedSegmentCap(old, caps[0], caps[1]), ReachedSegmentCap(batched, caps[0], caps[1]))
					}
				}
				require.Equal(t, 2, batched.NumDocs())
				require.Equal(t, 260, batched.NumPostings())
				if bytePK {
					copy(first.([]byte), "xxxxx")
					require.Equal(t, []byte("row-a"), batched.docs[0].Pk, "retained PK is copied")
				}
				want, err := old.FinishSegments(1)
				require.NoError(t, err)
				got, err := batched.FinishSegments(1)
				require.NoError(t, err)
				require.Len(t, got, 2)
				for i := range got {
					wb, err := want[i].Serialize()
					require.NoError(t, err)
					gb, err := got[i].Serialize()
					require.NoError(t, err)
					require.Equal(t, wb, gb, "same PK, INCLUDE, terms, positions, TF, stats and split")
				}
				require.Equal(t, int32(257), got[1].docLen[0], "document length is not TF-capped")
				pl, ok := got[1].lookup("repeat")
				require.True(t, ok)
				require.Equal(t, []uint8{255}, pl.tfs)
				if pf {
					require.Nil(t, pl.positions)
				} else {
					require.Len(t, pl.positions[0], 257)
					require.Equal(t, int32(256*7), pl.positions[0][256])
				}
			})
		}
	}
}

func TestBuilderAddWordsDoesNotResolveEmptyInput(t *testing.T) {
	b := NewBuilder("empty", int32(types.T_varchar))
	// This PK cannot be hashed. The original empty loop/invalid first Add never
	// reaches docOrd, so the batched API must not reach it either.
	pk := []int{1}
	require.NoError(t, b.AddWords(nil, pk))
	require.ErrorContains(t, b.AddWords([]WordPos{{Word: ""}}, pk), "empty word")
	require.Zero(t, b.NumDocs())
	require.Zero(t, b.NumPostings())
	require.Zero(t, b.NumDataBytes())
}

func TestBuilderAddWordsNilBytePK(t *testing.T) {
	old, batched := NewBuilder("nil-pk", int32(types.T_varchar)), NewBuilder("nil-pk", int32(types.T_varchar))
	for _, pk := range [][]byte{nil, {}} {
		words := []WordPos{{Word: "alpha", Pos: 0}, {Word: "beta", Pos: 6}}
		require.NoError(t, addWordsUsingAdd(old, words, pk))
		require.NoError(t, batched.AddWords(words, pk))
	}
	require.Equal(t, old.docs, batched.docs)
	require.Equal(t, old.ordMap, batched.ordMap)
	require.Equal(t, 1, batched.NumDocs(), "nil and empty byte keys normalize identically")
	require.Equal(t, 4, batched.NumPostings())
	require.Zero(t, batched.NumDataBytes())
}

var builderRowBenchmarkSink *Builder

func BenchmarkBuilderRowAppend(b *testing.B) {
	for _, bytePK := range []bool{false, true} {
		for _, n := range []int{1, 32, 128} {
			words := make([]WordPos, n)
			for i := range words {
				words[i] = WordPos{Word: "repeat", Pos: int32(i * 7)}
			}
			pks := make([]any, 16)
			pkType := types.T_int64
			for i := range pks {
				pks[i] = int64(i)
				if bytePK {
					pkType = types.T_varchar
					pks[i] = []byte(fmt.Sprintf("document-%08d", i))
				}
			}
			for _, batched := range []bool{false, true} {
				b.Run(fmt.Sprintf("bytePK=%t/terms=%d/batched=%t", bytePK, n, batched), func(b *testing.B) {
					b.ReportAllocs()
					b.ReportMetric(float64(len(pks)*n), "tokens/op")
					for i := 0; i < b.N; i++ {
						builder := NewBuilder("bench", int32(pkType))
						for _, pk := range pks {
							var err error
							if batched {
								err = builder.AddWords(words, pk)
							} else {
								err = addWordsUsingAdd(builder, words, pk)
							}
							if err != nil {
								b.Fatal(err)
							}
						}
						builderRowBenchmarkSink = builder
					}
				})
			}
		}
	}
}
