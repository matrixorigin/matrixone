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

package readutil_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

const membershipTopNBenchRows = 8192

// BenchmarkConstructedMembershipBlockTopN covers the real producer and public
// block-reader consumer, including the scoped-path eligibility decision. The
// legacy control only clears CachedMembership; its callback and reusableSels
// are supplied by ConstructBlockPKFilter, not a benchmark reimplementation.
// Both routes reuse their output buffers and synchronously populated file cache.
// This measures a cached block boundary, not end-to-end IVF query throughput.
func BenchmarkConstructedMembershipBlockTopN(b *testing.B) {
	for _, compound := range []bool{false, true} {
		layout := "fixed_int32"
		if compound {
			layout = "compound_varchar"
		}
		b.Run(layout, func(b *testing.B) {
			fixture := newMembershipTopNBenchFixture(b, compound)
			for _, hits := range []struct {
				name  string
				count int
				id    int32
			}{
				{name: "no_hit", count: 0, id: membershipTopNBenchRows},
				{name: "single_hit", count: 1, id: membershipTopNBenchRows / 2},
				{name: "dense", count: membershipTopNBenchRows},
			} {
				b.Run(hits.name, func(b *testing.B) {
					for _, legacy := range []bool{true, false} {
						route := "automatic"
						if legacy {
							route = "legacy_callback"
						}
						b.Run(route, func(b *testing.B) {
							mp := mpool.MustNewZero()
							b.Cleanup(func() { mpool.DeleteMPool(mp) })
							members := vector.NewVec(types.T_int32.ToType())
							b.Cleanup(func() { members.Free(mp) })
							if hits.count == membershipTopNBenchRows {
								for row := 0; row < membershipTopNBenchRows; row++ {
									require.NoError(b, vector.AppendFixed(members, int32(row), false, mp))
								}
							} else {
								require.NoError(b, vector.AppendFixed(members, hits.id, false, mp))
							}
							payload, err := docfilter.Build(members)
							require.NoError(b, err)
							member, err := docfilter.New(payload)
							require.NoError(b, err)
							b.Cleanup(member.Free)
							filter, err := readutil.ConstructBlockPKFilter(false, fixture.base, member)
							require.NoError(b, err)
							if filter.Cleanup != nil {
								b.Cleanup(filter.Cleanup)
							}
							require.NotNil(b, filter.CachedMembership)
							if legacy {
								filter.CachedMembership = nil
							}

							output := batch.NewWithSize(len(fixture.columns))
							for i, typ := range fixture.columnTypes {
								output.Vecs[i] = vector.NewOffHeapVecWithType(typ)
							}
							b.Cleanup(func() { output.Clean(mp) })
							cacheVectors := containers.NewVectors(len(fixture.columns) + 1)
							b.Cleanup(func() { cacheVectors.Free(mp) })
							top := &objectio.IndexReaderTopOp{
								ColPos: 1, Limit: 10, Typ: types.T_array_float32,
								NumVec: types.ArrayToBytes([]float32{0, 0}), MetricType: metric.Metric_L2sqDistance,
							}
							read := func() error {
								output.CleanOnlyData()
								top.DistHeap = top.DistHeap[:0]
								return blockio.BlockDataRead(
									fixture.ctx, &fixture.info, &membershipTopNBenchSource{},
									fixture.columns, fixture.columnTypes, -1, timestamp.Timestamp{},
									fixture.filterColumns, fixture.filterTypes, filter, top,
									fileservice.Policy(0), "membership-topn-benchmark", output, cacheVectors, mp, fixture.fs,
								)
							}
							// Warm both the persisted-column cache and real callback's reused offsets.
							require.NoError(b, read())
							require.NoError(b, read())
							func() {
								location := fixture.info.MetaLocation()
								meta, err := objectio.FastLoadObjectMeta(fixture.ctx, &location, false, fixture.fs)
								require.NoError(b, err)
								dataMeta := meta.MustGetMeta(objectio.SchemaData)
								cached, err := objectio.ReadOneBlock(fixture.ctx, &dataMeta,
									location.Name().UnsafeString(), location.ID(), fixture.filterColumns, fixture.filterTypes,
									mp, fixture.fs, fileservice.Policy(0), objectio.ShareScopedDecodedColumn)
								require.NoError(b, err)
								defer cached.Release()
								for _, entry := range cached.Entries {
									require.True(b, entry.WasFromCache(), "filter column must be a cache hit")
								}
							}()
							wantRows := min(hits.count, 10)
							require.Equal(b, wantRows, output.RowCount())
							require.Zero(b, output.Vecs[1].Length(), "Top-K must not materialize embeddings")
							ids := vector.MustFixedColWithTypeCheck[int32](output.Vecs[0])
							for i, id := range ids {
								want := int32(i)
								if hits.count == 1 {
									want = hits.id
								}
								require.Equal(b, want, id)
							}
							b.ReportAllocs()
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								if err := read(); err != nil {
									b.Fatal(err)
								}
								if output.RowCount() != wantRows {
									b.Fatalf("got %d rows, want %d", output.RowCount(), wantRows)
								}
							}
							b.StopTimer()
						})
					}
				})
			}
		})
	}
}

type membershipTopNBenchSource struct{ engine.DataSource }

func (*membershipTopNBenchSource) ApplyTombstones(
	_ context.Context, _ *objectio.Blockid, rows []int64, _ engine.TombstoneApplyPolicy,
) ([]int64, error) {
	return rows, nil
}

type membershipTopNBenchFixture struct {
	ctx           context.Context
	fs            fileservice.FileService
	info          objectio.BlockInfo
	columns       []uint16
	columnTypes   []types.Type
	filterColumns []uint16
	filterTypes   []types.Type
	base          readutil.BasePKFilter
}

// Uses the same local-FS/ConstructWriter persisted fixture as the existing
// BlockDataRead vector benchmarks, with a two-dimensional payload so filter
// overhead is measurable rather than hidden under a large distance kernel.
func newMembershipTopNBenchFixture(b *testing.B, compound bool) membershipTopNBenchFixture {
	b.Helper()
	ctx := context.Background()
	capacity := toml.ByteSize(8 << 20)
	fs, err := fileservice.NewLocalFS2(ctx, defines.SharedFileServiceName, b.TempDir(),
		fileservice.CacheConfig{MemoryCapacity: &capacity}, nil)
	require.NoError(b, err)
	b.Cleanup(func() { fs.Close(ctx) })
	fs.SetAsyncUpdate(false)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	columnTypes := []types.Type{types.T_int32.ToType(), types.New(types.T_array_float32, 2, 0)}
	seqnums := []uint16{0, 1}
	filterColumns := []uint16{0}
	filterTypes := columnTypes[:1]
	base := readutil.BasePKFilter{}
	const prefix = "compound-index-entry-prefix/"
	if compound {
		columnTypes = append([]types.Type{types.T_varchar.ToType()}, columnTypes...)
		seqnums = []uint16{0, 1, 2}
		filterColumns = []uint16{0, 1}
		filterTypes = columnTypes[:2]
		base = readutil.BasePKFilter{Valid: true, Oid: types.T_varchar, Op: function.PREFIX_EQ, LB: []byte(prefix)}
	}
	input := batch.NewWithSize(len(columnTypes))
	defer input.Clean(mp)
	for i, typ := range columnTypes {
		input.Vecs[i] = vector.NewVec(typ)
	}
	idPos := len(columnTypes) - 2
	for row := 0; row < membershipTopNBenchRows; row++ {
		if compound {
			require.NoError(b, vector.AppendBytes(input.Vecs[0], []byte(fmt.Sprintf("%s%08d", prefix, row)), false, mp))
		}
		require.NoError(b, vector.AppendFixed(input.Vecs[idPos], int32(row), false, mp))
		require.NoError(b, vector.AppendArray(input.Vecs[idPos+1], []float32{float32(row + 1), 0}, false, mp))
	}
	input.SetRowCount(membershipTopNBenchRows)
	writer := ioutil.ConstructWriter(0, seqnums, -1, false, false, fs)
	_, err = writer.WriteBatch(input)
	require.NoError(b, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(b, err)
	stats := writer.GetObjectStats()
	return membershipTopNBenchFixture{
		ctx: ctx, fs: fs, info: stats.ConstructBlockInfo(0),
		columns: seqnums[idPos:], columnTypes: columnTypes[idPos:],
		filterColumns: filterColumns, filterTypes: filterTypes, base: base,
	}
}
