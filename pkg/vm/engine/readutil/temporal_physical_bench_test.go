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

package readutil

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

// Real writer -> metadata loader -> compiled sorted-key reader. Row kernels
// are measured separately; each tiny block keeps metadata/operator shape real
// without turning this calibration into a data-volume or elapsed-time UT.
func BenchmarkTemporalPhysicalReaderWork(b *testing.B) {
	for _, blocks := range []int{1, 64, 256, 1024} {
		b.Run(fmt.Sprintf("blocks=%d", blocks), func(b *testing.B) {
			ctx := b.Context()
			proc := testutil.NewProcess(b)
			defer proc.Free()
			fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
			require.NoError(b, err)
			defer fs.Close(ctx)
			packer := types.NewPacker()
			defer packer.Close()
			encode := func(i int) []byte {
				packer.Reset()
				packer.EncodeDatetime(types.Datetime(i))
				return append([]byte(nil), packer.GetBuf()...)
			}
			writer := ioutil.ConstructWriter(0, []uint16{0, 1}, 0, false, false, fs)
			bat := batch.NewWithSize(2)
			defer bat.Clean(proc.Mp())
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
			for i := 0; i < blocks; i++ {
				require.NoError(b, vector.AppendBytes(bat.Vecs[0], encode(i), false, proc.Mp()))
				require.NoError(b, vector.AppendFixed(bat.Vecs[1], int64(i), false, proc.Mp()))
				bat.SetRowCount(1)
				_, err = writer.WriteBatch(bat)
				require.NoError(b, err)
				bat.CleanOnlyData()
			}
			_, _, err = writer.Sync(ctx)
			require.NoError(b, err)
			stats := writer.GetObjectStats(objectio.WithSorted())
			location := objectio.BuildLocation(stats.ObjectName(), stats.Extent(), stats.Rows(), 0)
			meta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
			require.NoError(b, err)
			table := &plan.TableDef{Cols: []*plan.ColDef{{Name: "key", Primary: true, Typ: types.T_varchar.ToType().PlanType()}, {Name: "id", Seqnum: 1, Typ: types.T_int64.ToType().PlanType()}}, Name2ColIndex: map[string]int32{"key": 0, "id": 1}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "key", Names: []string{"key"}}}
			expr := MakeFunctionExprForTest("prefix_between", []*plan.Expr{MakeColExprForTest(0, types.T_varchar, "key"), plan2.MakePlan2StringConstExprWithType(string(encode(blocks-1)), true), plan2.MakePlan2StringConstExprWithType(string(encode(blocks)), true)})
			expr.TemporalCandidateFilter = true
			var exes []colexec.ExpressionExecutor
			plan2.ReplaceFoldExpr(proc, expr, &exes)
			for _, exe := range exes {
				defer exe.Free()
			}
			plan2.EvalFoldExpr(proc, expr, &exes)
			fast, load, obj, block, seek, compiled, high := CompileFilterExpr(expr, table, fs)
			require.True(b, compiled)
			require.NotNil(b, fast)
			require.NotNil(b, seek)
			var out objectio.BlockInfoSlice
			_, loads, _, _, _, _, _, _, err := FilterObjects(ctx, engine.RangesParam{}, fast, load, obj, block, seek, nil, nil, []objectio.ObjectStats{stats}, &out, high, nil, fs)
			require.NoError(b, err)
			require.Equal(b, 1, loads)
			require.Equal(b, 1, out.Len())
			for _, kind := range []string{"object_fast", "seek", "block_filter", "metadata_cache", "metadata_read_decode", "reader"} {
				b.Run(kind, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						switch kind {
						case "object_fast":
							_, err = fast(&stats)
						case "seek":
							_ = seek(meta.MustDataMeta())
						case "block_filter":
							_, _, err = block(blocks-1, meta.MustDataMeta().GetBlockMeta(uint32(blocks-1)), nil)
						case "metadata_cache":
							_, _, err = load(ctx, &stats, nil, nil)
						case "metadata_read_decode":
							extent := stats.Extent()
							_, err = objectio.ReadExtent(ctx, stats.ObjectName().String(), &extent, 0, fs, func(size int64, alg uint8) objectio.CacheConstructor {
								return func(ctx context.Context, r io.Reader, data []byte, a fileservice.CacheDataAllocator) (fscache.Data, error) {
									if len(data) == 0 {
										var err error
										data, err = io.ReadAll(r)
										if err != nil {
											return nil, err
										}
									}
									return objectio.DecompressColumnExtent(ctx, data, objectio.NewExtent(alg, 0, uint32(len(data)), uint32(size)), a)
								}
							})
						case "reader":
							out = nil
							_, _, _, _, _, _, _, _, err = FilterObjects(ctx, engine.RangesParam{}, fast, load, obj, block, seek, nil, nil, []objectio.ObjectStats{stats}, &out, high, nil, fs)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
					if kind == "seek" || kind == "reader" {
						b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*blocks), "ns/block")
					}
					if kind == "metadata_read_decode" {
						b.ReportMetric(float64(stats.Extent().Length()), "compressed-B/op")
						b.ReportMetric(float64(stats.Extent().OriginSize()), "decoded-B/op")
					}
				})
			}
		})
	}
}
