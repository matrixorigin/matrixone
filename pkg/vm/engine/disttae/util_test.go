// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestLinearSearchOffsetByValFactory_Varchar(t *testing.T) {
	mp := mpool.MustNewZero()

	// Build keys vector with varchar values
	keys := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(keys, []byte("alice"), false, mp))
	require.NoError(t, vector.AppendBytes(keys, []byte("bob"), false, mp))

	searchFn := LinearSearchOffsetByValFactory(keys)

	// Target vector that does NOT contain the keys
	target := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(target, []byte("charlie"), false, mp))
	require.NoError(t, vector.AppendBytes(target, []byte("dave"), false, mp))

	hits := searchFn(target)
	require.Empty(t, hits, "should not match when target has different values")

	// Target vector that contains one of the keys
	target2 := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(target2, []byte("charlie"), false, mp))
	require.NoError(t, vector.AppendBytes(target2, []byte("bob"), false, mp))
	require.NoError(t, vector.AppendBytes(target2, []byte("dave"), false, mp))

	hits2 := searchFn(target2)
	require.Equal(t, []int64{1}, hits2, "should match 'bob' at index 1")

	keys.Free(mp)
	target.Free(mp)
	target2.Free(mp)
}

// narrowArrayLinearSearch exercises LinearSearchOffsetByValFactory for a narrow
// vector array key type (vecbf16/vecf16/vecint8/vecuint8). Both the key-side map
// build and the target-side search switch on the element type, so a narrow array
// must be handled in both or this panics "not supported".
func narrowArrayLinearSearch[T types.ArrayElement](t *testing.T, mp *mpool.MPool, oid types.T, a, b, c []T) {
	typ := types.New(oid, int32(len(a)), 0)

	keys := vector.NewVec(typ)
	require.NoError(t, vector.AppendArray[T](keys, a, false, mp))
	require.NoError(t, vector.AppendArray[T](keys, b, false, mp))
	searchFn := LinearSearchOffsetByValFactory(keys)

	// target with no matching key
	target := vector.NewVec(typ)
	require.NoError(t, vector.AppendArray[T](target, c, false, mp))
	require.Empty(t, searchFn(target))

	// target containing key b at index 1
	target2 := vector.NewVec(typ)
	require.NoError(t, vector.AppendArray[T](target2, c, false, mp))
	require.NoError(t, vector.AppendArray[T](target2, b, false, mp))
	require.Equal(t, []int64{1}, searchFn(target2))

	keys.Free(mp)
	target.Free(mp)
	target2.Free(mp)
}

func TestLinearSearchOffsetByValFactory_NarrowArray(t *testing.T) {
	mp := mpool.MustNewZero()
	narrowArrayLinearSearch[types.Float16](t, mp, types.T_array_float16,
		types.Float32ToFloat16Slice([]float32{1, 1}),
		types.Float32ToFloat16Slice([]float32{2, 2}),
		types.Float32ToFloat16Slice([]float32{3, 3}))
	narrowArrayLinearSearch[types.BF16](t, mp, types.T_array_bf16,
		types.Float32ToBF16Slice([]float32{1, 1}),
		types.Float32ToBF16Slice([]float32{2, 2}),
		types.Float32ToBF16Slice([]float32{3, 3}))
	narrowArrayLinearSearch[int8](t, mp, types.T_array_int8,
		[]int8{1, 1}, []int8{2, 2}, []int8{3, 3})
	narrowArrayLinearSearch[uint8](t, mp, types.T_array_uint8,
		[]uint8{1, 1}, []uint8{2, 2}, []uint8{3, 3})
}

func TestLinearSearchOffsetByValFactory_Int64(t *testing.T) {
	mp := mpool.MustNewZero()

	keys := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(keys, int64(10), false, mp))
	require.NoError(t, vector.AppendFixed(keys, int64(20), false, mp))

	searchFn := LinearSearchOffsetByValFactory(keys)

	target := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(target, int64(5), false, mp))
	require.NoError(t, vector.AppendFixed(target, int64(15), false, mp))

	require.Empty(t, searchFn(target))

	target2 := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(target2, int64(20), false, mp))
	require.NoError(t, vector.AppendFixed(target2, int64(30), false, mp))
	require.NoError(t, vector.AppendFixed(target2, int64(10), false, mp))

	require.Equal(t, []int64{0, 2}, searchFn(target2))

	keys.Free(mp)
	target.Free(mp)
	target2.Free(mp)
}

func TestTombstonePKExistsInRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	pState := logtailreplay.NewPartitionState("", true, 0, false)
	int32Type := types.T_int32.ToType()

	// Helper: write a CN tombstone object with given PK values, return its ObjectStats.
	writeTombstone := func(pkValues []int32) objectio.ObjectStats {
		writer := colexec.NewCNS3TombstoneWriter(proc.Mp(), fs, int32Type, -1)
		bat := readutil.NewCNTombstoneBatch(&int32Type, objectio.HiddenColumnSelection_None)
		for _, pk := range pkValues {
			vector.AppendFixed[types.Rowid](bat.Vecs[0], types.RandomRowid(), false, proc.GetMPool())
			vector.AppendFixed[int32](bat.Vecs[1], pk, false, proc.GetMPool())
		}
		bat.SetRowCount(bat.Vecs[0].Length())
		require.NoError(t, writer.Write(ctx, bat))
		ss, err := writer.Sync(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(ss))
		return ss[0]
	}

	// Write tombstone with PKs [100, 200, 300]
	stats1 := writeTombstone([]int32{100, 200, 300})
	// Write tombstone with PKs [400, 500]
	stats2 := writeTombstone([]int32{400, 500})

	// Insert into partition state with CreateTime after 'from'
	from := types.BuildTS(10, 0)
	require.NoError(t, pState.HandleObjectEntry(ctx, fs, objectio.ObjectEntry{
		ObjectStats: stats1,
		CreateTime:  types.BuildTS(15, 0),
	}, true))
	require.NoError(t, pState.HandleObjectEntry(ctx, fs, objectio.ObjectEntry{
		ObjectStats: stats2,
		CreateTime:  types.BuildTS(20, 0),
	}, true))
	// Case 1: search for PK=200, should find it
	keys1 := vector.NewVec(int32Type)
	require.NoError(t, vector.AppendFixed[int32](keys1, 200, false, proc.GetMPool()))
	changed, _, err := tombstonePKExistsInRange(ctx, pState, from, types.MaxTs(), keys1, int32Type, fs, proc.GetMPool())
	require.NoError(t, err)
	require.True(t, changed)

	// Case 2: search for PK=999, should not find it
	keys2 := vector.NewVec(int32Type)
	require.NoError(t, vector.AppendFixed[int32](keys2, 999, false, proc.GetMPool()))
	changed, _, err = tombstonePKExistsInRange(ctx, pState, from, types.MaxTs(), keys2, int32Type, fs, proc.GetMPool())
	require.NoError(t, err)
	require.False(t, changed)

	// Case 3: search for PK=500, should find it in second tombstone
	keys3 := vector.NewVec(int32Type)
	require.NoError(t, vector.AppendFixed[int32](keys3, 500, false, proc.GetMPool()))
	changed, _, err = tombstonePKExistsInRange(ctx, pState, from, types.MaxTs(), keys3, int32Type, fs, proc.GetMPool())
	require.NoError(t, err)
	require.True(t, changed)

	// Case 4: no tombstone objects changed after from=25
	changed, _, err = tombstonePKExistsInRange(ctx, pState, types.BuildTS(25, 0), types.MaxTs(), keys1, int32Type, fs, proc.GetMPool())
	require.NoError(t, err)
	require.False(t, changed)
}

func TestTombstonePKExistsInRangeVarcharScopedSearch(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProc(t)
	fs, err := fileservice.Get[fileservice.FileService](
		proc.GetFileService(),
		defines.SharedFileServiceName,
	)
	require.NoError(t, err)
	mp := proc.GetMPool()
	varcharType := types.T_varchar.ToType()

	dataObjectID := objectio.NewObjectid()
	makeRowid := func(row uint32) types.Rowid {
		return types.NewRowIDWithObjectIDBlkNumAndRowID(dataObjectID, 0, row)
	}

	tnWriter := ioutil.ConstructTombstoneWriter(
		objectio.HiddenColumnSelection_CommitTS,
		fs,
	)
	tnBatch := batch.NewWithSize(3)
	tnBatch.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tnBatch.Vecs[1] = vector.NewVec(varcharType)
	tnBatch.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	for row, value := range []string{"z", "needle", "a"} {
		require.NoError(t, vector.AppendFixed(tnBatch.Vecs[0], makeRowid(uint32(row)), false, mp))
		require.NoError(t, vector.AppendBytes(tnBatch.Vecs[1], []byte(value), false, mp))
		require.NoError(t, vector.AppendFixed(
			tnBatch.Vecs[2],
			types.BuildTS(int64((row+1)*10), 0),
			false,
			mp,
		))
	}
	tnBatch.SetRowCount(3)
	_, err = tnWriter.WriteBatch(tnBatch)
	require.NoError(t, err)
	_, _, err = tnWriter.Sync(ctx)
	require.NoError(t, err)
	tnStats := tnWriter.GetObjectStats()
	tnBatch.Clean(mp)

	tnState := logtailreplay.NewPartitionState("", true, 0, false)
	require.NoError(t, tnState.HandleObjectEntry(ctx, fs, objectio.ObjectEntry{
		ObjectStats: tnStats,
		CreateTime:  types.BuildTS(40, 0),
	}, true))
	key := vector.NewVec(varcharType)
	require.NoError(t, vector.AppendBytes(key, []byte("needle"), false, mp))
	baseline := mp.CurrNB()

	changed, reason, err := tombstonePKExistsInRange(
		ctx,
		tnState,
		types.BuildTS(15, 0),
		types.BuildTS(25, 0),
		key,
		varcharType,
		fs,
		mp,
	)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, "tombstone_commit_ts_hit", reason)
	require.Equal(t, baseline, mp.CurrNB())

	changed, reason, err = tombstonePKExistsInRange(
		ctx,
		tnState,
		types.BuildTS(20, 0),
		types.BuildTS(30, 0),
		key,
		varcharType,
		fs,
		mp,
	)
	require.NoError(t, err)
	require.False(t, changed, "the selected commit timestamp equals the open lower bound")
	require.Empty(t, reason)
	require.Equal(t, baseline, mp.CurrNB())

	missing := vector.NewVec(varcharType)
	require.NoError(t, vector.AppendBytes(missing, []byte("missing"), false, mp))
	changed, reason, err = tombstonePKExistsInRange(
		ctx,
		tnState,
		types.BuildTS(15, 0),
		types.BuildTS(25, 0),
		missing,
		varcharType,
		fs,
		mp,
	)
	require.NoError(t, err)
	require.False(t, changed)
	require.Empty(t, reason)

	cnWriter := ioutil.ConstructTombstoneWriter(
		objectio.HiddenColumnSelection_None,
		fs,
	)
	cnBatch := batch.NewWithSize(2)
	cnBatch.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	cnBatch.Vecs[1] = vector.NewVec(varcharType)
	require.NoError(t, vector.AppendFixed(cnBatch.Vecs[0], makeRowid(3), false, mp))
	require.NoError(t, vector.AppendBytes(cnBatch.Vecs[1], []byte("needle"), false, mp))
	cnBatch.SetRowCount(1)
	_, err = cnWriter.WriteBatch(cnBatch)
	require.NoError(t, err)
	_, _, err = cnWriter.Sync(ctx)
	require.NoError(t, err)
	cnObjectStats := cnWriter.GetObjectStats()
	cnStats := cnObjectStats.Clone()
	objectio.WithCNCreated()(cnStats)
	cnBatch.Clean(mp)
	cnState := logtailreplay.NewPartitionState("", true, 0, false)
	require.NoError(t, cnState.HandleObjectEntry(ctx, fs, objectio.ObjectEntry{
		ObjectStats: *cnStats,
		CreateTime:  types.BuildTS(40, 0),
	}, true))
	changed, reason, err = tombstonePKExistsInRange(
		ctx,
		cnState,
		types.BuildTS(15, 0),
		types.BuildTS(25, 0),
		key,
		varcharType,
		fs,
		mp,
	)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, "tombstone_cn_hit", reason)

	key.Free(mp)
	missing.Free(mp)
}

func TestBlockMetaMarshal(t *testing.T) {
	location := []byte("test")
	var info objectio.BlockInfo
	info.SetMetaLocation(location)
	data := objectio.EncodeBlockInfo(&info)
	info2 := objectio.DecodeBlockInfo(data)
	require.Equal(t, info, *info2)
}

func TestCheckExprIsZonemappable(t *testing.T) {
	type asserts = struct {
		result bool
		expr   *plan.Expr
	}
	testCases := []asserts{
		// a > 1  -> true
		{true, readutil.MakeFunctionExprForTest(">", []*plan.Expr{
			readutil.MakeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(10),
		})},
		// a >= b -> true
		{true, readutil.MakeFunctionExprForTest(">=", []*plan.Expr{
			readutil.MakeColExprForTest(0, types.T_int64),
			readutil.MakeColExprForTest(1, types.T_int64),
		})},
		// abs(a) -> false
		{false, readutil.MakeFunctionExprForTest("abs", []*plan.Expr{
			readutil.MakeColExprForTest(0, types.T_int64),
		})},
	}

	t.Run("test checkExprIsZonemappable", func(t *testing.T) {
		for i, testCase := range testCases {
			zonemappable := plan2.ExprIsZonemappable(context.TODO(), testCase.expr)
			if zonemappable != testCase.result {
				t.Fatalf("checkExprIsZonemappable testExprs[%d] is different with expected", i)
			}
		}
	})
}

func TestEvalZonemapFilter(t *testing.T) {
	m := mpool.MustNew(t.Name())
	proc := testutil.NewProcessWithMPool(t, "", m)
	type myCase = struct {
		exprs  []*plan.Expr
		meta   objectio.BlockObject
		desc   []string
		expect []bool
	}

	zm0 := index.NewZM(types.T_float64, 0)
	zm0.Update(float64(-10))
	zm0.Update(float64(20))
	zm1 := index.NewZM(types.T_float64, 0)
	zm1.Update(float64(5))
	zm1.Update(float64(25))
	zm2 := index.NewZM(types.T_varchar, 0)
	zm2.Update([]byte("abc"))
	zm2.Update([]byte("opq"))
	zm3 := index.NewZM(types.T_varchar, 0)
	zm3.Update([]byte("efg"))
	zm3.Update(index.MaxBytesValue)
	cases := []myCase{
		{
			desc: []string{
				"a>10", "a>30", "a<=-10", "a<-10", "a+b>60", "a+b<-5", "a-b<-34", "a-b<-35", "a-b<=-35", "a>b",
				"a>b+15", "a>=b+15", "a>100 or b>10", "a>100 and b<0", "d>xyz", "d<=efg", "d<efg", "c>d", "c<d",
			},
			exprs: []*plan.Expr{
				readutil.MakeFunctionExprForTest(">", []*plan.Expr{
					readutil.MakeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(10),
				}),
				readutil.MakeFunctionExprForTest(">", []*plan.Expr{
					readutil.MakeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(30),
				}),
				readutil.MakeFunctionExprForTest("<=", []*plan.Expr{
					readutil.MakeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(-10),
				}),
				readutil.MakeFunctionExprForTest("<", []*plan.Expr{
					readutil.MakeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(-10),
				}),
				readutil.MakeFunctionExprForTest(">", []*plan.Expr{
					readutil.MakeFunctionExprForTest("+", []*plan.Expr{
						readutil.MakeColExprForTest(0, types.T_float64),
						readutil.MakeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(60),
				}),
				readutil.MakeFunctionExprForTest("<", []*plan.Expr{
					readutil.MakeFunctionExprForTest("+", []*plan.Expr{
						readutil.MakeColExprForTest(0, types.T_float64),
						readutil.MakeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(-5),
				}),
				readutil.MakeFunctionExprForTest("<", []*plan.Expr{
					readutil.MakeFunctionExprForTest("-", []*plan.Expr{
						readutil.MakeColExprForTest(0, types.T_float64),
						readutil.MakeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(-34),
				}),
				readutil.MakeFunctionExprForTest("<", []*plan.Expr{
					readutil.MakeFunctionExprForTest("-", []*plan.Expr{
						readutil.MakeColExprForTest(0, types.T_float64),
						readutil.MakeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(-35),
				}),
				readutil.MakeFunctionExprForTest("<=", []*plan.Expr{
					readutil.MakeFunctionExprForTest("-", []*plan.Expr{
						readutil.MakeColExprForTest(0, types.T_float64),
						readutil.MakeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(-35),
				}),
				readutil.MakeFunctionExprForTest(">", []*plan.Expr{
					readutil.MakeColExprForTest(0, types.T_float64),
					readutil.MakeColExprForTest(1, types.T_float64),
				}),
				readutil.MakeFunctionExprForTest(">", []*plan.Expr{
					readutil.MakeColExprForTest(0, types.T_float64),
					readutil.MakeFunctionExprForTest("+", []*plan.Expr{
						readutil.MakeColExprForTest(1, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(15),
					}),
				}),
				readutil.MakeFunctionExprForTest(">=", []*plan.Expr{
					readutil.MakeColExprForTest(0, types.T_float64),
					readutil.MakeFunctionExprForTest("+", []*plan.Expr{
						readutil.MakeColExprForTest(1, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(15),
					}),
				}),
				readutil.MakeFunctionExprForTest("or", []*plan.Expr{
					readutil.MakeFunctionExprForTest(">", []*plan.Expr{
						readutil.MakeColExprForTest(0, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(100),
					}),
					readutil.MakeFunctionExprForTest(">", []*plan.Expr{
						readutil.MakeColExprForTest(1, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(10),
					}),
				}),
				readutil.MakeFunctionExprForTest("and", []*plan.Expr{
					readutil.MakeFunctionExprForTest(">", []*plan.Expr{
						readutil.MakeColExprForTest(0, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(100),
					}),
					readutil.MakeFunctionExprForTest("<", []*plan.Expr{
						readutil.MakeColExprForTest(1, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(0),
					}),
				}),
				readutil.MakeFunctionExprForTest(">", []*plan.Expr{
					readutil.MakeColExprForTest(3, types.T_varchar),
					plan2.MakePlan2StringConstExprWithType("xyz"),
				}),
				readutil.MakeFunctionExprForTest("<=", []*plan.Expr{
					readutil.MakeColExprForTest(3, types.T_varchar),
					plan2.MakePlan2StringConstExprWithType("efg"),
				}),
				readutil.MakeFunctionExprForTest("<", []*plan.Expr{
					readutil.MakeColExprForTest(3, types.T_varchar),
					plan2.MakePlan2StringConstExprWithType("efg"),
				}),
				readutil.MakeFunctionExprForTest(">", []*plan.Expr{
					readutil.MakeColExprForTest(2, types.T_varchar),
					readutil.MakeColExprForTest(3, types.T_varchar),
				}),
				readutil.MakeFunctionExprForTest("<", []*plan.Expr{
					readutil.MakeColExprForTest(2, types.T_varchar),
					readutil.MakeColExprForTest(3, types.T_varchar),
				}),
			},
			meta: func() objectio.BlockObject {
				objDataMeta := objectio.BuildMetaData(1, 4)
				meta := objDataMeta.GetBlockMeta(0)
				meta.MustGetColumn(0).SetZoneMap(zm0)
				meta.MustGetColumn(1).SetZoneMap(zm1)
				meta.MustGetColumn(2).SetZoneMap(zm2)
				meta.MustGetColumn(3).SetZoneMap(zm3)
				return meta
			}(),
			expect: []bool{
				true, false, true, false, false, false, true, false, true, true,
				false, true, true, false, true, true, false, true, true,
			},
		},
	}

	columnMap := map[int]int{0: 0, 1: 1, 2: 2, 3: 3}

	for _, tc := range cases {
		for i, expr := range tc.exprs {
			cnt := plan2.AssignAuxIdForExpr(expr, 0)
			zms := make([]objectio.ZoneMap, cnt)
			vecs := make([]*vector.Vector, cnt)
			zm := colexec.EvaluateFilterByZoneMap(context.Background(), proc, expr, tc.meta, columnMap, zms, vecs)
			require.Equal(t, tc.expect[i], zm, tc.desc[i])
		}
	}
	require.Zero(t, m.CurrNB())
}

func mockStatsList(t *testing.T, statsCnt int) (statsList []objectio.ObjectStats) {
	for idx := 0; idx < statsCnt; idx++ {
		stats := objectio.NewObjectStats()
		blkCnt := rand.Uint32()%100 + 1
		require.Nil(t, objectio.SetObjectStatsBlkCnt(stats, blkCnt))
		require.Nil(t, objectio.SetObjectStatsRowCnt(stats, objectio.BlockMaxRows*(blkCnt-1)+objectio.BlockMaxRows*6/10))
		require.Nil(t, objectio.SetObjectStatsObjectName(stats, objectio.BuildObjectName(objectio.NewSegmentid(), uint16(blkCnt))))
		require.Nil(t, objectio.SetObjectStatsExtent(stats, objectio.NewExtent(0, 0, 0, 0)))
		require.Nil(t, objectio.SetObjectStatsSortKeyZoneMap(stats, index.NewZM(types.T_bool, 1)))

		statsList = append(statsList, *stats)
	}

	return
}

func TestForeachBlkInObjStatsList(t *testing.T) {
	statsList := mockStatsList(t, 100)

	count := 0
	objectio.ForeachBlkInObjStatsList(false, nil, func(blk objectio.BlockInfo, _ objectio.BlockObject) bool {
		count++
		return false
	}, statsList...)

	require.Equal(t, count, 1)

	count = 0
	objectio.ForeachBlkInObjStatsList(true, nil, func(blk objectio.BlockInfo, _ objectio.BlockObject) bool {
		count++
		return false
	}, statsList...)

	require.Equal(t, count, len(statsList))

	count = 0
	objectio.ForeachBlkInObjStatsList(true, nil, func(blk objectio.BlockInfo, _ objectio.BlockObject) bool {
		count++
		return true
	}, statsList...)

	objectio.ForeachObjectStats(func(stats *objectio.ObjectStats) bool {
		count -= int(stats.BlkCnt())
		return true
	}, statsList...)

	require.Equal(t, count, 0)

	count = 0
	objectio.ForeachBlkInObjStatsList(false, nil, func(blk objectio.BlockInfo, _ objectio.BlockObject) bool {
		count++
		return true
	}, statsList...)

	objectio.ForeachObjectStats(func(stats *objectio.ObjectStats) bool {
		count -= int(stats.BlkCnt())
		return true
	}, statsList...)

	require.Equal(t, count, 0)
}

func TestDeletedBlocks_GetDeletedRowIDs(t *testing.T) {
	delBlks := deletedBlocks{
		offsets: map[types.Blockid][]int64{},
	}
	for i := 0; i < 100; i++ {
		row := types.RandomRowid()
		bid, offset := row.Decode()

		delBlks.offsets[*bid] = append(delBlks.offsets[*bid], int64(offset))
	}

	rowIds := make([]types.Rowid, 0)

	delBlks.getDeletedRowIDs(func(row types.Rowid) {
		rowIds = append(rowIds, row)
	})

	for i := range rowIds {
		bid, offset := rowIds[i].Decode()
		have, ok := delBlks.offsets[*bid]
		require.True(t, ok)
		require.NotEqual(t, 0, len(have))

		x := slices.Index(have, int64(offset))
		require.NotEqual(t, -1, x)
	}

	selected := rowIds[0].CloneBlockID()
	scoped := make([]types.Rowid, 0, len(delBlks.offsets[selected]))
	delBlks.getDeletedRowIDsForBlocks([]types.Blockid{selected}, func(row types.Rowid) {
		scoped = append(scoped, row)
	})
	require.Len(t, scoped, len(delBlks.offsets[selected]))
	for i := range scoped {
		require.True(t, scoped[i].BorrowBlockID().EQ(&selected))
	}
}

func TestConcurrentExecutor_Run(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ex := newConcurrentExecutor(3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ex.Run(ctx)
	require.Equal(t, 3, ex.GetConcurrency())

	var wg sync.WaitGroup
	submit := func(task concurrentTask) {
		wg.Add(1)
		err := ex.AppendTask(ctx, task, func(error) {
			wg.Done()
		})
		if err != nil {
			wg.Done()
		}
		require.NoError(t, err)
	}
	submit(func() error { return nil })
	submit(func() error { return context.Canceled })
	submit(func() error { return io.EOF })
	wg.Wait()
}

func TestConcurrentExecutorRejectsQueuedTasksOnShutdown(t *testing.T) {
	ex := newConcurrentExecutor(1)
	executorCtx, stopExecutor := context.WithCancel(context.Background())
	defer stopExecutor()
	ex.Run(executorCtx)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	require.NoError(t, ex.AppendTask(context.Background(), func() error {
		close(firstStarted)
		<-releaseFirst
		return nil
	}, func(err error) {
		if err != nil {
			t.Errorf("running task was unexpectedly rejected: %v", err)
		}
	}))
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		close(releaseFirst)
		t.Fatal("executor did not start its admitted task")
	}

	secondRan := atomic.Bool{}
	secondRejected := make(chan error, 1)
	require.NoError(t, ex.AppendTask(context.Background(), func() error {
		secondRan.Store(true)
		return nil
	}, func(err error) {
		secondRejected <- err
	}))

	stopExecutor()
	<-ex.(*concurrentExecutor).stopCh
	close(releaseFirst)
	select {
	case err := <-secondRejected:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("executor shutdown abandoned an admitted task")
	}
	require.False(t, secondRan.Load())

	err := ex.AppendTask(context.Background(), func() error { return nil }, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func visibleObjectStateForExecutorTest(t *testing.T, count int) *logtailreplay.PartitionState {
	t.Helper()
	ctx := context.Background()
	state := logtailreplay.NewPartitionState("", true, 42, false)
	state.UpdateDuration(types.TS{}, types.MaxTs())
	for i := 0; i < count; i++ {
		oid := types.NewObjectid()
		stats := objectio.NewObjectStatsWithObjectID(&oid, false, false, false)
		require.NoError(t, objectio.SetObjectStatsSize(stats, 1))
		require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
			ObjectStats: *stats,
			CreateTime:  types.BuildTS(int64(i+1), 0),
		}, false))
	}
	return state
}

// delayedLifecycleContext models a conforming lifecycle whose Done predicate is
// observable before its AfterFunc notification is dispatched. This keeps the
// cancellation race deterministic without sleeps or scheduler assumptions.
type delayedLifecycleContext struct {
	done chan struct{}

	mu        sync.Mutex
	err       error
	callbacks []*delayedLifecycleCallback
	// cancelOnRegister closes Done while context.AfterFunc is registering its
	// callback, but before that callback is dispatched.
	cancelOnRegister bool
}

type delayedLifecycleCallback struct {
	active bool
	fn     func()
}

func newDelayedLifecycleContext() *delayedLifecycleContext {
	return &delayedLifecycleContext{done: make(chan struct{})}
}

func (c *delayedLifecycleContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *delayedLifecycleContext) Done() <-chan struct{}       { return c.done }
func (c *delayedLifecycleContext) Value(any) any               { return nil }

func (c *delayedLifecycleContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *delayedLifecycleContext) AfterFunc(fn func()) func() bool {
	c.mu.Lock()
	callback := &delayedLifecycleCallback{active: true, fn: fn}
	c.callbacks = append(c.callbacks, callback)
	if c.cancelOnRegister && c.err == nil {
		c.err = context.Canceled
		close(c.done)
	}
	c.mu.Unlock()
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !callback.active {
			return false
		}
		callback.active = false
		return true
	}
}

func (c *delayedLifecycleContext) cancelWithoutNotification() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return
	}
	c.err = context.Canceled
	close(c.done)
}

type inlineLifecycleExecutor struct {
	lifecycle context.Context
}

func (e *inlineLifecycleExecutor) AppendTask(
	ctx context.Context,
	task concurrentTask,
	complete func(error),
) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	err := task()
	if complete != nil {
		complete(err)
	}
	return nil
}

func (*inlineLifecycleExecutor) Run(context.Context) {}

func (e *inlineLifecycleExecutor) LifecycleContext() context.Context {
	return e.lifecycle
}

func (*inlineLifecycleExecutor) GetConcurrency() int { return 1 }

func TestForeachVisibleObjectsPropagatesConcurrentTaskError(t *testing.T) {
	state := visibleObjectStateForExecutorTest(t, 2)
	ex := newConcurrentExecutor(2)
	executorCtx, stopExecutor := context.WithCancel(context.Background())
	defer stopExecutor()
	ex.Run(executorCtx)

	wantErr := errors.New("object metadata unavailable")
	allStarted := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(allStarted) }) })
	var calls atomic.Int32
	result := make(chan error, 1)
	go func() {
		result <- ForeachVisibleObjects(
			context.Background(), state, types.MaxTs(),
			func(_ context.Context, _ objectio.ObjectEntry) error {
				call := calls.Add(1)
				if call == 2 {
					releaseOnce.Do(func() { close(allStarted) })
				}
				<-allStarted
				if call == 1 {
					return wantErr
				}
				return nil
			},
			ex,
			false,
		)
	}()
	var err error
	select {
	case err = <-result:
	case <-time.After(time.Second):
		t.Fatal("concurrent visible-object traversal did not join its tasks")
	}
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, int32(2), calls.Load(), "one successful and one failed task must both be joined")
}

func TestForeachVisibleObjectsHonorsCancellationBeforeAdmission(t *testing.T) {
	state := visibleObjectStateForExecutorTest(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancel()

	var called atomic.Bool
	err := ForeachVisibleObjects(
		ctx, state, types.MaxTs(),
		func(context.Context, objectio.ObjectEntry) error {
			called.Store(true)
			return nil
		},
		newConcurrentExecutor(1),
		false,
	)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, called.Load())
}

func TestForeachVisibleObjectsCancelsInFlightTask(t *testing.T) {
	state := visibleObjectStateForExecutorTest(t, 1)
	ex := newConcurrentExecutor(1)
	executorCtx, stopExecutor := context.WithCancel(context.Background())
	defer stopExecutor()
	ex.Run(executorCtx)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	taskStarted := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- ForeachVisibleObjects(
			ctx, state, types.MaxTs(),
			func(taskCtx context.Context, _ objectio.ObjectEntry) error {
				close(taskStarted)
				<-taskCtx.Done()
				return context.Cause(taskCtx)
			},
			ex,
			false,
		)
	}()

	select {
	case <-taskStarted:
	case <-time.After(time.Second):
		t.Fatal("executor did not start the admitted visible-object task")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("in-flight object task ignored caller cancellation")
	}
}

func TestForeachVisibleObjectsCancelsInFlightTaskOnExecutorShutdown(t *testing.T) {
	state := visibleObjectStateForExecutorTest(t, 1)
	ex := newConcurrentExecutor(1)
	executorCtx, stopExecutor := context.WithCancel(context.Background())
	defer stopExecutor()
	ex.Run(executorCtx)

	taskStarted := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- ForeachVisibleObjects(
			context.Background(), state, types.MaxTs(),
			func(taskCtx context.Context, _ objectio.ObjectEntry) error {
				close(taskStarted)
				<-taskCtx.Done()
				return context.Cause(taskCtx)
			},
			ex,
			false,
		)
	}()

	select {
	case <-taskStarted:
	case <-time.After(time.Second):
		t.Fatal("executor did not start the admitted visible-object task")
	}
	stopExecutor()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("in-flight object task outlived executor shutdown")
	}
}

func TestForeachVisibleObjectsRejectsExecutorShutdownWhenTaskReturnsNil(t *testing.T) {
	state := visibleObjectStateForExecutorTest(t, 1)
	ex := newConcurrentExecutor(1)
	executorCtx, stopExecutor := context.WithCancel(context.Background())
	t.Cleanup(stopExecutor)
	ex.Run(executorCtx)

	taskStarted := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- ForeachVisibleObjects(
			context.Background(), state, types.MaxTs(),
			func(taskCtx context.Context, _ objectio.ObjectEntry) error {
				close(taskStarted)
				<-taskCtx.Done()
				// Model a callback that observes shutdown only as a release
				// signal and fails to propagate the context error itself.
				return nil
			},
			ex,
			false,
		)
	}()

	select {
	case <-taskStarted:
	case <-time.After(time.Second):
		t.Fatal("executor did not start the admitted visible-object task")
	}
	stopExecutor()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled,
			"executor shutdown must remain visible even when a callback returns nil")
	case <-time.After(time.Second):
		t.Fatal("visible-object traversal did not join the shutdown task")
	}
}

func TestForeachVisibleObjectsReadsLifecyclePredicateAfterJoin(t *testing.T) {
	state := visibleObjectStateForExecutorTest(t, 1)
	lifecycle := newDelayedLifecycleContext()
	ex := &inlineLifecycleExecutor{lifecycle: lifecycle}

	err := ForeachVisibleObjects(
		context.Background(), state, types.MaxTs(),
		func(context.Context, objectio.ObjectEntry) error {
			lifecycle.cancelWithoutNotification()
			return nil
		},
		ex,
		false,
	)
	require.ErrorIs(t, err, context.Canceled,
		"executor shutdown is a failed traversal even before async notification dispatch")
}

func TestForeachVisibleObjectsClosesLifecycleWatcherRegistrationRace(t *testing.T) {
	state := visibleObjectStateForExecutorTest(t, 1)
	lifecycle := newDelayedLifecycleContext()
	lifecycle.cancelOnRegister = true
	ex := &inlineLifecycleExecutor{lifecycle: lifecycle}
	var called atomic.Bool

	err := ForeachVisibleObjects(
		context.Background(), state, types.MaxTs(),
		func(context.Context, objectio.ObjectEntry) error {
			called.Store(true)
			return nil
		},
		ex,
		false,
	)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, called.Load(),
		"work must not be admitted after lifecycle cancellation becomes observable")
}

func TestCollectAndCalculateStatsRejectsStoppedExecutorOnZeroObjectFastPath(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	cancel()
	ex := &inlineLifecycleExecutor{lifecycle: lifecycle}
	req := &updateStatsRequest{
		statsInfo:       plan2.NewStatsInfo(),
		tableDef:        &plan.TableDef{Cols: []*plan.ColDef{{Name: "__mo_rowid"}}},
		approxObjectNum: 0,
	}

	_, err := CollectAndCalculateStats(context.Background(), req, ex)
	require.ErrorIs(t, err, context.Canceled,
		"the zero-object fast path must not bypass executor lifecycle failure")
}

func TestCollectAndCalculateStatsAcceptsTableWideObservationWithoutObjects(t *testing.T) {
	tableDef := &plan2.TableDef{
		Name:    "events",
		Version: 7,
		Cols: []*plan2.ColDef{
			{Name: "url"},
			{Name: catalog.Row_ID},
		},
	}
	stats := plan2.NewStatsInfo()
	req := &updateStatsRequest{
		statsInfo:       stats,
		tableDef:        tableDef,
		approxObjectNum: 0,
	}

	ratio, err := CollectAndCalculateStats(context.Background(), req, nil)
	require.NoError(t, err)
	require.Equal(t, 1.0, ratio)
	rowCount := float64(42)
	require.NoError(t, applyStatsRefreshOptions(stats, tableDef, engine.StatsRefreshOptions{
		TableDefVersion: &tableDef.Version,
		TableRowCount:   &rowCount,
		ColumnNDVs:      map[string]float64{"url": 40},
	}))
	require.Zero(t, stats.AccurateObjectNumber)
	require.Equal(t, rowCount, stats.TableCnt)
	require.Equal(t, float64(40), stats.NdvMap["url"])
	require.True(t, plan2.StatsInfoUsable(stats))
}

func TestCollectAndCalculateStatsDoesNotApplyFailedObjectScan(t *testing.T) {
	ctx := context.Background()
	state := logtailreplay.NewPartitionState("", true, 42, false)
	state.UpdateDuration(types.TS{}, types.MaxTs())
	location := objectio.NewRandomLocation(1, 128)
	stats := objectio.NewObjectStats()
	objectio.SetObjectStatsLocation(stats, location)
	require.NoError(t, objectio.SetObjectStatsSize(stats, 1))
	require.NoError(t, state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(1, 0),
	}, false))
	require.Equal(t, 1, state.ApproxDataObjectsNum())

	fs, err := fileservice.NewMemoryFS(
		defines.SharedFileServiceName,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)
	published := plan2.NewStatsInfo()
	req := &updateStatsRequest{
		statsInfo: published,
		tableDef: &plan.TableDef{
			Name: "events",
			Cols: []*plan.ColDef{
				{Name: "event_id", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "__mo_rowid", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_Rowid)}},
			},
		},
		partitionState:  state,
		fs:              fs,
		ts:              types.MaxTs(),
		approxObjectNum: 1,
		samplingMode:    "full",
	}
	ex := newConcurrentExecutor(1)
	executorCtx, stopExecutor := context.WithCancel(context.Background())
	defer stopExecutor()
	ex.Run(executorCtx)

	_, err = CollectAndCalculateStats(ctx, req, ex)
	require.Error(t, err)
	require.Zero(t, published.TableCnt)
	require.Zero(t, published.AccurateObjectNumber)
	require.Empty(t, published.NdvMap)
	require.Empty(t, published.ShuffleRangeMap)
}

func TestShrinkBatchWithRowids(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.NewWithSchema(
		false,
		[]string{"rowid"},
		[]types.Type{types.T_Rowid.ToType()},
	)
	defer bat.Clean(mp)

	var rowid objectio.Rowid
	for i := 0; i < 10; i++ {
		rowid.SetRowOffset(uint32(i))
		err := vector.AppendFixed(bat.Vecs[0], rowid, false, mp)
		require.NoError(t, err)
	}
	bat.SetRowCount(10)

	shrinkBatchWithRowids(bat, []int64{1, 3, 5, 7})
	require.Equal(t, bat.RowCount(), 6)

	rowids := vector.MustFixedColWithTypeCheck[objectio.Rowid](bat.Vecs[0])
	offsets := make([]uint32, 0, bat.RowCount())
	for i := range rowids {
		offsets = append(offsets, rowids[i].GetRowOffset())
	}
	require.Equal(t, offsets, []uint32{0, 1, 2, 3, 4, 5})
}
