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
	"io"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
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

	legacyBatch := batch.NewWithSize(3)
	legacyBatch.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	legacyBatch.Vecs[1] = vector.NewVec(varcharType)
	legacyBatch.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(legacyBatch.Vecs[0], makeRowid(4), false, mp))
	require.NoError(t, vector.AppendBytes(legacyBatch.Vecs[1], []byte("legacy"), false, mp))
	require.NoError(t, vector.AppendFixed(
		legacyBatch.Vecs[2], types.BuildTS(10, 0), false, mp,
	))
	legacyBatch.SetRowCount(1)
	legacyName := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	legacyWriter, err := ioutil.NewBlockWriter(fs, legacyName.String())
	require.NoError(t, err)
	_, err = legacyWriter.WriteBatch(legacyBatch)
	require.NoError(t, err)
	legacyBlocks, legacyExtent, err := legacyWriter.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, legacyBlocks, 1)
	require.Equal(t, uint16(3), legacyBlocks[0].GetMetaColumnCount())
	require.Equal(t, uint16(2), legacyBlocks[0].GetMaxSeqnum())
	require.Equal(t, uint8(types.T_TS), legacyBlocks[0].ColumnMeta(2).DataType())
	legacyStats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsObjectName(legacyStats, legacyName))
	require.NoError(t, objectio.SetObjectStatsExtent(legacyStats, legacyExtent))
	require.NoError(t, objectio.SetObjectStatsRowCnt(legacyStats, uint32(legacyBatch.RowCount())))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(legacyStats, uint32(len(legacyBlocks))))
	require.NoError(t, objectio.SetObjectStatsSize(legacyStats, legacyExtent.End()))
	legacyBatch.Clean(mp)

	legacyState := logtailreplay.NewPartitionState("", true, 0, false)
	require.NoError(t, legacyState.HandleObjectEntry(ctx, fs, objectio.ObjectEntry{
		ObjectStats: *legacyStats,
		CreateTime:  types.BuildTS(40, 0),
	}, true))
	legacyKey := vector.NewVec(varcharType)
	require.NoError(t, vector.AppendBytes(legacyKey, []byte("legacy"), false, mp))
	changed, reason, err = tombstonePKExistsInRange(
		ctx,
		legacyState,
		types.BuildTS(20, 0),
		types.BuildTS(30, 0),
		legacyKey,
		varcharType,
		fs,
		mp,
	)
	require.NoError(t, err)
	require.False(t, changed, "the legacy physical commit timestamp is before the range")
	require.Empty(t, reason)

	changed, reason, err = tombstonePKExistsInRange(
		ctx,
		legacyState,
		types.BuildTS(5, 0),
		types.BuildTS(15, 0),
		legacyKey,
		varcharType,
		fs,
		mp,
	)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, "tombstone_commit_ts_hit", reason)

	key.Free(mp)
	missing.Free(mp)
	legacyKey.Free(mp)
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
}

func TestConcurrentExecutor_Run(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ex := newConcurrentExecutor(3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ex.Run(ctx)
	require.Equal(t, 3, ex.GetConcurrency())

	var wg sync.WaitGroup
	wg.Add(1)
	ex.AppendTask(func() error {
		defer wg.Done()
		return nil
	})

	wg.Add(1)
	ex.AppendTask(func() error {
		defer wg.Done()
		return context.Canceled
	})

	wg.Add(1)
	ex.AppendTask(func() error {
		defer wg.Done()
		return io.EOF
	})
	wg.Wait()
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
