// Copyright 2022 Matrix Origin
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

package plan

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	index2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

var area = make([]byte, 0, 10000)

func TestRangeShuffle(t *testing.T) {
	require.Equal(t, GetRangeShuffleIndexUnsignedMinMax(0, 1000000, 299999, 10), uint64(2))
	require.Equal(t, GetRangeShuffleIndexUnsignedMinMax(0, 1000000, 888, 10), uint64(0))
	require.Equal(t, GetRangeShuffleIndexUnsignedMinMax(0, 1000000, 100000000, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, 299999, 10), uint64(2))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, 888, 10), uint64(0))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, 100000000, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, -2, 10), uint64(0))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, 999000, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, 99999, 10), uint64(0))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, 100000, 10), uint64(1))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, 100001, 10), uint64(1))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, 199999, 10), uint64(1))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, 200000, 10), uint64(2))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, 999999, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, 899999, 10), uint64(8))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, 900000, 10), uint64(9))
	require.Equal(t, GetRangeShuffleIndexSignedMinMax(0, 1000000, 1000000, 10), uint64(9))
}

func buildVarlenaFromByteSlice(bs []byte) *types.Varlena {
	var v types.Varlena
	vlen := len(bs)
	if vlen <= types.VarlenaInlineSize {
		// first clear varlena to 0
		p1 := v.UnsafePtr()
		*(*int64)(p1) = 0
		*(*int64)(unsafe.Add(p1, 8)) = 0
		*(*int64)(unsafe.Add(p1, 16)) = 0
		v[0] = byte(vlen)
		copy(v[1:1+vlen], bs)
		return &v
	} else {
		voff := len(area)
		area = append(area, bs...)
		v.SetOffsetLen(uint32(voff), uint32(vlen))
	}
	return &v
}

// The result will be 0 if a == b, -1 if a < b, and +1 if a > b.
func compareUint64(a, b uint64) int {
	if a == b {
		return 0
	} else if a < b {
		return -1
	} else {
		return 1
	}
}

func TestStringToUint64(t *testing.T) {
	s1 := []byte("abc")
	u1 := VarlenaToUint64Inline(buildVarlenaFromByteSlice(s1))
	require.Equal(t, u1, ByteSliceToUint64(s1))
	s2 := []byte("abcde")
	u2 := VarlenaToUint64Inline(buildVarlenaFromByteSlice(s2))
	require.Equal(t, u2, ByteSliceToUint64(s2))
	s3 := []byte("abcdeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	u3 := VarlenaToUint64(buildVarlenaFromByteSlice(s3), area)
	require.Equal(t, u3, ByteSliceToUint64(s3))
	s4 := []byte("a")
	u4 := VarlenaToUint64(buildVarlenaFromByteSlice(s4), area)
	require.Equal(t, u4, ByteSliceToUint64(s4))
	s5 := []byte("")
	u5 := VarlenaToUint64(buildVarlenaFromByteSlice(s5), area)
	require.Equal(t, u5, ByteSliceToUint64(s5))
	s6 := []byte("A")
	u6 := VarlenaToUint64(buildVarlenaFromByteSlice(s6), area)
	require.Equal(t, u6, ByteSliceToUint64(s6))
	s7 := []byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	u7 := VarlenaToUint64(buildVarlenaFromByteSlice(s7), area)
	require.Equal(t, u7, ByteSliceToUint64(s7))

	require.Equal(t, bytes.Compare(s1, s2), compareUint64(u1, u2))
	require.Equal(t, bytes.Compare(s1, s3), compareUint64(u1, u3))
	require.Equal(t, bytes.Compare(s1, s4), compareUint64(u1, u4))
	require.Equal(t, bytes.Compare(s1, s5), compareUint64(u1, u5))
	require.Equal(t, bytes.Compare(s1, s6), compareUint64(u1, u6))
	require.Equal(t, bytes.Compare(s1, s7), compareUint64(u1, u7))
	require.Equal(t, bytes.Compare(s2, s3), compareUint64(u2, u3))
	require.Equal(t, bytes.Compare(s2, s4), compareUint64(u2, u4))
	require.Equal(t, bytes.Compare(s2, s5), compareUint64(u2, u5))
	require.Equal(t, bytes.Compare(s2, s6), compareUint64(u2, u6))
	require.Equal(t, bytes.Compare(s2, s7), compareUint64(u2, u7))
	require.Equal(t, bytes.Compare(s3, s4), compareUint64(u3, u4))
	require.Equal(t, bytes.Compare(s3, s5), compareUint64(u3, u5))
	require.Equal(t, bytes.Compare(s3, s6), compareUint64(u3, u6))
	require.Equal(t, bytes.Compare(s3, s7), compareUint64(u3, u7))
	require.Equal(t, bytes.Compare(s4, s5), compareUint64(u4, u5))
	require.Equal(t, bytes.Compare(s4, s6), compareUint64(u4, u6))
	require.Equal(t, bytes.Compare(s4, s7), compareUint64(u4, u7))
	require.Equal(t, bytes.Compare(s5, s6), compareUint64(u5, u6))
	require.Equal(t, bytes.Compare(s5, s7), compareUint64(u5, u7))
	require.Equal(t, bytes.Compare(s6, s7), compareUint64(u6, u7))
}

type ShuffleRangeTestCase struct {
	min []float64
	max []float64
}

func TestShuffleRange(t *testing.T) {
	testcase := make([]ShuffleRangeTestCase, 0)
	testcase = append(testcase, ShuffleRangeTestCase{
		min: []float64{},
		max: []float64{},
	})
	testcase[0].min = append(testcase[0].min, 0)
	testcase[0].max = append(testcase[0].max, 10000)
	for i := 1; i < 100000; i++ {
		testcase[0].min = append(testcase[0].min, testcase[0].max[i-1]+float64(rand.Int()%10000))
		testcase[0].max = append(testcase[0].max, testcase[0].min[i]+float64(rand.Int()%10000+100))
	}
	testcase[0].min = append(testcase[0].min, testcase[0].max[99999]/2)
	testcase[0].max = append(testcase[0].max, testcase[0].min[100000]+10000)
	for i := 100001; i <= 200000; i++ {
		testcase[0].min = append(testcase[0].min, testcase[0].max[i-1]+float64(rand.Int()%10000))
		testcase[0].max = append(testcase[0].max, testcase[0].min[i]+float64(rand.Int()%10000+100))
	}

	testcase = append(testcase, ShuffleRangeTestCase{
		min: []float64{},
		max: []float64{},
	})
	for i := 0; i <= 100000; i++ {
		testcase[1].min = append(testcase[1].min, float64(rand.Int()))
		testcase[1].max = append(testcase[1].max, testcase[1].min[i]+float64(rand.Int()))
	}

	testcase = append(testcase, ShuffleRangeTestCase{
		min: []float64{},
		max: []float64{},
	})
	testcase[2].min = append(testcase[2].min, 0)
	testcase[2].max = append(testcase[2].max, 10000)
	for i := 1; i < 100000; i++ {
		testcase[2].min = append(testcase[2].min, testcase[2].max[i-1]-10)
		testcase[2].max = append(testcase[2].max, testcase[2].min[i]+10000)
	}

	leng := len(testcase)

	for i := 0; i < leng; i++ {
		shufflerange := NewShuffleRange(false)
		for j := 0; j < len(testcase[i].min); j++ {
			shufflerange.Update(testcase[i].min[j], testcase[i].max[j], 1000, 1)
		}
		shufflerange.Eval()
		shufflerange.ReleaseUnused()
	}
	shufflerange := NewShuffleRange(true)
	shufflerange.UpdateString([]byte("0000"), []byte("1000"), 1000, 1)
	shufflerange.UpdateString([]byte("2000"), []byte("3000"), 1000, 1)
	shufflerange.UpdateString([]byte("4000"), []byte("5000"), 1000, 1)
	shufflerange.UpdateString([]byte("6000"), []byte("7000"), 1000, 1)
	shufflerange.UpdateString([]byte("8000"), []byte("9000"), 1000, 1)
	shufflerange.Eval()
	shufflerange.ReleaseUnused()
}

func TestRangeShuffleSlice(t *testing.T) {
	require.Equal(t, GetRangeShuffleIndexSignedSlice([]int64{1, 3, 5, 7, 9}, 5), uint64(2))
	require.Equal(t, GetRangeShuffleIndexSignedSlice([]int64{1, 2, 3, 100}, 101), uint64(4))
	require.Equal(t, GetRangeShuffleIndexSignedSlice([]int64{-20, -1, 0, 1, 5}, -99), uint64(0))
	require.Equal(t, GetRangeShuffleIndexUnsignedSlice([]uint64{100, 200, 300}, 150), uint64(1))
	require.Equal(t, GetRangeShuffleIndexUnsignedSlice([]uint64{10001, 10002, 10003, 10004, 10005, 10006}, 10006), uint64(5))
	require.Equal(t, GetRangeShuffleIndexUnsignedSlice([]uint64{30, 50, 60, 90, 120}, 61), uint64(3))
}

func TestShouldSkipObjByShuffle(t *testing.T) {
	row := types.RandomRowid()
	stats := objectio.NewObjectStatsWithObjectID(row.BorrowObjectID(), false, false, true)
	objectio.SetObjectStatsRowCnt(stats, 100)
	tableDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "a",
			Names:       []string{"a"},
		},
	}
	node := &plan.Node{
		TableDef: tableDef,
		Stats:    DefaultStats(),
	}
	node.Stats.HashmapStats.Shuffle = true
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash
	rsp := &engine.RangesShuffleParam{
		Node:  node,
		CNCNT: 2,
		CNIDX: 0,
		Init:  false,
	}
	ShouldSkipObjByShuffle(rsp, stats)
	rsp.CNIDX = 1
	ShouldSkipObjByShuffle(rsp, stats)
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
	node.Stats.HashmapStats.ShuffleColMin = 0
	node.Stats.HashmapStats.ShuffleColMax = 10000
	ShouldSkipObjByShuffle(rsp, stats)
	zm := index2.NewZM(types.T_int32, 0)
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 0)
	index2.UpdateZM(zm, bs)
	binary.LittleEndian.PutUint32(bs, 10)
	index2.UpdateZM(zm, bs)
	objectio.SetObjectStatsSortKeyZoneMap(stats, zm)
	ShouldSkipObjByShuffle(rsp, stats)
}

func TestShouldSkipAppendableObjByShuffleKeepsDefaultLocalBehavior(t *testing.T) {
	row := types.RandomRowid()
	stats := objectio.NewObjectStatsWithObjectID(row.BorrowObjectID(), true, false, true)
	node := &plan.Node{TableDef: &plan.TableDef{}, Stats: DefaultStats()}

	for cnidx := int32(0); cnidx < 3; cnidx++ {
		rsp := &engine.RangesShuffleParam{Node: node, CNCNT: 3, CNIDX: cnidx}
		require.True(t, ShouldSkipObjByShuffle(rsp, stats))
		rsp.IsLocalCN = true
		require.False(t, ShouldSkipObjByShuffle(rsp, stats))
	}
}

func TestShouldSkipAppendableObjByShuffleCanAssignUniqueObjectOwner(t *testing.T) {
	row := types.RandomRowid()
	stats := objectio.NewObjectStatsWithObjectID(row.BorrowObjectID(), true, false, true)
	node := &plan.Node{TableDef: &plan.TableDef{}, Stats: DefaultStats()}
	owners := 0

	for cnidx := int32(0); cnidx < 3; cnidx++ {
		rsp := &engine.RangesShuffleParam{
			Node:              node,
			CNCNT:             3,
			CNIDX:             cnidx,
			ShuffleByObjectID: true,
		}
		if !ShouldSkipObjByShuffle(rsp, stats) {
			owners++
		}
	}
	require.Equal(t, 1, owners)
}

func productionShapedObjectID(rng *rand.Rand, sequence uint64) types.Objectid {
	var objectID types.Objectid

	// Object IDs use a UUIDv7 segment ID followed by a uint16 object number.
	// Model slowly changing UUIDv7 timestamps plus an object-number suffix;
	// the UUIDv7 random bits still vary for every generated ObjectID.
	timestamp := uint64(1_752_422_400_000) + sequence/4
	var encodedTimestamp [8]byte
	binary.BigEndian.PutUint64(encodedTimestamp[:], timestamp)
	copy(objectID[:6], encodedTimestamp[2:])
	_, _ = rng.Read(objectID[6:types.UuidSize])
	objectID[6] = objectID[6]&0x0f | 0x70 // UUID version 7
	objectID[8] = objectID[8]&0x3f | 0x80 // RFC 4122 variant
	binary.LittleEndian.PutUint16(objectID[types.UuidSize:], uint16(sequence%4))
	return objectID
}

func TestIVFObjectIDHashUsesCompleteObjectIDAndIsDeterministic(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	first := productionShapedObjectID(rng, 17)
	second := first
	second[2]++ // SimpleCharHashToRange does not sample this byte.

	const cnCount = uint64(1 << 32)
	require.Equal(t, xxhash.Sum64(first[:])%cnCount, IVFObjectIDHashToRange(first, cnCount))
	require.Equal(t, IVFObjectIDHashToRange(first, cnCount), IVFObjectIDHashToRange(first, cnCount))
	require.NotEqual(t, IVFObjectIDHashToRange(first, cnCount), IVFObjectIDHashToRange(second, cnCount))
}

func TestIVFObjectIDShuffleAssignsExactlyOneOwner(t *testing.T) {
	rng := rand.New(rand.NewSource(43))
	objectID := productionShapedObjectID(rng, 23)
	node := &plan.Node{TableDef: &plan.TableDef{}, Stats: DefaultStats()}

	owners := 0
	for cnidx := int32(0); cnidx < 8; cnidx++ {
		rsp := &engine.RangesShuffleParam{
			Node:              node,
			CNCNT:             8,
			CNIDX:             cnidx,
			ShuffleByObjectID: true,
		}
		stats := objectio.NewObjectStatsWithObjectID(&objectID, false, false, true)
		if !ShouldSkipObjByShuffle(rsp, stats) {
			owners++
		}
	}
	require.Equal(t, 1, owners)
}

func TestIVFObjectIDShuffleUsesSameOwnerForPersistedAndAppendable(t *testing.T) {
	rng := rand.New(rand.NewSource(44))
	objectID := productionShapedObjectID(rng, 29)
	persisted := objectio.NewObjectStatsWithObjectID(&objectID, false, false, true)
	appendable := objectio.NewObjectStatsWithObjectID(&objectID, true, false, true)
	node := &plan.Node{TableDef: &plan.TableDef{}, Stats: DefaultStats()}
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range

	for cnidx := int32(0); cnidx < 4; cnidx++ {
		rsp := &engine.RangesShuffleParam{
			Node:              node,
			CNCNT:             4,
			CNIDX:             cnidx,
			ShuffleByObjectID: true,
		}
		require.Equal(t,
			ShouldSkipObjByShuffle(rsp, persisted),
			ShouldSkipObjByShuffle(rsp, appendable),
		)
	}
}

func TestIVFObjectIDShuffleDoesNotChangeOrdinaryOwnership(t *testing.T) {
	objectID := types.Objectid{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
	stats := objectio.NewObjectStatsWithObjectID(&objectID, false, false, true)
	node := &plan.Node{TableDef: &plan.TableDef{}, Stats: DefaultStats()}
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash

	const cnCount = int32(4)
	wantOwner := int32(SimpleCharHashToRange(objectID[:], uint64(cnCount)))
	for cnidx := int32(0); cnidx < cnCount; cnidx++ {
		rsp := &engine.RangesShuffleParam{Node: node, CNCNT: cnCount, CNIDX: cnidx}
		require.Equal(t, cnidx != wantOwner, ShouldSkipObjByShuffle(rsp, stats))
	}
}

func TestIVFObjectIDHashUUIDv7Distribution(t *testing.T) {
	const objectCount = 16_384
	rng := rand.New(rand.NewSource(45))
	objectIDs := make([]types.Objectid, objectCount)
	for i := range objectIDs {
		objectIDs[i] = productionShapedObjectID(rng, uint64(i))
	}

	for _, cnCount := range []uint64{2, 3, 4, 8} {
		t.Run(fmt.Sprintf("%d-cns", cnCount), func(t *testing.T) {
			counts := make([]int, cnCount)
			for _, objectID := range objectIDs {
				counts[IVFObjectIDHashToRange(objectID, cnCount)]++
			}

			expected := float64(objectCount) / float64(cnCount)
			for cnidx, count := range counts {
				deviation := float64(count)/expected - 1
				require.InDeltaf(t, 0, deviation, 0.0625,
					"CN %d owns %d objects; counts=%v", cnidx, count, counts)
			}
		})
	}
}

func TestDetermineShuffleForDedupJoin(t *testing.T) {
	cases := []struct {
		name        string
		dedupCtx    *plan.DedupJoinCtx
		wantShuffle bool
	}{
		{
			name:        "plain_dedup_join_large_build_side_can_shuffle",
			dedupCtx:    &plan.DedupJoinCtx{},
			wantShuffle: true,
		},
		{
			name: "old_col_list_disables_shuffle",
			dedupCtx: &plan.DedupJoinCtx{
				OldColList: []plan.ColRef{{RelPos: 1, ColPos: 0}},
			},
		},
		{
			name: "old_col_capture_list_disables_shuffle",
			dedupCtx: &plan.DedupJoinCtx{
				OldColCaptureList: []plan.OldColCapture{
					{
						BuildPlaceholder: plan.ColRef{RelPos: 1, ColPos: 0},
						ProbeSource:      plan.ColRef{RelPos: 2, ColPos: 0},
					},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			builder := &QueryBuilder{
				qry: &plan.Query{
					Nodes: []*plan.Node{
						{Stats: DefaultStats()},
						{Stats: &plan.Stats{Outcnt: 320001, HashmapStats: &plan.HashMapStats{}}},
					},
				},
			}
			node := &plan.Node{
				NodeType:          plan.Node_JOIN,
				JoinType:          plan.Node_DEDUP,
				Children:          []int32{0, 1},
				OnDuplicateAction: plan.Node_FAIL,
				DedupJoinCtx:      c.dedupCtx,
				Stats:             DefaultStats(),
			}

			determineShuffleForJoin(node, builder)

			require.Equal(t, c.wantShuffle, node.Stats.HashmapStats.Shuffle)
			if c.wantShuffle {
				require.Equal(t, int32(0), node.Stats.HashmapStats.ShuffleColIdx)
				require.Equal(t, plan.ShuffleType_Hash, node.Stats.HashmapStats.ShuffleType)
			} else {
				require.Equal(t, int32(-1), node.Stats.HashmapStats.ShuffleColIdx)
			}
		})
	}
}

func TestGetRangeShuffleIndexForZM(t *testing.T) {
	zm := index2.NewZM(types.T_datetime, 0)
	defer func() {
		r := recover()
		fmt.Println("panic recover", r)
	}()
	GetRangeShuffleIndexForZM(0, 1000, zm, 4)
}

func TestShuffleByZonemap(t *testing.T) {
	node := &plan.Node{
		Stats: DefaultStats(),
	}
	node.Stats.HashmapStats.Shuffle = true
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
	node.Stats.HashmapStats.ShuffleColMin = 0
	node.Stats.HashmapStats.ShuffleColMax = 10000
	node.Stats.HashmapStats.Ranges = []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	zm := index2.NewZM(types.T_uint32, 0)
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, 0)
	index2.UpdateZM(zm, bs)
	binary.LittleEndian.PutUint32(bs, 10)
	index2.UpdateZM(zm, bs)

	rsp := &engine.RangesShuffleParam{
		Node:  node,
		CNCNT: 2,
		CNIDX: 0,
		Init:  false,
	}
	shuffleByZonemap(rsp, zm, 2)
}

func TestShuffleByValueExtractedFromZonemap(t *testing.T) {
	node := &plan.Node{
		Stats: DefaultStats(),
	}
	node.Stats.HashmapStats.Shuffle = true
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
	node.Stats.HashmapStats.ShuffleColMin = 0
	node.Stats.HashmapStats.ShuffleColMax = 4000000000
	node.Stats.HashmapStats.ShuffleColIdx = int32(types.T_int64)

	zm := index2.NewZM(types.T_varchar, 0)
	bs := []byte{59, 24, 223, 254, 115, 192, 58, 21, 1}
	index2.UpdateZM(zm, bs)
	bs = []byte{59, 24, 224, 7, 119, 160, 58, 21, 5}
	index2.UpdateZM(zm, bs)

	rsp := &engine.RangesShuffleParam{
		Node:  node,
		CNCNT: 3,
		CNIDX: 0,
		Init:  false,
	}
	idx := shuffleByValueExtractedFromZonemap(rsp, zm, 3)
	require.Equal(t, idx, uint64(2))

	node = &plan.Node{
		Stats: DefaultStats(),
	}
	node.Stats.HashmapStats.Shuffle = true
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
	node.Stats.HashmapStats.ShuffleColMin = 0
	node.Stats.HashmapStats.ShuffleColMax = 4000000000
	node.Stats.HashmapStats.ShuffleColIdx = int32(types.T_uint64)

	zm = index2.NewZM(types.T_varchar, 0)
	packer := types.NewPacker()
	packer.EncodeUint64(1500000000)
	packer.EncodeUint64(1)
	index2.UpdateZM(zm, packer.Bytes())
	packer = types.NewPacker()
	packer.EncodeUint64(1600000000)
	packer.EncodeUint64(1)
	index2.UpdateZM(zm, packer.Bytes())

	rsp = &engine.RangesShuffleParam{
		Node:  node,
		CNCNT: 4,
		CNIDX: 0,
		Init:  false,
	}
	idx = shuffleByValueExtractedFromZonemap(rsp, zm, 4)
	require.Equal(t, idx, uint64(1))
}
