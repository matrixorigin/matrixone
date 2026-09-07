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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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

func TestStableCharHashToRangeUsesCompleteKey(t *testing.T) {
	first := []byte("https://example.invalid/events/00000000.json")
	second := append([]byte(nil), first...)
	second[31] = '1'

	const bucketCount = uint64(1 << 32)
	require.Equal(t, hashtable.StableBytesHash(first)%bucketCount, StableCharHashToRange(first, bucketCount))
	require.Equal(t, StableCharHashToRange(first, bucketCount), StableCharHashToRange(first, bucketCount))
	require.NotEqual(t, StableCharHashToRange(first, bucketCount), StableCharHashToRange(second, bucketCount))
	require.Equal(t, uint64(0), StableCharHashToRange(nil, 16))
	require.Equal(t, hashtable.StableBytesHash([]byte{'x'})%16, StableCharHashToRange([]byte{'x'}, 16))
	require.Equal(t, StableCharHashToRange([]byte("duplicate"), 16),
		StableCharHashToRange([]byte("duplicate"), 16))
}

func TestSimpleCharHashToRangePreservesLegacyContract(t *testing.T) {
	first := []byte("https://example.invalid/events/00000000.json")
	second := append([]byte(nil), first...)
	second[31] = '1'

	require.Equal(t, SimpleCharHashToRange(first, 16), SimpleCharHashToRange(second, 16),
		"the rollout fallback must remain byte-for-byte compatible with old CNs")
	require.Equal(t, uint64(0), SimpleCharHashToRange(nil, 16))
	require.Equal(t, uint64('x')%16, SimpleCharHashToRange([]byte{'x'}, 16))
}

func TestStableCharHashToRangeDistribution(t *testing.T) {
	const keyCount = 16_384
	keySets := map[string]func(int) []byte{
		"pseudo-random": func(i int) []byte {
			key := make([]byte, 32)
			x := uint64(i) + 0x9e3779b97f4a7c15
			for offset := 0; offset < len(key); offset += 8 {
				x ^= x >> 12
				x ^= x << 25
				x ^= x >> 27
				binary.LittleEndian.PutUint64(key[offset:], x*0x2545f4914f6cdd1d)
			}
			return key
		},
		"common-prefix-sequential-suffix": func(i int) []byte {
			return []byte(fmt.Sprintf("https://example.invalid/events/partition/static/%08d.json", i))
		},
		"common-suffix": func(i int) []byte {
			return []byte(fmt.Sprintf("%08d/static/common/suffix.json", i))
		},
		"utf8": func(i int) []byte {
			return []byte(fmt.Sprintf("租户/事件/固定前缀/%08d/完成", i))
		},
		"binary": func(i int) []byte {
			key := make([]byte, 32)
			binary.BigEndian.PutUint64(key[20:28], uint64(i))
			key[31] = 0xff
			return key
		},
		"short": func(i int) []byte {
			key := make([]byte, 4)
			binary.LittleEndian.PutUint32(key, uint32(i))
			return key
		},
	}

	for name, makeKey := range keySets {
		t.Run(name, func(t *testing.T) {
			for _, bucketCount := range []uint64{1, 2, 8, 16} {
				counts := make([]int, bucketCount)
				for i := range keyCount {
					counts[StableCharHashToRange(makeKey(i), bucketCount)]++
				}

				expected := float64(keyCount) / float64(bucketCount)
				for bucket, count := range counts {
					require.InDeltaf(t, expected, float64(count), expected*0.15,
						"bucket %d owns %d keys; counts=%v", bucket, count, counts)
				}
			}
		})
	}
}

var simpleCharHashBenchmarkSink uint64

func BenchmarkStableCharHashToRange(b *testing.B) {
	for _, keyLength := range []int{8, 32, 64, 1024, 64 << 10, 1 << 20} {
		key := make([]byte, keyLength)
		for i := range key {
			key[i] = byte(i*131 + 17)
		}
		b.Run(fmt.Sprintf("%dB", keyLength), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(keyLength))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				simpleCharHashBenchmarkSink = StableCharHashToRange(key, 16)
			}
		})
	}
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
		name              string
		dedupCtx          *plan.DedupJoinCtx
		onDuplicateAction plan.Node_OnDuplicateAction
		keyType           types.T
		wantShuffle       bool
	}{
		{
			name:        "plain_dedup_join_large_build_side_can_shuffle",
			dedupCtx:    &plan.DedupJoinCtx{},
			keyType:     types.T_int64,
			wantShuffle: true,
		},
		{
			name:     "float32_key_stays_unshuffled",
			dedupCtx: &plan.DedupJoinCtx{},
			keyType:  types.T_float32,
		},
		{
			name:     "float64_key_stays_unshuffled",
			dedupCtx: &plan.DedupJoinCtx{},
			keyType:  types.T_float64,
		},
		{
			name: "old_col_list_disables_shuffle",
			dedupCtx: &plan.DedupJoinCtx{
				OldColList: []plan.ColRef{{RelPos: 1, ColPos: 0}},
			},
			keyType: types.T_int64,
		},
		{
			name: "ignore_old_col_list_disables_shuffle",
			dedupCtx: &plan.DedupJoinCtx{
				OldColList: []plan.ColRef{{RelPos: 1, ColPos: 0}},
			},
			onDuplicateAction: plan.Node_IGNORE,
			keyType:           types.T_int64,
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
			keyType: types.T_int64,
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
				OnList:            []*plan.Expr{makeRightDedupEquality(c.keyType)},
				OnDuplicateAction: c.onDuplicateAction,
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

func TestDetermineShuffleForJoinNDVGuard(t *testing.T) {
	left := &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		BindingTags: []int32{1},
		Stats:       &plan.Stats{Outcnt: 1000, HashmapStats: &plan.HashMapStats{}},
	}
	right := &plan.Node{
		NodeType:    plan.Node_SINK_SCAN,
		BindingTags: []int32{2},
		Stats:       &plan.Stats{Outcnt: 10_000_000, HashmapStats: &plan.HashMapStats{}},
	}
	leftKey := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}},
	}
	rightKey := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 2, ColPos: 0}},
	}
	cond, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*plan.Expr{leftKey, rightKey})
	require.NoError(t, err)
	builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{left, right}}}
	newJoin := func(ndv float64) *plan.Node {
		joinCond := DeepCopyExpr(cond)
		joinCond.Ndv = ndv
		return &plan.Node{
			NodeType: plan.Node_JOIN,
			JoinType: plan.Node_INNER,
			Children: []int32{0, 1},
			OnList:   []*plan.Expr{joinCond},
			Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
				HashmapSize: 10_000_000,
			}},
		}
	}

	unknownNDVJoin := newJoin(-1)
	determineShuffleForJoin(unknownNDVJoin, builder)
	require.True(t, unknownNDVJoin.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(0), unknownNDVJoin.Stats.HashmapStats.ShuffleColIdx)
	require.Equal(t, plan.ShuffleType_Hash, unknownNDVJoin.Stats.HashmapStats.ShuffleType)

	lowNDVJoin := newJoin(10)
	determineShuffleForJoin(lowNDVJoin, builder)
	require.False(t, lowNDVJoin.Stats.HashmapStats.Shuffle)
}

func TestDetermineShuffleForGroupByCanUseDependentHighNDVColumn(t *testing.T) {
	child := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		Stats: &plan.Stats{
			Outcnt:       3_000_000,
			HashmapStats: &plan.HashMapStats{},
		},
	}
	groupBy := make([]*plan.Expr, 3)
	for i, ndv := range []float64{1_000, 2_000, 100_000} {
		groupBy[i] = &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Ndv:  ndv,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: int32(i)}},
		}
	}
	agg := &plan.Node{
		NodeType:       plan.Node_AGG,
		Children:       []int32{0},
		GroupBy:        groupBy,
		GroupByHashKey: []int32{0, 1},
		Stats: &plan.Stats{
			Outcnt:      3_000_000,
			Selectivity: 1,
			HashmapStats: &plan.HashMapStats{
				HashmapSize: 3_000_000,
			},
		},
	}
	builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{child, agg}}}

	determineShuffleForGroupBy(agg, builder)

	require.True(t, agg.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(2), agg.Stats.HashmapStats.ShuffleColIdx,
		"a logical group column determined by the physical key remains a safe distribution key")
}

func TestDetermineShuffleForGroupByDoesNotForceUnknownNDV(t *testing.T) {
	newAggregate := func(inputRows float64) (*plan.Node, *QueryBuilder) {
		child := &plan.Node{
			NodeType: plan.Node_TABLE_SCAN,
			Stats: &plan.Stats{
				Outcnt:       inputRows,
				Selectivity:  1,
				HashmapStats: &plan.HashMapStats{},
			},
		}
		agg := &plan.Node{
			NodeType: plan.Node_AGG,
			Children: []int32{0},
			GroupBy: []*plan.Expr{{
				Typ:  plan.Type{Id: int32(types.T_varchar)},
				Ndv:  -1,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0, Name: "url"}},
			}},
			Stats: &plan.Stats{
				Outcnt:      1,
				Selectivity: 1,
				HashmapStats: &plan.HashMapStats{
					HashmapSize: 1,
				},
			},
		}
		return agg, &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{child, agg}}}
	}

	for _, inputRows := range []float64{threshHoldForHashShuffle - 1, threshHoldForHashShuffle, 10_000_000} {
		agg, builder := newAggregate(inputRows)
		determineShuffleForGroupBy(agg, builder)
		require.Falsef(t, agg.Stats.HashmapStats.Shuffle,
			"unknown NDV must not route all raw rows by an assumed high cardinality at input=%v", inputRows)
	}
}

func TestDetermineShuffleForGroupByAccountsForCountDistinctState(t *testing.T) {
	statsCache := NewStatsCache()
	stats := NewStatsInfo()
	stats.TableCnt = 1_000_000
	stats.NdvMap["user_id"] = 1_000_000
	statsCache.Set(1, stats)
	ctx := &statsCacheCompilerContext{
		MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
		statsCache:          statsCache,
	}
	child := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		Stats: &plan.Stats{
			Outcnt:       1_000_000,
			Selectivity:  1,
			HashmapStats: &plan.HashMapStats{},
		},
	}
	groupBy := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int32)},
		Ndv:  100,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}},
	}
	countID := function.EncodeOverloadID(function.COUNT, 0)
	sumID := function.EncodeOverloadID(function.SUM, 0)
	newAggregate := func(distinctNDV float64) *plan.Node {
		stats.NdvMap["user_id"] = distinctNDV
		arg := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 1}},
		}
		return &plan.Node{
			NodeType: plan.Node_AGG,
			Children: []int32{0},
			GroupBy:  []*plan.Expr{DeepCopyExpr(groupBy)},
			// Keep a regular aggregate beside COUNT(DISTINCT), matching the mixed
			// aggregate shape that cannot use optimizeDistinctAgg's single-aggregate
			// rewrite.
			AggList: []*plan.Expr{
				{
					Expr: &plan.Expr_F{F: &plan.Function{
						Func: &plan.ObjectRef{Obj: countID},
						Args: []*plan.Expr{DeepCopyExpr(arg)},
					}},
				},
				{
					Expr: &plan.Expr_F{F: &plan.Function{
						Func: &plan.ObjectRef{Obj: int64(uint64(countID) | function.Distinct)},
						Args: []*plan.Expr{arg},
					}},
				},
			},
			Stats: &plan.Stats{
				Outcnt:      100,
				Selectivity: 1,
				HashmapStats: &plan.HashMapStats{
					HashmapSize: 100,
				},
			},
		}
	}
	builder := &QueryBuilder{
		qry:     &plan.Query{Nodes: []*plan.Node{child}},
		compCtx: ctx,
		tag2Table: map[int32]*plan.TableDef{1: {
			TblId: 1,
			Cols: []*plan.ColDef{
				{Name: "region_id", Typ: plan.Type{Id: int32(types.T_int32)}},
				{Name: "user_id", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		}},
	}

	large := newAggregate(1_000_000)
	determineShuffleForGroupBy(large, builder)
	require.True(t, large.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(0), large.Stats.HashmapStats.ShuffleColIdx)

	small := newAggregate(threshHoldForShuffleGroup)
	determineShuffleForGroupBy(small, builder)
	require.False(t, small.Stats.HashmapStats.Shuffle)

	lowGroupNDV := newAggregate(1_000_000)
	lowGroupNDV.GroupBy[0].Ndv = shuffleDistinctGroupMinNDV - 1
	determineShuffleForGroupBy(lowGroupNDV, builder)
	require.False(t, lowGroupNDV.Stats.HashmapStats.Shuffle,
		"too few groups would serialize the high-cardinality distinct state")

	emptyGroupingSet := newAggregate(1_000_000)
	emptyGroupingSet.GroupingFlag = []bool{false}
	determineShuffleForGroupBy(emptyGroupingSet, builder)
	require.False(t, emptyGroupingSet.Stats.HashmapStats.Shuffle,
		"an empty grouping set has no raw child key safe for pre-group shuffle")

	activeGroupingKey := newAggregate(1_000_000)
	activeGroupingKey.GroupBy = append(activeGroupingKey.GroupBy, &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int32)},
		Ndv:  1_000_000,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}},
	})
	activeGroupingKey.GroupingFlag = []bool{false, true}
	determineShuffleForGroupBy(activeGroupingKey, builder)
	require.True(t, activeGroupingKey.Stats.HashmapStats.Shuffle,
		"a grouping-set branch may still shuffle on an active key")
	require.Equal(t, int32(1), activeGroupingKey.Stats.HashmapStats.ShuffleColIdx,
		"inactive high-NDV keys must not become the distribution key")

	child.Stats.Outcnt = 100_000_000
	lowStateRatio := newAggregate(1_000_000)
	determineShuffleForGroupBy(lowStateRatio, builder)
	require.False(t, lowStateRatio.Stats.HashmapStats.Shuffle,
		"do not redistribute a large input for a comparatively small exact state")

	stats.TableCnt = 100_000_000
	child.Stats.Outcnt = 1_000_000
	child.Stats.Selectivity = 0.01
	selective := newAggregate(1_000_000)
	selective.GroupBy[0].Ndv = 100_000
	determineShuffleForGroupBy(selective, builder)
	require.False(t, selective.Stats.HashmapStats.Shuffle,
		"table-level NDV must not claim that all distinct values survive selection")
	selectiveManyOwners := newAggregate(100_000_000)
	selectiveManyOwners.GroupBy[0].Ndv = 100_000
	determineShuffleForGroupBy(selectiveManyOwners, builder)
	require.True(t, selectiveManyOwners.Stats.HashmapStats.Shuffle,
		"selection may retain shuffle when enough estimated owners and exact state remain")

	child.Stats.Selectivity = 1
	singleStage := newAggregate(100_000_000)
	singleStage.Stats.HashmapStats.HashmapSize = 3_000_000
	singleStage.Stats.HashmapStats.Shuffle = true
	arg := DeepCopyExpr(singleStage.AggList[1].GetF().Args[0])
	singleStage.AggList = append(singleStage.AggList, &plan.Expr{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: int64(uint64(sumID) | function.Distinct)},
			Args: []*plan.Expr{arg},
		}},
	})
	determineShuffleForGroupBy(singleStage, builder)
	require.False(t, singleStage.Stats.HashmapStats.Shuffle,
		"an aggregate forced to one CN must clear even stale shuffle state")
}

func TestEstimateNDVAfterSelectionRejectsInvalidStats(t *testing.T) {
	require.Equal(t, float64(-1), estimateNDVAfterSelection(100, nil))
	require.Equal(t, float64(-1), estimateNDVAfterSelection(math.NaN(), &plan.Stats{Outcnt: 10}))
	require.Equal(t, float64(-1), estimateNDVAfterSelection(100, &plan.Stats{Outcnt: math.Inf(1)}))
	require.Equal(t, float64(10), estimateNDVAfterSelection(100, &plan.Stats{Outcnt: 10}))
	require.InDelta(t, 100*math.Pow(0.01, 0.8),
		estimateNDVAfterSelection(100, &plan.Stats{Outcnt: 100, Selectivity: 0.01}), 1e-9)
}

func TestDetermineShuffleForJoinFindsEligibleConditionAcrossPredicateOrder(t *testing.T) {
	joinTypes := []plan.Node_JoinType{
		plan.Node_INNER,
		plan.Node_ANTI,
		plan.Node_SEMI,
		plan.Node_LEFT,
		plan.Node_RIGHT,
		plan.Node_OUTER,
		plan.Node_MARK,
	}

	for _, joinType := range joinTypes {
		for _, afterRemap := range []bool{false, true} {
			for _, highFirst := range []bool{false, true} {
				name := fmt.Sprintf("%s/after-remap=%t/high-first=%t", joinType, afterRemap, highFirst)
				t.Run(name, func(t *testing.T) {
					leftRel, rightRel := int32(10), int32(20)
					if afterRemap {
						leftRel, rightRel = 0, 1
					}
					low := makeShuffleJoinEquality(t, types.T_int64, 64, leftRel, rightRel, 0)
					high := makeShuffleJoinEquality(t, types.T_int64, 100_000, leftRel, rightRel, 1)
					conditions := []*plan.Expr{low, high}
					wantIdx := int32(1)
					if highFirst {
						conditions = []*plan.Expr{high, low}
						wantIdx = 0
					}

					left := makeShuffleJoinTestChild(10, 10_000_000)
					right := makeShuffleJoinTestChild(20, 3_000_000)
					node := &plan.Node{
						NodeType: plan.Node_JOIN,
						JoinType: joinType,
						Children: []int32{0, 1},
						OnList:   conditions,
						Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
							HashmapSize: 3_000_000,
						}},
					}

					determineShuffleForJoinWithColRefMode(
						node,
						&QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{left, right}}},
						afterRemap,
					)

					require.True(t, node.Stats.HashmapStats.Shuffle)
					require.Equal(t, wantIdx, node.Stats.HashmapStats.ShuffleColIdx)
					require.Equal(t, float64(100_000), node.OnList[wantIdx].Ndv)
				})
			}
		}
	}
}

func TestDetermineShuffleForJoinCandidateFallbacks(t *testing.T) {
	sameSide := makeShuffleJoinEquality(t, types.T_int64, 200_000, 10, 20, 0)
	sameSide.GetF().Args[1].GetCol().RelPos = 10

	tests := []struct {
		name        string
		candidates  []*plan.Expr
		wantShuffle bool
		wantNDV     float64
	}{
		{
			name: "unsupported first does not hide supported key",
			candidates: []*plan.Expr{
				makeShuffleJoinEquality(t, types.T_float32, 200_000, 10, 20, 0),
				makeShuffleJoinEquality(t, types.T_int64, 100_000, 10, 20, 1),
			},
			wantShuffle: true,
			wantNDV:     100_000,
		},
		{
			name: "same-side equality does not hide join key",
			candidates: []*plan.Expr{
				sameSide,
				makeShuffleJoinEquality(t, types.T_int64, 100_000, 10, 20, 1),
			},
			wantShuffle: true,
			wantNDV:     100_000,
		},
		{
			name: "supported expression key remains eligible",
			candidates: []*plan.Expr{
				makeShuffleJoinEquality(t, types.T_int64, 64, 10, 20, 0),
				makeShuffleJoinSerialEquality(t, 100_000, 10, 20, 1),
			},
			wantShuffle: true,
			wantNDV:     100_000,
		},
		{
			name: "unknown first preserves existing eligible choice",
			candidates: []*plan.Expr{
				makeShuffleJoinEquality(t, types.T_int64, -1, 10, 20, 0),
				makeShuffleJoinEquality(t, types.T_int64, 100_000, 10, 20, 1),
			},
			wantShuffle: true,
			wantNDV:     -1,
		},
		{
			name: "unknown remains eligible when known candidates are low",
			candidates: []*plan.Expr{
				makeShuffleJoinEquality(t, types.T_int64, 64, 10, 20, 0),
				makeShuffleJoinEquality(t, types.T_int64, -1, 10, 20, 1),
			},
			wantShuffle: true,
			wantNDV:     -1,
		},
		{
			name: "NDV threshold is inclusive",
			candidates: []*plan.Expr{
				makeShuffleJoinEquality(t, types.T_int64, ShuffleThreshHoldOfNDV, 10, 20, 0),
			},
			wantShuffle: true,
			wantNDV:     ShuffleThreshHoldOfNDV,
		},
		{
			name: "candidate immediately below NDV threshold does not hide eligible key",
			candidates: []*plan.Expr{
				makeShuffleJoinEquality(t, types.T_int64, ShuffleThreshHoldOfNDV-1, 10, 20, 0),
				makeShuffleJoinEquality(t, types.T_int64, ShuffleThreshHoldOfNDV, 10, 20, 1),
			},
			wantShuffle: true,
			wantNDV:     ShuffleThreshHoldOfNDV,
		},
		{
			name: "all known candidates are low cardinality",
			candidates: []*plan.Expr{
				makeShuffleJoinEquality(t, types.T_int64, 64, 10, 20, 0),
				makeShuffleJoinEquality(t, types.T_int64, 1_000, 10, 20, 1),
			},
			wantNDV: 64,
		},
		{
			name: "all candidates use unsupported types",
			candidates: []*plan.Expr{
				makeShuffleJoinEquality(t, types.T_float32, 100_000, 10, 20, 0),
				makeShuffleJoinEquality(t, types.T_float64, 200_000, 10, 20, 1),
			},
			wantNDV: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &plan.Node{
				NodeType: plan.Node_JOIN,
				JoinType: plan.Node_INNER,
				Children: []int32{0, 1},
				OnList:   tt.candidates,
				Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
					HashmapSize: 3_000_000,
				}},
			}
			builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{
				makeShuffleJoinTestChild(10, 10_000_000),
				makeShuffleJoinTestChild(20, 3_000_000),
			}}}

			determineShuffleForJoin(node, builder)

			require.Equal(t, tt.wantShuffle, node.Stats.HashmapStats.Shuffle)
			if tt.wantNDV < 0 && !tt.wantShuffle {
				require.Equal(t, int32(-1), node.Stats.HashmapStats.ShuffleColIdx)
				return
			}
			require.NotEqual(t, int32(-1), node.Stats.HashmapStats.ShuffleColIdx)
			require.Equal(t, tt.wantNDV, node.OnList[node.Stats.HashmapStats.ShuffleColIdx].Ndv)
		})
	}
}

func TestDetermineShuffleForJoinPreservesReusableFirstCondition(t *testing.T) {
	left := makeShuffleJoinTestChild(10, 10_000_000)
	left.NodeType = plan.Node_AGG
	left.GroupBy = []*plan.Expr{
		{
			Typ:  plan.Type{Id: int32(types.T_int64), NotNullable: true},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 100, ColPos: 0}},
		},
		{},
	}
	left.Stats.HashmapStats = &plan.HashMapStats{
		Shuffle:       true,
		ShuffleColIdx: 0,
		ShuffleType:   plan.ShuffleType_Range,
		HashmapSize:   3_000_000,
		ShuffleColMin: 0,
		ShuffleColMax: 1_000_000,
	}
	right := makeShuffleJoinTestChild(20, 3_000_000)
	node := &plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
		Children: []int32{0, 1},
		OnList: []*plan.Expr{
			makeShuffleJoinEquality(t, types.T_int64, 64, 10, 20, 0),
			makeShuffleJoinEquality(t, types.T_int64, 100_000, 10, 20, 1),
		},
		Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
			HashmapSize: 3_000_000,
		}},
	}
	builder := &QueryBuilder{
		qry:       &plan.Query{Nodes: []*plan.Node{left, right}},
		tag2Table: map[int32]*plan.TableDef{100: {}},
	}

	determineShuffleForJoin(node, builder)

	require.True(t, node.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(0), node.Stats.HashmapStats.ShuffleColIdx)
	require.Equal(t, plan.ShuffleMethod_Reuse, node.Stats.HashmapStats.ShuffleMethod)
}

func TestDetermineShuffleForJoinReusesLeftKeyThroughJoinChain(t *testing.T) {
	makeChildJoin := func(joinType plan.Node_JoinType) (*QueryBuilder, *plan.Node) {
		left := makeShuffleJoinTestChild(10, 10_000_000)
		build := makeShuffleJoinTestChild(20, 3_000_000)
		childJoin := &plan.Node{
			NodeType: plan.Node_JOIN,
			JoinType: joinType,
			Children: []int32{0, 1},
			OnList: []*plan.Expr{
				makeShuffleJoinEquality(t, types.T_int64, 64, 10, 20, 0),
			},
			Stats: &plan.Stats{
				Outcnt: 10_000_000,
				HashmapStats: &plan.HashMapStats{
					Shuffle:       true,
					ShuffleColIdx: 0,
					ShuffleType:   plan.ShuffleType_Range,
					ShuffleMethod: plan.ShuffleMethod_Reuse,
					ShuffleColMin: 10,
					ShuffleColMax: 1_000_000,
				},
			},
		}
		right := makeShuffleJoinTestChild(30, 3_000_000)
		parent := &plan.Node{
			NodeType: plan.Node_JOIN,
			JoinType: plan.Node_LEFT,
			Children: []int32{2, 3},
			OnList: []*plan.Expr{
				makeShuffleJoinEquality(t, types.T_int64, 64, 10, 30, 0),
			},
			Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
				HashmapSize: 3_000_000,
			}},
		}
		return &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{
			left, build, childJoin, right,
		}}}, parent
	}

	t.Run("left join preserves its left partition key", func(t *testing.T) {
		builder, parent := makeChildJoin(plan.Node_LEFT)

		determineShuffleForJoin(parent, builder)

		require.True(t, parent.Stats.HashmapStats.Shuffle)
		require.Equal(t, int32(0), parent.Stats.HashmapStats.ShuffleColIdx)
		require.Equal(t, plan.ShuffleMethod_Reuse, parent.Stats.HashmapStats.ShuffleMethod)
		require.Equal(t, plan.ShuffleType_Range, parent.Stats.HashmapStats.ShuffleType)
	})

	t.Run("following join may reuse hybrid local ownership", func(t *testing.T) {
		builder, parent := makeChildJoin(plan.Node_LEFT)
		builder.qry.Nodes[2].Stats.HashmapStats.ShuffleTypeForMultiCN =
			plan.ShuffleTypeForMultiCN_Hybrid

		determineShuffleForJoin(parent, builder)

		require.True(t, parent.Stats.HashmapStats.Shuffle)
		require.Equal(t, plan.ShuffleMethod_Reuse, parent.Stats.HashmapStats.ShuffleMethod)
		require.Equal(t, plan.ShuffleTypeForMultiCN_Hybrid,
			parent.Stats.HashmapStats.ShuffleTypeForMultiCN)
	})

	t.Run("full join does not preserve one side as a distribution key", func(t *testing.T) {
		builder, parent := makeChildJoin(plan.Node_OUTER)

		determineShuffleForJoin(parent, builder)

		require.False(t, parent.Stats.HashmapStats.Shuffle)
		require.Equal(t, plan.ShuffleMethod_Normal, parent.Stats.HashmapStats.ShuffleMethod)
	})

	t.Run("left join build key cannot describe unmatched probe rows", func(t *testing.T) {
		builder, parent := makeChildJoin(plan.Node_LEFT)
		parent.OnList[0] = makeShuffleJoinEquality(t, types.T_int64, 64, 20, 30, 0)

		determineShuffleForJoin(parent, builder)

		require.False(t, parent.Stats.HashmapStats.Shuffle)
		require.Equal(t, plan.ShuffleMethod_Normal, parent.Stats.HashmapStats.ShuffleMethod)
	})

	t.Run("rollback hint disables join lineage reuse", func(t *testing.T) {
		builder, parent := makeChildJoin(plan.Node_LEFT)
		builder.optimizerHints = &OptimizerHints{outerAntiPlanning: 1}

		determineShuffleForJoin(parent, builder)

		require.False(t, parent.Stats.HashmapStats.Shuffle)
		require.Equal(t, plan.ShuffleMethod_Normal, parent.Stats.HashmapStats.ShuffleMethod)
	})
}

func TestReusableJoinShuffleChildAfterRemap(t *testing.T) {
	child := &plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_LEFT,
		Children: []int32{0, 1},
		OnList: []*plan.Expr{
			makeShuffleJoinEquality(t, types.T_int64, 64, 0, 1, 0),
		},
		ProjectList: []*plan.Expr{
			GetColExpr(plan.Type{Id: int32(types.T_int64)}, 0, 0),
			GetColExpr(plan.Type{Id: int32(types.T_int64)}, 1, 0),
		},
		Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
			Shuffle:       true,
			ShuffleColIdx: 0,
			ShuffleMethod: plan.ShuffleMethod_Reuse,
		}},
	}
	builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{
		makeShuffleJoinTestChild(10, 1),
		makeShuffleJoinTestChild(20, 1),
	}}}
	consumer := &plan.Node{NodeType: plan.Node_JOIN}

	require.True(t, reusableJoinShuffleChild(
		&plan.ColRef{RelPos: 0, ColPos: 0}, consumer, child, builder, true))
	require.False(t, reusableJoinShuffleChild(
		&plan.ColRef{RelPos: 0, ColPos: 1}, consumer, child, builder, true))
}

func TestDetermineShuffleForGroupByRequiresGlobalJoinOwnership(t *testing.T) {
	tests := []struct {
		name        string
		multiCN     plan.ShuffleTypeForMultiCN
		groupRel    int32
		rollback    bool
		wantMethod  plan.ShuffleMethod
		wantMultiCN plan.ShuffleTypeForMultiCN
	}{
		{
			name:        "hybrid left key is only locally partitioned",
			multiCN:     plan.ShuffleTypeForMultiCN_Hybrid,
			groupRel:    10,
			wantMethod:  plan.ShuffleMethod_Normal,
			wantMultiCN: plan.ShuffleTypeForMultiCN_Simple,
		},
		{
			name:        "simple left key has global ownership",
			multiCN:     plan.ShuffleTypeForMultiCN_Simple,
			groupRel:    10,
			wantMethod:  plan.ShuffleMethod_Reuse,
			wantMultiCN: plan.ShuffleTypeForMultiCN_Simple,
		},
		{
			name:        "rollback keeps established global aggregate reuse",
			multiCN:     plan.ShuffleTypeForMultiCN_Simple,
			groupRel:    10,
			rollback:    true,
			wantMethod:  plan.ShuffleMethod_Reuse,
			wantMultiCN: plan.ShuffleTypeForMultiCN_Simple,
		},
		{
			name:        "nullable build key does not preserve ownership",
			multiCN:     plan.ShuffleTypeForMultiCN_Simple,
			groupRel:    20,
			wantMethod:  plan.ShuffleMethod_Normal,
			wantMultiCN: plan.ShuffleTypeForMultiCN_Simple,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left := makeShuffleJoinTestChild(10, 10_000_000)
			right := makeShuffleJoinTestChild(20, 3_000_000)
			childJoin := &plan.Node{
				NodeId:   2,
				NodeType: plan.Node_JOIN,
				JoinType: plan.Node_LEFT,
				Children: []int32{0, 1},
				OnList: []*plan.Expr{
					makeShuffleJoinEquality(t, types.T_int64, 100_000, 10, 20, 0),
				},
				Stats: &plan.Stats{
					Outcnt:      10_000_000,
					Selectivity: 1,
					HashmapStats: &plan.HashMapStats{
						Shuffle:               true,
						ShuffleColIdx:         0,
						ShuffleType:           plan.ShuffleType_Hash,
						ShuffleTypeForMultiCN: tt.multiCN,
					},
				},
			}
			agg := &plan.Node{
				NodeId:   3,
				NodeType: plan.Node_AGG,
				Children: []int32{2},
				GroupBy: []*plan.Expr{{
					Typ:  plan.Type{Id: int32(types.T_int64)},
					Ndv:  100_000,
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tt.groupRel, ColPos: 0}},
				}},
				Stats: &plan.Stats{
					Outcnt:      10_000_000,
					Selectivity: 1,
					HashmapStats: &plan.HashMapStats{
						HashmapSize: 3_000_000,
					},
				},
			}
			builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{
				left, right, childJoin, agg,
			}}}
			if tt.rollback {
				builder.optimizerHints = &OptimizerHints{outerAntiPlanning: 1}
			}

			determineShuffleForGroupBy(agg, builder)

			require.True(t, agg.Stats.HashmapStats.Shuffle)
			require.Equal(t, tt.wantMethod, agg.Stats.HashmapStats.ShuffleMethod)
			require.Equal(t, tt.wantMultiCN, agg.Stats.HashmapStats.ShuffleTypeForMultiCN)
		})
	}
}

func TestDetermineShuffleMethod2RejectsHybridAggregateReuse(t *testing.T) {
	tests := []struct {
		name            string
		hashmapSize     float64
		wantJoinShuffle bool
	}{
		{
			name:            "small build also drops the unnecessary hybrid join",
			hashmapSize:     threshHoldForHybirdShuffle,
			wantJoinShuffle: false,
		},
		{
			name:            "large build keeps the hybrid join but globally repartitions the aggregate",
			hashmapSize:     threshHoldForHybirdShuffle + 1,
			wantJoinShuffle: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			childJoin := &plan.Node{
				NodeId:   0,
				NodeType: plan.Node_JOIN,
				Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
					Shuffle:               true,
					ShuffleType:           plan.ShuffleType_Range,
					ShuffleTypeForMultiCN: plan.ShuffleTypeForMultiCN_Hybrid,
					HashmapSize:           tt.hashmapSize,
				}},
			}
			agg := &plan.Node{
				NodeId:   1,
				NodeType: plan.Node_AGG,
				Children: []int32{0},
				Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
					Shuffle:               true,
					ShuffleType:           plan.ShuffleType_Range,
					ShuffleTypeForMultiCN: plan.ShuffleTypeForMultiCN_Hybrid,
					ShuffleMethod:         plan.ShuffleMethod_Reuse,
				}},
			}
			builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{childJoin, agg}}}

			determineShuffleMethod2(agg.NodeId, -1, builder)

			require.Equal(t, tt.wantJoinShuffle, childJoin.Stats.HashmapStats.Shuffle)
			require.Equal(t, plan.ShuffleMethod_Normal, agg.Stats.HashmapStats.ShuffleMethod)
			require.Equal(t, plan.ShuffleTypeForMultiCN_Simple,
				agg.Stats.HashmapStats.ShuffleTypeForMultiCN)
		})
	}
}

func TestDetermineShuffleForJoinReuseMatchesChildPartition(t *testing.T) {
	tests := []struct {
		name      string
		childIdx  int32
		childType plan.ShuffleType
		wantIdx   int32
	}{
		{
			name:      "reuse preserves hash partitioning",
			childIdx:  0,
			childType: plan.ShuffleType_Hash,
			wantIdx:   0,
		},
		{
			name:      "only the actual child shuffle key is reusable",
			childIdx:  1,
			childType: plan.ShuffleType_Range,
			wantIdx:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left := makeShuffleJoinTestChild(10, 10_000_000)
			left.NodeType = plan.Node_AGG
			left.GroupBy = []*plan.Expr{
				{
					Typ:  plan.Type{Id: int32(types.T_int64), NotNullable: true},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 100, ColPos: 0}},
				},
				{
					Typ:  plan.Type{Id: int32(types.T_int64), NotNullable: true},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 100, ColPos: 1}},
				},
			}
			left.Stats.HashmapStats = &plan.HashMapStats{
				Shuffle:               true,
				ShuffleColIdx:         tt.childIdx,
				ShuffleType:           tt.childType,
				ShuffleTypeForMultiCN: plan.ShuffleTypeForMultiCN_Hybrid,
				HashmapSize:           9_000_000,
				ShuffleColMin:         10,
				ShuffleColMax:         1_000_000,
				Ranges:                []float64{100, 1_000},
				Nullcnt:               7,
			}
			node := &plan.Node{
				NodeType: plan.Node_JOIN,
				JoinType: plan.Node_INNER,
				Children: []int32{0, 1},
				OnList: []*plan.Expr{
					makeShuffleJoinEquality(t, types.T_int64, 64, 10, 20, 0),
					makeShuffleJoinEquality(t, types.T_int64, 100_000, 10, 20, 1),
				},
				Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
					HashmapSize: 3_000_000,
				}},
			}
			builder := &QueryBuilder{
				qry: &plan.Query{Nodes: []*plan.Node{
					left,
					makeShuffleJoinTestChild(20, 3_000_000),
				}},
				tag2Table: map[int32]*plan.TableDef{100: {}},
			}

			determineShuffleForJoin(node, builder)

			require.True(t, node.Stats.HashmapStats.Shuffle)
			require.Equal(t, tt.wantIdx, node.Stats.HashmapStats.ShuffleColIdx)
			require.Equal(t, plan.ShuffleMethod_Reuse, node.Stats.HashmapStats.ShuffleMethod)
			require.Equal(t, tt.childType, node.Stats.HashmapStats.ShuffleType)
			require.Equal(t, plan.ShuffleTypeForMultiCN_Hybrid, node.Stats.HashmapStats.ShuffleTypeForMultiCN)
			require.Equal(t, float64(3_000_000), node.Stats.HashmapStats.HashmapSize,
				"reuse must not replace the join build cardinality with the aggregate cardinality")
		})
	}
}

func TestDetermineShuffleForJoinReprovesReuseAcrossRealRemap(t *testing.T) {
	builder, join, agg := makeShuffleJoinRealRemapFixture(t)

	// The normal optimizer pass proves that the low-NDV first condition reuses
	// the aggregate's existing partitioning.
	determineShuffleForJoin(join, builder)
	require.True(t, join.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(0), join.Stats.HashmapStats.ShuffleColIdx)
	require.Equal(t, plan.ShuffleMethod_Reuse, join.Stats.HashmapStats.ShuffleMethod)

	// Exercise the same remapping routine used by createQuery. In particular,
	// aggregate BindingTags disappear and join inputs become local RelPos 0/1.
	_, err := builder.remapAllColRefs(
		join.NodeId,
		0,
		make(map[[2]int32]int),
		make(map[[2]int32]bool),
		make(map[[2]int32]int),
	)
	require.NoError(t, err)
	require.Empty(t, agg.BindingTags)
	require.Len(t, agg.ProjectList, 2)
	require.Equal(t, int32(-1), agg.ProjectList[0].GetCol().RelPos)
	require.Equal(t, int32(0), agg.ProjectList[0].GetCol().ColPos)
	require.Equal(t, int32(0), join.OnList[0].GetF().Args[0].GetCol().RelPos)
	require.Equal(t, int32(1), join.OnList[0].GetF().Args[1].GetCol().RelPos)

	// The late DML pass must re-prove reuse through the remapped aggregate
	// output, not inherit the previous generation's method blindly.
	determineShuffleForJoinWithColRefMode(join, builder, true)
	require.True(t, join.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(0), join.Stats.HashmapStats.ShuffleColIdx)
	require.Equal(t, plan.ShuffleMethod_Reuse, join.Stats.HashmapStats.ShuffleMethod)
	require.Equal(t, plan.ShuffleType_Range, join.Stats.HashmapStats.ShuffleType)

	// Simulate a later planner generation changing the aggregate partition key
	// to a third group key. The first join key is now low and non-reusable, so the
	// second key wins. Its strategy must be clean Normal/Hash state; carrying the
	// old key's Reuse would make compile skip the probe shuffle incorrectly.
	agg.Stats.HashmapStats.ShuffleColIdx = 2
	determineShuffleForJoinWithColRefMode(join, builder, true)
	require.True(t, join.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(1), join.Stats.HashmapStats.ShuffleColIdx)
	require.Equal(t, plan.ShuffleMethod_Normal, join.Stats.HashmapStats.ShuffleMethod)
	require.Equal(t, plan.ShuffleType_Hash, join.Stats.HashmapStats.ShuffleType)
	require.Equal(t, plan.ShuffleTypeForMultiCN_Simple, join.Stats.HashmapStats.ShuffleTypeForMultiCN)
	require.Zero(t, join.Stats.HashmapStats.ShuffleColMin)
	require.Zero(t, join.Stats.HashmapStats.ShuffleColMax)
	require.Zero(t, join.Stats.HashmapStats.Nullcnt)
	require.Nil(t, join.Stats.HashmapStats.Ranges)
}

func TestDetermineShuffleForJoinNormalizesReversedConditionAfterRemap(t *testing.T) {
	condition := makeShuffleJoinEquality(t, types.T_int64, 100_000, 1, 0, 0)
	node := &plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
		Children: []int32{0, 1},
		OnList:   []*plan.Expr{condition},
		Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
			HashmapSize: 3_000_000,
		}},
	}
	builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{
		makeShuffleJoinTestChild(10, 10_000_000),
		makeShuffleJoinTestChild(20, 3_000_000),
	}}}

	determineShuffleForJoinWithColRefMode(node, builder, true)

	require.True(t, node.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(0), condition.GetF().Args[0].GetCol().RelPos)
	require.Equal(t, int32(1), condition.GetF().Args[1].GetCol().RelPos)
}

func TestDetermineShuffleForJoinSkipsCandidateRejectedByFinalRecheck(t *testing.T) {
	tests := []struct {
		name            string
		expressionFirst bool
		expressionOnly  bool
		hashmapSize     float64
		wantIdx         int32
		wantType        plan.ShuffleType
		wantMethod      plan.ShuffleMethod
	}{
		{
			name:        "reusable range key remains first choice",
			hashmapSize: threshHoldForHashShuffle - 1,
			wantIdx:     0,
			wantType:    plan.ShuffleType_Range,
			wantMethod:  plan.ShuffleMethod_Reuse,
		},
		{
			name:            "rejected hash key does not hide reusable range key",
			expressionFirst: true,
			hashmapSize:     threshHoldForHashShuffle - 1,
			wantIdx:         1,
			wantType:        plan.ShuffleType_Range,
			wantMethod:      plan.ShuffleMethod_Reuse,
		},
		{
			name:            "reusable key avoids an otherwise eligible reshuffle",
			expressionFirst: true,
			hashmapSize:     threshHoldForHashShuffle,
			wantIdx:         1,
			wantType:        plan.ShuffleType_Range,
			wantMethod:      plan.ShuffleMethod_Reuse,
		},
		{
			name:            "hash threshold is inclusive without a reusable key",
			expressionFirst: true,
			expressionOnly:  true,
			hashmapSize:     threshHoldForHashShuffle,
			wantIdx:         0,
			wantType:        plan.ShuffleType_Hash,
			wantMethod:      plan.ShuffleMethod_Normal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expressionKey := makeShuffleJoinSerialEquality(t, 100_000, 10, 20, 0)
			reusableKey := makeShuffleJoinEquality(t, types.T_int64, 100_000, 10, 20, 1)
			conditions := []*plan.Expr{reusableKey, expressionKey}
			if tt.expressionFirst {
				conditions = []*plan.Expr{expressionKey, reusableKey}
			}
			if tt.expressionOnly {
				conditions = []*plan.Expr{expressionKey}
			}

			left := makeShuffleJoinTestChild(10, 10_000_000)
			left.NodeType = plan.Node_AGG
			left.GroupBy = []*plan.Expr{
				{
					Typ:  plan.Type{Id: int32(types.T_int64), NotNullable: true},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 100, ColPos: 0}},
				},
				{
					Typ:  plan.Type{Id: int32(types.T_int64), NotNullable: true},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 100, ColPos: 1}},
				},
			}
			left.Stats.HashmapStats = &plan.HashMapStats{
				Shuffle:       true,
				ShuffleColIdx: 1,
				ShuffleType:   plan.ShuffleType_Range,
				HashmapSize:   3_000_000,
				ShuffleColMin: 0,
				ShuffleColMax: 1_000_000,
			}
			right := makeShuffleJoinTestChild(20, 3_000_000)
			node := &plan.Node{
				NodeType: plan.Node_JOIN,
				JoinType: plan.Node_INNER,
				Children: []int32{0, 1},
				OnList:   conditions,
				Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
					// The expression key is forced to hash shuffle and therefore
					// rejected below the 2 MiB hash threshold. The reusable range
					// key after it must still be considered.
					HashmapSize: tt.hashmapSize,
				}},
			}
			builder := &QueryBuilder{
				qry:       &plan.Query{Nodes: []*plan.Node{left, right}},
				tag2Table: map[int32]*plan.TableDef{100: {}},
			}

			determineShuffleForJoin(node, builder)

			require.True(t, node.Stats.HashmapStats.Shuffle)
			require.Equal(t, tt.wantIdx, node.Stats.HashmapStats.ShuffleColIdx)
			require.Equal(t, tt.wantType, node.Stats.HashmapStats.ShuffleType)
			require.Equal(t, tt.wantMethod, node.Stats.HashmapStats.ShuffleMethod)
		})
	}
}

func TestSelectShuffleJoinConditionAdversarialPermutations(t *testing.T) {
	unsupported := makeShuffleJoinEquality(t, types.T_float64, 100_000, 10, 20, 0)
	knownLow := makeShuffleJoinEquality(t, types.T_int64, ShuffleThreshHoldOfNDV-1, 10, 20, 1)
	rejectedExpression := makeShuffleJoinSerialEquality(t, 100_000, 10, 20, 2)
	reusable := makeShuffleJoinEquality(t, types.T_int64, 100_000, 10, 20, 3)

	left := makeShuffleJoinTestChild(10, 10_000_000)
	left.NodeType = plan.Node_AGG
	left.GroupBy = []*plan.Expr{
		{},
		{},
		{},
		{
			Typ:  plan.Type{Id: int32(types.T_int64), NotNullable: true},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 100, ColPos: 3}},
		},
	}
	left.Stats.HashmapStats = &plan.HashMapStats{
		Shuffle:       true,
		ShuffleColIdx: 3,
		ShuffleType:   plan.ShuffleType_Range,
		HashmapSize:   3_000_000,
		ShuffleColMin: 0,
		ShuffleColMax: 1_000_000,
	}
	right := makeShuffleJoinTestChild(20, 3_000_000)
	builder := &QueryBuilder{
		qry:       &plan.Query{Nodes: []*plan.Node{left, right}},
		tag2Table: map[int32]*plan.TableDef{100: {}},
	}
	leftTags := map[int32]bool{10: true}
	rightTags := map[int32]bool{20: true}

	permutationCount := 0
	var checkPermutations func([]*plan.Expr, int)
	checkPermutations = func(conditions []*plan.Expr, next int) {
		if next == len(conditions) {
			permutationCount++
			node := &plan.Node{
				NodeType: plan.Node_JOIN,
				JoinType: plan.Node_INNER,
				Children: []int32{0, 1},
				Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
					HashmapSize: threshHoldForHashShuffle - 1,
				}},
			}

			idx, _ := selectShuffleJoinCondition(node, builder, conditions, leftTags, rightTags, false, nil)

			require.NotEqual(t, -1, idx)
			require.Same(t, reusable, conditions[idx])
			return
		}

		for i := next; i < len(conditions); i++ {
			permutation := append([]*plan.Expr(nil), conditions...)
			permutation[next], permutation[i] = permutation[i], permutation[next]
			checkPermutations(permutation, next+1)
		}
	}

	checkPermutations([]*plan.Expr{unsupported, knownLow, rejectedExpression, reusable}, 0)
	require.Equal(t, 24, permutationCount)
}

func makeShuffleJoinEquality(
	t *testing.T,
	keyType types.T,
	ndv float64,
	leftRel, rightRel, colPos int32,
) *plan.Expr {
	t.Helper()

	typ := keyType.ToType()
	equal, err := function.GetFunctionByName(context.Background(), "=", []types.Type{typ, typ})
	require.NoError(t, err)

	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool), NotNullable: true},
		Ndv: ndv,
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: equal.GetEncodedOverloadID(), ObjName: "="},
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(keyType), NotNullable: true},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: leftRel,
						ColPos: colPos,
					}},
				},
				{
					Typ: plan.Type{Id: int32(keyType), NotNullable: true},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: rightRel,
						ColPos: colPos,
					}},
				},
			},
		}},
	}
}

func makeShuffleJoinSerialEquality(
	t *testing.T,
	ndv float64,
	leftRel, rightRel, colPos int32,
) *plan.Expr {
	t.Helper()

	condition := makeShuffleJoinEquality(t, types.T_varchar, ndv, leftRel, rightRel, colPos)
	for i, arg := range condition.GetF().Args {
		condition.GetF().Args[i] = &plan.Expr{
			Typ: arg.Typ,
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "serial"},
				Args: []*plan.Expr{arg},
			}},
		}
	}
	return condition
}

func makeShuffleJoinTestChild(bindingTag int32, outcnt float64) *plan.Node {
	return &plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		BindingTags: []int32{bindingTag},
		ProjectList: []*plan.Expr{
			makeMarkShuffleColumn(true, bindingTag, 0),
			makeMarkShuffleColumn(true, bindingTag, 1),
		},
		Stats: &plan.Stats{Outcnt: outcnt, HashmapStats: &plan.HashMapStats{}},
	}
}

func makeShuffleJoinRealRemapFixture(t *testing.T) (*QueryBuilder, *plan.Node, *plan.Node) {
	t.Helper()

	intType := plan.Type{Id: int32(types.T_int64), NotNullable: true}
	makeTableDef := func(id uint64, prefix string) *plan.TableDef {
		return &plan.TableDef{
			TblId: id,
			Cols: []*plan.ColDef{
				{Name: prefix + "_a", Typ: intType},
				{Name: prefix + "_b", Typ: intType},
				{Name: prefix + "_c", Typ: intType},
			},
		}
	}

	leftTable := makeTableDef(1, "left")
	leftScan := &plan.Node{
		NodeId:      0,
		NodeType:    plan.Node_TABLE_SCAN,
		BindingTags: []int32{100},
		TableDef:    leftTable,
		Stats: &plan.Stats{
			Outcnt:       10_000_000,
			HashmapStats: &plan.HashMapStats{},
		},
	}
	agg := &plan.Node{
		NodeId:      1,
		NodeType:    plan.Node_AGG,
		Children:    []int32{0},
		BindingTags: []int32{10, 11},
		GroupBy: []*plan.Expr{
			{Typ: intType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 100, ColPos: 0}}},
			{Typ: intType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 100, ColPos: 1}}},
			{Typ: intType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 100, ColPos: 2}}},
		},
		Stats: &plan.Stats{
			Outcnt: 10_000_000,
			HashmapStats: &plan.HashMapStats{
				Shuffle:               true,
				ShuffleColIdx:         0,
				ShuffleType:           plan.ShuffleType_Range,
				ShuffleTypeForMultiCN: plan.ShuffleTypeForMultiCN_Hybrid,
				HashmapSize:           9_000_000,
				ShuffleColMin:         10,
				ShuffleColMax:         1_000_000,
				Ranges:                []float64{100, 1_000},
				Nullcnt:               7,
			},
		},
	}

	rightTable := makeTableDef(2, "right")
	rightScan := &plan.Node{
		NodeId:      2,
		NodeType:    plan.Node_TABLE_SCAN,
		BindingTags: []int32{20},
		TableDef:    rightTable,
		Stats: &plan.Stats{
			Outcnt:       3_000_000,
			HashmapStats: &plan.HashMapStats{},
		},
	}
	join := &plan.Node{
		NodeId:   3,
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
		Children: []int32{1, 2},
		OnList: []*plan.Expr{
			makeShuffleJoinEquality(t, types.T_int64, 64, 10, 20, 0),
			makeShuffleJoinEquality(t, types.T_int64, 100_000, 10, 20, 1),
		},
		Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
			HashmapSize: 3_000_000,
		}},
	}
	builder := &QueryBuilder{
		qry: &plan.Query{
			Nodes: []*plan.Node{leftScan, agg, rightScan, join},
			Steps: []int32{join.NodeId},
		},
		tag2Table: map[int32]*plan.TableDef{
			100: leftTable,
			20:  rightTable,
		},
	}
	return builder, join, agg
}

func TestDetermineShuffleForLatePlanStep(t *testing.T) {
	// IVF maintenance also contains internal scans without binding tags. The
	// post-createQuery shuffle pass must recognize its local RelPos 0/1 join
	// condition while tolerating unrelated untagged scans.
	ivfScanWithoutBindingTag := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		}},
		Stats: &plan.Stats{Outcnt: 1000, HashmapStats: &plan.HashMapStats{}},
	}
	left := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{},
		Stats:    &plan.Stats{Outcnt: 1000, HashmapStats: &plan.HashMapStats{}},
	}
	right := &plan.Node{
		NodeType: plan.Node_SINK_SCAN,
		Stats:    &plan.Stats{Outcnt: 10_000_000, HashmapStats: &plan.HashMapStats{}},
	}
	cond, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}}},
	})
	require.NoError(t, err)
	cond.Ndv = -1
	join := &plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
		Children: []int32{0, 1},
		OnList:   []*plan.Expr{cond},
		Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
			HashmapSize: 10_000_000,
		}},
	}
	builder := NewQueryBuilder(plan.Query_INSERT, NewMockCompilerContext(true), false, true)
	builder.qry = &plan.Query{
		Nodes: []*plan.Node{left, right, join, ivfScanWithoutBindingTag},
		Steps: []int32{3, 2},
	}

	builder.determineShuffleForDMLSteps()

	require.True(t, join.Stats.HashmapStats.Shuffle)
	require.Len(t, join.RuntimeFilterProbeList, 1)
	require.Len(t, join.RuntimeFilterBuildList, 1)
	require.Equal(t, join.RuntimeFilterProbeList[0].Tag, join.RuntimeFilterBuildList[0].Tag)
	require.Nil(t, join.RuntimeFilterProbeList[0].Expr)
	require.Nil(t, join.RuntimeFilterBuildList[0].Expr)

	// A same-side equality remains non-equi for join planning after remapping.
	sameSideCond, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}}},
	})
	require.NoError(t, err)
	join.OnList = []*plan.Expr{sameSideCond}
	join.Stats.HashmapStats.Shuffle = false
	join.RuntimeFilterProbeList = nil
	join.RuntimeFilterBuildList = nil
	builder.determineShuffleForDMLSteps()
	require.False(t, join.Stats.HashmapStats.Shuffle)
	require.Empty(t, join.RuntimeFilterProbeList)
	require.Empty(t, join.RuntimeFilterBuildList)
}

func TestDetermineShuffleForMarkJoin(t *testing.T) {
	tests := []struct {
		name        string
		notNullable bool
		sameSide    bool
		afterRemap  bool
		wantShuffle bool
	}{
		{
			name:        "non-null keys can shuffle",
			notNullable: true,
			wantShuffle: true,
		},
		{
			name:        "non-null keys can shuffle after remap",
			notNullable: true,
			afterRemap:  true,
			wantShuffle: true,
		},
		{
			name: "nullable keys stay broadcast",
		},
		{
			name:        "same-side equality stays broadcast",
			notNullable: true,
			sameSide:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left := &plan.Node{
				NodeType:    plan.Node_TABLE_SCAN,
				BindingTags: []int32{10},
				Stats:       &plan.Stats{Outcnt: 10_000_000, HashmapStats: &plan.HashMapStats{}},
				ProjectList: []*plan.Expr{makeMarkShuffleColumn(tt.notNullable, 0, 0)},
			}
			right := &plan.Node{
				NodeType:    plan.Node_TABLE_SCAN,
				BindingTags: []int32{20},
				Stats:       &plan.Stats{Outcnt: 3_000_000, HashmapStats: &plan.HashMapStats{}},
				ProjectList: []*plan.Expr{makeMarkShuffleColumn(tt.notNullable, 0, 0)},
			}
			leftRel, rightRel := int32(10), int32(20)
			if tt.afterRemap {
				leftRel, rightRel = 0, 1
			}
			condition := makeMarkShuffleEquality(t, tt.notNullable, leftRel, rightRel)
			if tt.sameSide {
				condition.GetF().Args[1].GetCol().RelPos = leftRel
			}
			node := &plan.Node{
				NodeType: plan.Node_JOIN,
				JoinType: plan.Node_MARK,
				Children: []int32{0, 1},
				OnList:   []*plan.Expr{condition},
				Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
					// Verify that ineligible MARK plans also clear stale
					// shuffle metadata instead of reaching the compiler.
					Shuffle:     true,
					HashmapSize: 3_000_000,
				}},
			}
			builder := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{left, right}}}

			determineShuffleForJoinWithColRefMode(node, builder, tt.afterRemap)

			require.Equal(t, tt.wantShuffle, node.Stats.HashmapStats.Shuffle)
			if tt.wantShuffle {
				require.Equal(t, int32(0), node.Stats.HashmapStats.ShuffleColIdx)
				require.Equal(t, plan.ShuffleType_Hash, node.Stats.HashmapStats.ShuffleType)
			} else {
				require.Equal(t, int32(-1), node.Stats.HashmapStats.ShuffleColIdx)
			}
		})
	}
}

func TestDetermineShuffleForMarkJoinRejectsPreRemapOuterExtension(t *testing.T) {
	nation := &plan.Node{
		NodeId:      0,
		NodeType:    plan.Node_TABLE_SCAN,
		BindingTags: []int32{10},
		ProjectList: []*plan.Expr{makeMarkShuffleColumn(true, 10, 0)},
		Stats:       &plan.Stats{Outcnt: 10_000_000, HashmapStats: &plan.HashMapStats{}},
	}
	region := &plan.Node{
		NodeId:      1,
		NodeType:    plan.Node_TABLE_SCAN,
		BindingTags: []int32{20},
		ProjectList: []*plan.Expr{makeMarkShuffleColumn(true, 20, 0)},
		Stats:       &plan.Stats{Outcnt: 5, HashmapStats: &plan.HashMapStats{}},
	}
	outerJoin := &plan.Node{
		NodeId:   2,
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_LEFT,
		Children: []int32{0, 1},
		Stats:    &plan.Stats{Outcnt: 10_000_000, HashmapStats: &plan.HashMapStats{}},
	}
	lineitem := &plan.Node{
		NodeId:      3,
		NodeType:    plan.Node_TABLE_SCAN,
		BindingTags: []int32{30},
		ProjectList: []*plan.Expr{makeMarkShuffleColumn(true, 30, 0)},
		Stats:       &plan.Stats{Outcnt: 3_000_000, HashmapStats: &plan.HashMapStats{}},
	}
	mark := &plan.Node{
		NodeId:   4,
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_MARK,
		Children: []int32{2, 3},
		OnList:   []*plan.Expr{makeMarkShuffleEquality(t, true, 20, 30)},
		Stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
			HashmapSize: 3_000_000,
		}},
	}
	builder := &QueryBuilder{qry: &plan.Query{
		Nodes: []*plan.Node{nation, region, outerJoin, lineitem, mark},
	}}

	determineShuffleForJoinWithColRefMode(mark, builder, false)

	require.False(t, mark.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(-1), mark.Stats.HashmapStats.ShuffleColIdx)
}

func TestNodeNullExtendsChild(t *testing.T) {
	for _, tt := range []struct {
		name     string
		node     *plan.Node
		childIdx int
		want     bool
	}{
		{
			name:     "left join right child",
			node:     &plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_LEFT},
			childIdx: 1,
			want:     true,
		},
		{
			name:     "left join left child",
			node:     &plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_LEFT},
			childIdx: 0,
		},
		{
			name:     "right join left child",
			node:     &plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_RIGHT},
			childIdx: 0,
			want:     true,
		},
		{
			name:     "full join left child",
			node:     &plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_OUTER},
			childIdx: 0,
			want:     true,
		},
		{
			name:     "full join right child",
			node:     &plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_OUTER},
			childIdx: 1,
			want:     true,
		},
		{
			name:     "left single build child",
			node:     &plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_SINGLE},
			childIdx: 1,
			want:     true,
		},
		{
			name: "right single swapped child",
			node: &plan.Node{
				NodeType:    plan.Node_JOIN,
				JoinType:    plan.Node_SINGLE,
				IsRightJoin: true,
			},
			childIdx: 0,
			want:     true,
		},
		{
			name: "outer apply right child",
			node: &plan.Node{
				NodeType:  plan.Node_APPLY,
				ApplyType: plan.Node_OUTERAPPLY,
			},
			childIdx: 1,
			want:     true,
		},
		{
			name: "cross apply right child",
			node: &plan.Node{
				NodeType:  plan.Node_APPLY,
				ApplyType: plan.Node_CROSSAPPLY,
			},
			childIdx: 1,
		},
		{
			name:     "full join invalid child",
			node:     &plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_OUTER},
			childIdx: 2,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, nodeNullExtendsChild(tt.node, tt.childIdx))
		})
	}
}

func TestOutputTypeAfterNullExtension(t *testing.T) {
	notNullType := plan.Type{
		Id:          int32(types.T_int64),
		NotNullable: true,
	}
	for _, tt := range []struct {
		name     string
		node     *plan.Node
		childIdx int
		want     bool
	}{
		{
			name:     "inner preserves not null",
			node:     &plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER},
			childIdx: 1,
			want:     true,
		},
		{
			name:     "left join null extends right",
			node:     &plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_LEFT},
			childIdx: 1,
		},
		{
			name:     "left single null extends build",
			node:     &plan.Node{NodeType: plan.Node_JOIN, JoinType: plan.Node_SINGLE},
			childIdx: 1,
		},
		{
			name: "right single null extends swapped build",
			node: &plan.Node{
				NodeType:    plan.Node_JOIN,
				JoinType:    plan.Node_SINGLE,
				IsRightJoin: true,
			},
			childIdx: 0,
		},
		{
			name: "outer apply null extends function output",
			node: &plan.Node{
				NodeType:  plan.Node_APPLY,
				ApplyType: plan.Node_OUTERAPPLY,
			},
			childIdx: 1,
		},
		{
			name: "cross apply preserves not null",
			node: &plan.Node{
				NodeType:  plan.Node_APPLY,
				ApplyType: plan.Node_CROSSAPPLY,
			},
			childIdx: 1,
			want:     true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			outputType := outputTypeAfterNullExtension(tt.node, tt.childIdx, notNullType)
			require.Equal(t, tt.want, outputType.NotNullable)
			require.True(t, notNullType.NotNullable, "input type must not be mutated")
		})
	}
}

func TestSetOperationOutputType(t *testing.T) {
	notNullType := plan.Type{
		Id:          int32(types.T_int64),
		NotNullable: true,
	}
	nullableType := notNullType
	nullableType.NotNullable = false

	for _, tt := range []struct {
		name      string
		nodeType  plan.Node_NodeType
		leftType  plan.Type
		rightType plan.Type
		want      bool
	}{
		{
			name:      "union preserves non-null when both inputs are non-null",
			nodeType:  plan.Node_UNION,
			leftType:  notNullType,
			rightType: notNullType,
			want:      true,
		},
		{
			name:      "union sees nullable left input",
			nodeType:  plan.Node_UNION,
			leftType:  nullableType,
			rightType: notNullType,
		},
		{
			name:      "union sees nullable right input",
			nodeType:  plan.Node_UNION,
			leftType:  notNullType,
			rightType: nullableType,
		},
		{
			name:      "union all sees nullable right input",
			nodeType:  plan.Node_UNION_ALL,
			leftType:  notNullType,
			rightType: nullableType,
		},
		{
			name:      "intersect keeps left output contract",
			nodeType:  plan.Node_INTERSECT,
			leftType:  notNullType,
			rightType: nullableType,
			want:      true,
		},
		{
			name:      "intersect all stays conservative for nullable left",
			nodeType:  plan.Node_INTERSECT_ALL,
			leftType:  nullableType,
			rightType: notNullType,
		},
		{
			name:      "minus ignores right nullability",
			nodeType:  plan.Node_MINUS,
			leftType:  notNullType,
			rightType: nullableType,
			want:      true,
		},
		{
			name:      "minus all keeps nullable left",
			nodeType:  plan.Node_MINUS_ALL,
			leftType:  nullableType,
			rightType: notNullType,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := setOperationOutputType(tt.nodeType, tt.leftType, tt.rightType)
			require.Equal(t, tt.want, got.NotNullable)
			require.Equal(t, notNullType.Id, got.Id)
		})
	}
}

func TestUnionAllOutputNullabilityBeforeOptimization(t *testing.T) {
	optimizer := NewMockOptimizer(true)
	statements, err := mysql.Parse(
		optimizer.CurrentContext().GetContext(),
		`select n.n_regionkey as regionkey from tpch.nation n
		 union all
		 select cast(null as int) as regionkey from tpch.region r
		 union all
		 select r.r_regionkey as regionkey from tpch.region r`,
		1,
	)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	stmt, ok := statements[0].(*tree.Select)
	require.True(t, ok)

	builder := NewQueryBuilder(plan.Query_SELECT, optimizer.CurrentContext(), false, true)
	bindCtx := NewBindContext(builder, nil)
	rootID, err := builder.bindSelect(stmt, bindCtx, true)
	require.NoError(t, err)

	var lastUnionAll *plan.Node
	for _, node := range builder.qry.Nodes {
		if node.NodeType == plan.Node_UNION_ALL {
			lastUnionAll = node
		}
	}
	require.NotNil(t, lastUnionAll)
	require.Len(t, lastUnionAll.ProjectList, 1)
	require.False(t, lastUnionAll.ProjectList[0].Typ.NotNullable,
		"a later non-null branch must not erase a nullable middle branch")

	root := builder.qry.Nodes[rootID]
	require.Len(t, root.ProjectList, 1)
	require.False(t, root.ProjectList[0].Typ.NotNullable,
		"the set result projection must expose the combined contract before optimization")
}

func TestMarkShuffleRejectsOuterJoinNullExtension(t *testing.T) {
	for _, tt := range []struct {
		name          string
		sql           string
		outerJoinType plan.Node_JoinType
	}{
		{
			name:          "left join null extends right key",
			outerJoinType: plan.Node_LEFT,
			sql: `select r.r_regionkey in (
				select l.l_partkey from tpch.lineitem l
			)
			from tpch.nation n
			left join tpch.region r on n.n_regionkey = r.r_regionkey`,
		},
		{
			name:          "full join null extends both keys",
			outerJoinType: plan.Node_OUTER,
			sql: `select n.n_regionkey in (
				select l.l_partkey from tpch.lineitem l
			)
			from tpch.nation n
			full outer join tpch.region r on n.n_regionkey = r.r_regionkey`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, tt.sql)
			require.NoError(t, err)

			query := logicPlan.GetQuery()
			require.NotNil(t, query)

			var mark, outerJoin *plan.Node
			for _, node := range query.Nodes {
				if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_MARK {
					mark = node
				}
				if node.NodeType == plan.Node_JOIN && node.JoinType == tt.outerJoinType {
					outerJoin = node
				}
			}
			require.NotNil(t, mark)
			require.NotNil(t, outerJoin)
			require.Len(t, mark.Children, 2)
			require.NotEmpty(t, mark.OnList)

			for _, childID := range outerJoin.Children {
				for _, output := range query.Nodes[childID].ProjectList {
					require.True(t, output.Typ.NotNullable,
						"outer-join null extension must not mutate a child output")
				}
			}
			outerHasNullableOutput := false
			for _, output := range outerJoin.ProjectList {
				outerHasNullableOutput = outerHasNullableOutput || !output.Typ.NotNullable
			}
			require.True(t, outerHasNullableOutput)

			left := query.Nodes[mark.Children[0]]
			right := query.Nodes[mark.Children[1]]
			condition := mark.OnList[0].GetF()
			require.NotNil(t, condition)
			require.Len(t, condition.Args, 2)
			effectiveNullability := []bool{
				IsJoinExprProvenNotNullable(condition.Args[0], left, right),
				IsJoinExprProvenNotNullable(condition.Args[1], left, right),
			}
			require.ElementsMatch(t, []bool{false, true}, effectiveNullability)

			left.Stats.Outcnt = 10_000_000
			right.Stats.Outcnt = 3_000_000
			mark.Stats.HashmapStats.HashmapSize = 3_000_000
			mark.OnList[0].Ndv = 100_000
			mark.Stats.HashmapStats.Shuffle = true

			builder := &QueryBuilder{qry: query}
			determineShuffleForJoinWithColRefMode(mark, builder, true)

			require.False(t, mark.Stats.HashmapStats.Shuffle)
			require.Equal(t, int32(-1), mark.Stats.HashmapStats.ShuffleColIdx)
		})
	}
}

func TestMarkShuffleRejectsSingleJoinNullExtension(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t, `
		select d.regionkey in (
			select l.l_partkey from tpch.lineitem l
		)
		from (
			select (
				select r.r_regionkey
				from tpch.region r
				where r.r_regionkey = n.n_regionkey
			) as regionkey
			from tpch.nation n
		) d`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	var single, mark *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_SINGLE {
			single = node
		}
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_MARK {
			mark = node
		}
	}
	require.NotNil(t, single)
	require.False(t, single.IsRightJoin)
	require.NotNil(t, mark)
	require.Len(t, mark.Children, 2)
	require.NotEmpty(t, mark.OnList)

	for _, output := range query.Nodes[single.Children[1]].ProjectList {
		require.True(t, output.Typ.NotNullable,
			"SINGLE null extension must not mutate its build child")
	}

	buildOutputCount := 0
	for _, output := range single.ProjectList {
		if col := output.GetCol(); col != nil && col.RelPos == 1 {
			buildOutputCount++
			require.False(t, output.Typ.NotNullable,
				"SINGLE's build output is NULL-extended when the scalar subquery returns no row")
		}
	}
	require.Positive(t, buildOutputCount)

	left := query.Nodes[mark.Children[0]]
	right := query.Nodes[mark.Children[1]]
	condition := mark.OnList[0].GetF()
	require.NotNil(t, condition)
	require.Len(t, condition.Args, 2)
	require.ElementsMatch(t, []bool{false, true}, []bool{
		IsJoinExprProvenNotNullable(condition.Args[0], left, right),
		IsJoinExprProvenNotNullable(condition.Args[1], left, right),
	})

	left.Stats.Outcnt = 10_000_000
	right.Stats.Outcnt = 3_000_000
	mark.Stats.HashmapStats.HashmapSize = 3_000_000
	mark.OnList[0].Ndv = 100_000
	mark.Stats.HashmapStats.Shuffle = true

	builder := &QueryBuilder{qry: query}
	determineShuffleForJoinWithColRefMode(mark, builder, true)

	require.False(t, mark.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(-1), mark.Stats.HashmapStats.ShuffleColIdx)
}

func buildMarkPlanBeforeOptimization(
	t *testing.T,
	sql string,
) (*QueryBuilder, *plan.Node) {
	t.Helper()

	optimizer := NewMockOptimizer(true)
	statements, err := mysql.Parse(
		optimizer.CurrentContext().GetContext(),
		sql,
		1,
	)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	stmt, ok := statements[0].(*tree.Select)
	require.True(t, ok)

	builder := NewQueryBuilder(plan.Query_SELECT, optimizer.CurrentContext(), false, true)
	bindCtx := NewBindContext(builder, nil)
	_, err = builder.bindSelect(stmt, bindCtx, true)
	require.NoError(t, err)

	for _, node := range builder.qry.Nodes {
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_MARK {
			return builder, node
		}
	}
	require.FailNow(t, "MARK join not found")
	return nil, nil
}

func TestMarkShuffleRejectsNullExtensionAcrossGroupingMaterializers(t *testing.T) {
	for _, tt := range []struct {
		name                     string
		sql                      string
		materializer             plan.Node_NodeType
		wantShuffle              bool
		skipMaterializerContract bool
		simulateStaleKeyType     bool
	}{
		{
			name:         "aggregate group key",
			materializer: plan.Node_AGG,
			sql: `
				select d.regionkey in (
					select l.l_partkey from tpch.lineitem l
				)
				from (
					select r.r_regionkey as regionkey
					from tpch.nation n
					left join tpch.region r on n.n_regionkey = r.r_regionkey
					group by r.r_regionkey
					limit 10
				) d`,
		},
		{
			name:         "computed aggregate group key",
			materializer: plan.Node_AGG,
			sql: `
				select d.regionkey in (
					select l.l_partkey from tpch.lineitem l
				)
				from (
					select r.r_regionkey + 0 as regionkey
					from tpch.nation n
					left join tpch.region r on n.n_regionkey = r.r_regionkey
					group by r.r_regionkey + 0
					limit 10
				) d`,
		},
		{
			name:                     "value-nullable aggregate function group key",
			materializer:             plan.Node_AGG,
			skipMaterializerContract: true,
			sql: `
				select d.regionkey in (
					select r2.r_name from tpch.region r2
				)
				from (
					select cast(
						json_extract(
							concat('{"a":', n.n_nationkey, '}'),
							'$.missing'
						) as varchar
					) as regionkey
					from tpch.nation n
					group by cast(
						json_extract(
							concat('{"a":', n.n_nationkey, '}'),
							'$.missing'
						) as varchar
					)
					limit 10
				) d`,
		},
		{
			name:         "scalar aggregate output over empty input",
			materializer: plan.Node_AGG,
			sql: `
				select d.regionkey in (
					select l.l_partkey from tpch.lineitem l
				)
				from (
					select max(n.n_regionkey) as regionkey
					from tpch.nation n
					where n.n_name = '__missing__'
				) d`,
		},
		{
			name:         "preserved-side aggregate group key remains eligible",
			materializer: plan.Node_AGG,
			wantShuffle:  true,
			sql: `
				select d.regionkey in (
					select l.l_partkey from tpch.lineitem l
				)
				from (
					select n.n_regionkey as regionkey
					from tpch.nation n
					left join tpch.region r on n.n_regionkey = r.r_regionkey
					group by n.n_regionkey
					limit 10
				) d`,
		},
		{
			name:         "sample group key",
			materializer: plan.Node_SAMPLE,
			sql: `
				select d.regionkey in (
					select l.l_partkey from tpch.lineitem l
				)
				from (
					select r.r_regionkey as regionkey,
						sample(n.n_nationkey, 1 rows) as sampled_key
					from tpch.nation n
					left join tpch.region r on n.n_regionkey = r.r_regionkey
					group by r.r_regionkey
					limit 10
				) d`,
		},
		{
			name:         "non-null sample output remains eligible",
			materializer: plan.Node_SAMPLE,
			wantShuffle:  true,
			sql: `
				select d.regionkey in (
					select l.l_partkey from tpch.lineitem l
				)
				from (
					select sample(n.n_regionkey, 1 rows) as regionkey
					from tpch.nation n
				) d`,
		},
		{
			name:         "time window partition key",
			materializer: plan.Node_TIME_WINDOW,
			sql: `
				select d.regionkey in (
					select l2.l_partkey from tpch.lineitem l2
				)
				from (
					select r.r_regionkey as regionkey, _wstart,
						max(l.l_quantity) as max_quantity
					from tpch.lineitem l
					left join tpch.region r on l.l_partkey = r.r_regionkey
					group by r.r_regionkey
					interval(l.l_shipdate, 5, day)
					limit 10
				) d`,
		},
		{
			name:         "value-nullable time window aggregate",
			materializer: plan.Node_TIME_WINDOW,
			sql: `
				select d.regionkey in (
					select l2.l_partkey from tpch.lineitem l2
				)
				from (
					select max(cast(json_extract(
						concat('{"a":', l.l_partkey, '}'),
						'$.missing'
					) as int)) as regionkey
					from tpch.lineitem l
					interval(l.l_shipdate, 5, day)
					limit 10
				) d`,
		},
		{
			name:                 "lag window output",
			materializer:         plan.Node_WINDOW,
			simulateStaleKeyType: true,
			sql: `
				select d.regionkey in (
					select l.l_partkey from tpch.lineitem l
				)
				from (
					select lag(n.n_regionkey) over (
							partition by n.n_regionkey
							order by n.n_nationkey
						) as regionkey,
						row_number() over (
							partition by n.n_name
							order by n.n_nationkey
						) as unused_rank
					from tpch.nation n
				) d`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			preRemapBuilder, preRemapMark := buildMarkPlanBeforeOptimization(t, tt.sql)
			require.Len(t, preRemapMark.Children, 2)
			preRemapLeft := preRemapBuilder.qry.Nodes[preRemapMark.Children[0]]
			preRemapRight := preRemapBuilder.qry.Nodes[preRemapMark.Children[1]]
			preRemapLeft.Stats.Outcnt = 10_000_000
			preRemapRight.Stats.Outcnt = 3_000_000
			preRemapMark.Stats.HashmapStats.HashmapSize = 3_000_000
			preRemapMark.OnList[0].Ndv = 100_000
			preRemapMark.Stats.HashmapStats.Shuffle = true

			if tt.simulateStaleKeyType {
				foundNullableWindow := false
				for _, node := range preRemapBuilder.qry.Nodes {
					for _, expr := range node.WinSpecList {
						windowFunc := expr.GetW().GetWindowFunc().GetF()
						if windowFunc != nil && windowFunc.Func.ObjName == "lag" {
							require.False(t, expr.Typ.NotNullable,
								"LAG without a default must expose its partition-boundary NULL")
							foundNullableWindow = true
						}
					}
				}
				require.True(t, foundNullableWindow)

				condition := preRemapMark.OnList[0].GetF()
				require.NotNil(t, condition)
				for _, arg := range condition.Args {
					arg.Typ.NotNullable = true
				}
			}
			determineShuffleForJoinWithColRefMode(preRemapMark, preRemapBuilder, false)
			require.Equal(t, tt.wantShuffle, preRemapMark.Stats.HashmapStats.Shuffle,
				"pre-remap eligibility must follow the group expression to its materialized child")

			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, tt.sql)
			require.NoError(t, err)

			query := logicPlan.GetQuery()
			require.NotNil(t, query)

			var materializer, mark *plan.Node
			for _, node := range query.Nodes {
				if node.NodeType == tt.materializer {
					materializer = node
				}
				if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_MARK {
					mark = node
				}
			}
			require.NotNil(t, materializer)
			require.NotNil(t, mark)
			require.Len(t, mark.Children, 2)
			require.NotEmpty(t, mark.OnList)

			materializerHasNullableOutput := false
			for _, output := range materializer.ProjectList {
				materializerHasNullableOutput =
					materializerHasNullableOutput || !output.Typ.NotNullable
			}
			if !tt.skipMaterializerContract {
				require.Equal(t, !tt.wantShuffle, materializerHasNullableOutput,
					"the materializer must expose the group expression's runtime NULL contract")
			}

			left := query.Nodes[mark.Children[0]]
			right := query.Nodes[mark.Children[1]]
			condition := mark.OnList[0].GetF()
			require.NotNil(t, condition)
			require.Len(t, condition.Args, 2)
			if tt.simulateStaleKeyType {
				for _, arg := range condition.Args {
					arg.Typ.NotNullable = true
				}
			}
			effectivelyNotNullable :=
				IsJoinExprProvenNotNullable(condition.Args[0], left, right) &&
					IsJoinExprProvenNotNullable(condition.Args[1], left, right)
			require.Equal(t, tt.wantShuffle, effectivelyNotNullable,
				"the compiler guard must observe the materialized output contract")

			left.Stats.Outcnt = 10_000_000
			right.Stats.Outcnt = 3_000_000
			mark.Stats.HashmapStats.HashmapSize = 3_000_000
			mark.OnList[0].Ndv = 100_000
			mark.Stats.HashmapStats.Shuffle = true

			builder := &QueryBuilder{qry: query}
			determineShuffleForJoinWithColRefMode(mark, builder, true)

			require.Equal(t, tt.wantShuffle, mark.Stats.HashmapStats.Shuffle)
			if tt.wantShuffle {
				require.Equal(t, int32(0), mark.Stats.HashmapStats.ShuffleColIdx)
			} else {
				require.Equal(t, int32(-1), mark.Stats.HashmapStats.ShuffleColIdx)
			}
		})
	}
}

func TestMarkShuffleUsesRecursiveCTEOutputNullability(t *testing.T) {
	for _, tt := range []struct {
		name        string
		sql         string
		wantShuffle bool
	}{
		{
			name: "nullable recursive member stays broadcast",
			sql: `
				with recursive c(k) as (
					select n.n_regionkey
					from tpch.nation n
					union all
					select cast(null as int)
					from c
					where c.k is not null
				)
				select c.k in (
					select l.l_partkey from tpch.lineitem l
				)
				from c`,
		},
		{
			name: "null-extended recursive seed stays broadcast",
			sql: `
				with recursive c(k) as (
					select n.n_regionkey
					from tpch.region r
					left join tpch.nation n
						on r.r_regionkey = n.n_regionkey
					union all
					select c.k
					from c
					where c.k < 0
				)
				select c.k in (
					select l.l_partkey from tpch.lineitem l
				)
				from c`,
		},
		{
			name: "nullable recursive dependency reaches fixed point",
			sql: `
				with recursive c(a, b) as (
					select n.n_regionkey, n.n_regionkey
					from tpch.nation n
					union all
					select cast(null as int), c.a + 1
					from c
					where c.b < 2
				)
				select c.b in (
					select l.l_partkey from tpch.lineitem l
				)
				from c`,
		},
		{
			name:        "non-null recursive contract remains eligible",
			wantShuffle: true,
			sql: `
				with recursive c(k) as (
					select n.n_regionkey
					from tpch.nation n
					union all
					select c.k
					from c
					where c.k < 0
				)
				select c.k in (
					select l.l_partkey from tpch.lineitem l
				)
				from c`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			preRemapBuilder, preRemapMark := buildMarkPlanBeforeOptimization(t, tt.sql)
			require.Len(t, preRemapMark.Children, 2)
			preRemapLeft := preRemapBuilder.qry.Nodes[preRemapMark.Children[0]]
			preRemapRight := preRemapBuilder.qry.Nodes[preRemapMark.Children[1]]
			preRemapLeft.Stats.Outcnt = 10_000_000
			preRemapRight.Stats.Outcnt = 3_000_000
			preRemapMark.Stats.HashmapStats.HashmapSize = 3_000_000
			preRemapMark.OnList[0].Ndv = 100_000
			preRemapMark.Stats.HashmapStats.Shuffle = true

			determineShuffleForJoinWithColRefMode(preRemapMark, preRemapBuilder, false)
			require.Equal(t, tt.wantShuffle, preRemapMark.Stats.HashmapStats.Shuffle)

			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, tt.sql)
			require.NoError(t, err)
			query := logicPlan.GetQuery()

			var mark *plan.Node
			for _, node := range query.Nodes {
				if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_MARK {
					mark = node
				}
			}
			require.NotNil(t, mark)
			require.Len(t, mark.Children, 2)
			left := query.Nodes[mark.Children[0]]
			right := query.Nodes[mark.Children[1]]
			condition := mark.OnList[0].GetF()
			require.NotNil(t, condition)
			require.Len(t, condition.Args, 2)
			effectivelyNotNullable :=
				IsJoinExprProvenNotNullable(condition.Args[0], left, right) &&
					IsJoinExprProvenNotNullable(condition.Args[1], left, right)
			require.Equal(t, tt.wantShuffle, effectivelyNotNullable)

			left.Stats.Outcnt = 10_000_000
			right.Stats.Outcnt = 3_000_000
			mark.Stats.HashmapStats.HashmapSize = 3_000_000
			mark.OnList[0].Ndv = 100_000
			mark.Stats.HashmapStats.Shuffle = true

			determineShuffleForJoinWithColRefMode(mark, &QueryBuilder{qry: query}, true)
			require.Equal(t, tt.wantShuffle, mark.Stats.HashmapStats.Shuffle)
			if tt.wantShuffle {
				require.Equal(t, int32(0), mark.Stats.HashmapStats.ShuffleColIdx)
			} else {
				require.Equal(t, int32(-1), mark.Stats.HashmapStats.ShuffleColIdx)
			}
		})
	}
}

func TestMarkShuffleUsesSetOperationOutputNullability(t *testing.T) {
	for _, tt := range []struct {
		name         string
		setClause    string
		firstExpr    string
		secondExpr   string
		trailingSet  string
		wantNodeType plan.Node_NodeType
		wantShuffle  bool
	}{
		{
			name:         "union all nullable second branch stays broadcast",
			setClause:    "union all",
			firstExpr:    "n.n_regionkey",
			secondExpr:   "cast(null as int)",
			wantNodeType: plan.Node_UNION_ALL,
		},
		{
			name:         "union nullable second branch stays broadcast",
			setClause:    "union",
			firstExpr:    "n.n_regionkey",
			secondExpr:   "cast(null as int)",
			wantNodeType: plan.Node_UNION,
		},
		{
			name:         "union all nullable first branch stays broadcast",
			setClause:    "union all",
			firstExpr:    "cast(null as int)",
			secondExpr:   "r.r_regionkey",
			wantNodeType: plan.Node_UNION_ALL,
		},
		{
			name:         "union all nullable middle branch stays broadcast",
			setClause:    "union all",
			firstExpr:    "n.n_regionkey",
			secondExpr:   "cast(null as int)",
			trailingSet:  "union all select r2.r_regionkey as regionkey from tpch.region r2",
			wantNodeType: plan.Node_UNION_ALL,
		},
		{
			name:         "union all non-null branches can shuffle",
			setClause:    "union all",
			firstExpr:    "n.n_regionkey",
			secondExpr:   "r.r_regionkey",
			wantNodeType: plan.Node_UNION_ALL,
			wantShuffle:  true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, fmt.Sprintf(`
				select d.regionkey in (
					select l.l_partkey from tpch.lineitem l
				)
				from (
					select %s as regionkey from tpch.nation n
					%s
					select %s as regionkey from tpch.region r
					%s
				) d`, tt.firstExpr, tt.setClause, tt.secondExpr, tt.trailingSet))
			require.NoError(t, err)

			query := logicPlan.GetQuery()
			require.NotNil(t, query)

			var setNode, mark *plan.Node
			for _, node := range query.Nodes {
				if node.NodeType == tt.wantNodeType {
					setNode = node
				}
				if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_MARK {
					mark = node
				}
			}
			require.NotNil(t, setNode)
			require.Len(t, setNode.ProjectList, 1)
			require.Equal(t, tt.wantShuffle, setNode.ProjectList[0].Typ.NotNullable)
			require.NotNil(t, mark)
			require.Len(t, mark.Children, 2)
			require.NotEmpty(t, mark.OnList)

			left := query.Nodes[mark.Children[0]]
			right := query.Nodes[mark.Children[1]]
			condition := mark.OnList[0].GetF()
			require.NotNil(t, condition)
			require.Len(t, condition.Args, 2)
			require.Equal(t, tt.wantShuffle,
				IsJoinExprProvenNotNullable(condition.Args[0], left, right) &&
					IsJoinExprProvenNotNullable(condition.Args[1], left, right),
				"compiler guard must observe the materialized set output contract")

			left.Stats.Outcnt = 10_000_000
			right.Stats.Outcnt = 3_000_000
			mark.Stats.HashmapStats.HashmapSize = 3_000_000
			mark.OnList[0].Ndv = 100_000
			mark.Stats.HashmapStats.Shuffle = true

			builder := &QueryBuilder{qry: query}
			determineShuffleForJoinWithColRefMode(mark, builder, true)

			require.Equal(t, tt.wantShuffle, mark.Stats.HashmapStats.Shuffle)
			if tt.wantShuffle {
				require.Equal(t, int32(0), mark.Stats.HashmapStats.ShuffleColIdx)
			} else {
				require.Equal(t, int32(-1), mark.Stats.HashmapStats.ShuffleColIdx)
			}
		})
	}
}

func TestUnionAllNullabilitySurvivesColumnPruning(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t, `
		select d.regionkey
		from (
			select n.n_nationkey as unused_key, n.n_regionkey as regionkey
			from tpch.nation n
			union all
			select r.r_regionkey as unused_key, cast(null as int) as regionkey
			from tpch.region r
		) d`)
	require.NoError(t, err)

	var unionAll *plan.Node
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == plan.Node_UNION_ALL {
			unionAll = node
		}
	}
	require.NotNil(t, unionAll)
	require.Len(t, unionAll.ProjectList, 1,
		"the unused leading set column should be pruned from both branches")
	require.False(t, unionAll.ProjectList[0].Typ.NotNullable,
		"the retained column must combine the corresponding post-prune child types")
}

func makeMarkShuffleEquality(t *testing.T, notNullable bool, leftRel, rightRel int32) *plan.Expr {
	t.Helper()

	typ := types.T_int64.ToType()
	equal, err := function.GetFunctionByName(context.Background(), "=", []types.Type{typ, typ})
	require.NoError(t, err)

	args := make([]*plan.Expr, 2)
	for i, relPos := range []int32{leftRel, rightRel} {
		args[i] = &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_int64), NotNullable: notNullable},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: relPos,
				ColPos: 0,
			}},
		}
	}
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool), NotNullable: notNullable},
		Ndv: 100_000,
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: equal.GetEncodedOverloadID(), ObjName: "="},
			Args: args,
		}},
	}
}

func makeMarkShuffleColumn(notNullable bool, relPos, colPos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64), NotNullable: notNullable},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: relPos,
			ColPos: colPos,
		}},
	}
}

func TestGetRangeShuffleIndexForZM(t *testing.T) {
	zm := index2.NewZM(types.T_datetime, 0)
	require.PanicsWithValue(t, "unsupported shuffle type!", func() {
		GetRangeShuffleIndexForZM(0, 1000, zm, 4)
	})
}

func TestDetermineShuffleTypeFallsBackWhenRangeStatsAreAbsent(t *testing.T) {
	builder := newStatsTestBuilderWithNDV("d", 1_000)
	node := &plan.Node{NodeType: plan.Node_PROJECT, Stats: DefaultStats()}
	determineShuffleType(&plan.ColRef{RelPos: 0, ColPos: 0, Name: "d"}, node, builder)
	require.Equal(t, plan.ShuffleType_Hash, node.Stats.HashmapStats.ShuffleType)
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
