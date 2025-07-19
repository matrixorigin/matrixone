// Copyright 2021-2024 Matrix Origin
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

package readutil

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestRemoteDataSource_ApplyTombstones(t *testing.T) {
	var rowIds []types.Rowid
	var pks []int32
	var committs []types.TS
	for i := 0; i < 100; i++ {
		row := types.RandomRowid()
		rowIds = append(rowIds, row)
		pks = append(pks, rand.Int31())
		committs = append(committs, types.TimestampToTS(timestamp.Timestamp{
			PhysicalTime: rand.Int63(),
			LogicalTime:  rand.Uint32(),
		}))
	}

	proc := testutil.NewProc(t)
	ctx := proc.Ctx
	proc.Base.FileService = testutil.NewSharedFS()

	bat := batch.NewWithSize(3)
	bat.Attrs = objectio.TombstoneAttrs_TN_Created
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_TS.ToType())

	for i := 0; i < len(rowIds)/2; i++ {
		require.NoError(t, vector.AppendFixed[types.Rowid](bat.Vecs[0], rowIds[i], false, proc.Mp()))
		require.NoError(t, vector.AppendFixed[int32](bat.Vecs[1], pks[i], false, proc.Mp()))
		require.NoError(t, vector.AppendFixed[types.TS](bat.Vecs[2], committs[i], false, proc.Mp()))
	}

	bat.SetRowCount(bat.Vecs[0].Length())

	writer := colexec.NewCNS3TombstoneWriter(proc.Mp(), proc.GetFileService(), types.T_int32.ToType())

	err := writer.Write(ctx, proc.Mp(), bat)
	require.NoError(t, err)

	ss, err := writer.Sync(ctx, proc.Mp())
	assert.Nil(t, err)
	require.Equal(t, 1, len(ss))
	require.Equal(t, len(rowIds)/2, int(ss[0].Rows()))

	tData := NewEmptyTombstoneData()
	for i := len(rowIds) / 2; i < len(rowIds)-1; i++ {
		require.NoError(t, tData.AppendInMemory(rowIds[i]))
	}

	require.NoError(t, tData.AppendFiles(ss[0]))

	relData := NewBlockListRelationData(0)
	require.NoError(t, relData.AttachTombstones(tData))

	ts := types.MaxTs()
	ds := NewRemoteDataSource(context.Background(), proc.GetFileService(), ts.ToTimestamp(), relData)

	bid, offset := rowIds[0].Decode()
	left, err := ds.ApplyTombstones(context.Background(), bid, []int64{int64(offset)}, engine.Policy_CheckAll)
	assert.Nil(t, err)

	require.Equal(t, 0, len(left))

	bid, offset = rowIds[len(rowIds)/2+1].Decode()
	left, err = ds.ApplyTombstones(context.Background(), bid, []int64{int64(offset)}, engine.Policy_CheckAll)
	require.Nil(t, err)
	require.Equal(t, 0, len(left))

	bid, offset = rowIds[len(rowIds)-1].Decode()
	left, err = ds.ApplyTombstones(context.Background(), bid, []int64{int64(offset)}, engine.Policy_CheckAll)
	require.Nil(t, err)
	require.Equal(t, 1, len(left))
}

func TestObjListRelData(t *testing.T) { // for test coverage
	objlistRelData := &ObjListRelData{
		NeedFirstEmpty: true,
		TotalBlocks:    1,
	}
	logutil.Infof("%v", objlistRelData.String())
	var s objectio.BlockInfoSlice
	s.AppendBlockInfo(&objectio.BlockInfo{BlockID: types.Blockid{1}})
	objlistRelData.AppendBlockInfoSlice(s)
	objlistRelData.SetBlockList(s)
	objlistRelData.AttachTombstones(nil)
	buf, err := objlistRelData.MarshalBinary()
	require.NoError(t, err)
	objlistRelData.UnmarshalBinary(buf)
}

func TestObjListRelData1(t *testing.T) {
	defer func() {
		r := recover()
		fmt.Println("panic recover", r)
	}()
	objlistRelData := &ObjListRelData{}
	objlistRelData.GetShardIDList()
}

func TestObjListRelData2(t *testing.T) {
	defer func() {
		r := recover()
		fmt.Println("panic recover", r)
	}()
	objlistRelData := &ObjListRelData{}
	objlistRelData.GetShardID(1)

}

func TestObjListRelData3(t *testing.T) {
	defer func() {
		r := recover()
		fmt.Println("panic recover", r)
	}()
	objlistRelData := &ObjListRelData{}
	objlistRelData.SetShardID(1, 1)
}

func TestObjListRelData4(t *testing.T) {
	defer func() {
		r := recover()
		fmt.Println("panic recover", r)
	}()
	objlistRelData := &ObjListRelData{}
	objlistRelData.AppendShardID(1)
}

func TestFastApplyDeletesByRowIds(t *testing.T) {
	rowIdStrs := []string{
		"0196a9cb-3fc6-7245-a9ad-51f37d9541cb-0-0-2900",
		"0196a9cb-4184-775b-a4cb-2047eace6e7c-0-0-1022",
		"0196a9cb-4184-775b-a4cb-2047eace6e7c-0-1-4345",
		"0196a9cb-44fd-7631-94e7-abbe8df59685-0-0-100",
		"0196a9cb-44fd-7631-94e7-abbe8df59685-0-1-302",
		"0196a9cc-7718-7810-8ac3-d6acbd662256-0-2-1231",
		"0196a9cc-7718-7810-8ac3-d6acbd662256-0-2-2834",
		"0196a9cd-1213-79cb-b81f-1b4a74f8b50a-0-0-6305",
		"0196a9cd-1213-79cb-b81f-1b4a74f8b50a-0-0-6994",
	}

	deletedRowIds := make([]types.Rowid, 0, len(rowIdStrs))
	for _, rowIdStr := range rowIdStrs {
		rowId, err := types.ParseRowIdFromString(rowIdStr)
		assert.Nil(t, err)
		deletedRowIds = append(deletedRowIds, *rowId)
	}

	checkRowIdStr := "0196a9cd-1213-79cb-b81f-1b4a74f8b50a-0-0-0"
	checkRowId, err := types.ParseRowIdFromString(checkRowIdStr)
	assert.Nil(t, err)

	checkBid := checkRowId.CloneBlockID()

	leftRows := []int64{
		3967, 3988, 4068, 4111, 4207, 4328,
		4515, 5007, 5051, 5492, 5777, 5988, 6273,
		6305, 6564, 7459, 7676, 7849,
	}

	sorted := slices.IsSortedFunc(deletedRowIds, func(a, b types.Rowid) int { return a.Compare(&b) })
	require.True(t, sorted)

	FastApplyDeletesByRowIds(&checkBid, &leftRows, nil, deletedRowIds, true)

	idx := slices.Index(leftRows, 6305)
	require.Equal(t, -1, idx)
}

func TestFastApplyDeletesByRowIds2(t *testing.T) {

	var rowStrs = []string{
		"0196ce31-83b4-7aef-bce6-c2861ca688e3-0-0-6652", "0196ce31-89c1-79de-be5a-28a83b64c136-0-0-6304", "0196ce31-8db9-7528-bd1e-06b0a425ddbe-0-1-292",
		"0196ce31-c69f-76e5-843d-ea5cf032dd73-0-0-4444", "0196ce31-ca5b-71e8-acd4-39420e2fda1b-0-0-5220", "0196ce31-cdd6-71e5-9816-76ffa6d037e8-0-0-6633",
		"0196ce31-d85b-7327-988c-bcf025bc44f7-0-0-1453", "0196ce31-da2b-7db8-b8f5-06b1f9bd1de0-0-0-3646", "0196ce3c-66a5-7f07-b77b-a14927b35f52-0-6-296",
		"0196ce3c-66a5-7f07-b77b-a14927b35f52-0-8-5297", "0196ce3c-66a5-7f07-b77b-a14927b35f52-0-8-7440", "0196ce3c-66a5-7f07-b77b-a14927b35f52-0-13-1227",
		"0196ce3c-66a5-7f07-b77b-a14927b35f52-0-13-7805", "0196ce3c-66a5-7f07-b77b-a14927b35f52-0-15-1508", "0196ce3c-66a5-7f07-b77b-a14927b35f52-0-15-8115",
		"0196ce3c-66a5-7f07-b77b-a14927b35f52-0-16-1723", "0196ce3c-66a5-7f07-b77b-a14927b35f52-0-16-1780", "0196ce3c-66a5-7f07-b77b-a14927b35f52-0-16-2923",
		"0196ce3c-66a5-7f07-b77b-a14927b35f52-0-16-4415", "0196ce3c-66a5-7f07-b77b-a14927b35f52-0-17-3895", "0196ce3c-66a5-7f07-b77b-a14927b35f52-0-17-4533",
		"0196ce3c-66a5-7f07-b77b-a14927b35f52-0-18-3725", "0196ce3c-66a5-7f07-b77b-a14927b35f52-0-21-5350", "0196ce3c-66a5-7f07-b77b-a14927b35f52-0-22-6284",
		"0196ce3c-66a5-7f07-b77b-a14927b35f52-1-0-1770", "0196ce3c-66a5-7f07-b77b-a14927b35f52-1-9-549", "0196ce3c-66a5-7f07-b77b-a14927b35f52-1-11-2219",
		"0196ce3c-66a5-7f07-b77b-a14927b35f52-1-11-3475", "0196ce3c-66a5-7f07-b77b-a14927b35f52-1-13-3167", "0196ce3c-66a5-7f07-b77b-a14927b35f52-1-15-1089",
		"0196ce3c-66a5-7f07-b77b-a14927b35f52-1-15-7896", "0196ce3c-66a5-7f07-b77b-a14927b35f52-1-16-216", "0196ce3c-dce0-7747-a936-951744b37cd8-0-1-6907",
		"0196ce3d-032b-7ee2-a4b7-705b4d32b5d9-0-2-4629", "0196ce3d-4059-72d7-996e-8181e40e40ca-0-0-1965", "0196ce3d-4059-72d7-996e-8181e40e40ca-0-1-3088",
		"0196ce3d-50d5-7668-a83a-4a41b2378690-0-0-3139", "0196ce3d-789d-75cc-af03-93a7d7b6f673-0-0-5517", "0196ce3d-789d-75cc-af03-93a7d7b6f673-0-2-4722",
		"0196ce3d-789d-75cc-af03-93a7d7b6f673-0-2-6054", "0196ce3d-9fa8-7dcf-8428-67ce2a210c1c-0-0-1057", "0196ce3d-9fa8-7dcf-8428-67ce2a210c1c-0-1-1172",
		"0196ce3d-c653-79bd-b5e9-1d8d535900fd-0-0-7973", "0196ce3d-c653-79bd-b5e9-1d8d535900fd-0-1-6284", "0196ce3d-ed52-78d9-9c27-cc700d1c3ce9-0-0-2721",
		"0196ce3d-ed52-78d9-9c27-cc700d1c3ce9-0-1-1558", "0196ce3d-ed52-78d9-9c27-cc700d1c3ce9-0-2-2733", "0196ce3e-2944-767d-baae-807fb26d187e-0-0-7479",
		"0196ce3e-4f07-7b9d-8276-76c11cead3e5-0-0-3250", "0196ce3e-4f07-7b9d-8276-76c11cead3e5-0-0-3268", "0196ce3e-4f07-7b9d-8276-76c11cead3e5-0-1-7663",
		"0196ce3e-4f07-7b9d-8276-76c11cead3e5-0-2-6890", "0196ce3e-5649-7cc8-a069-fa5b9ea88591-0-0-376", "0196ce3e-5649-7cc8-a069-fa5b9ea88591-0-0-2087",
		"0196ce3e-8a75-799f-aeba-1bc9ee2135db-0-0-4290", "0196ce3e-8a75-799f-aeba-1bc9ee2135db-0-1-5827", "0196ce3e-8a75-799f-aeba-1bc9ee2135db-0-1-6940",
		"0196ce3e-8a75-799f-aeba-1bc9ee2135db-0-3-4137", "0196ce3e-b0a0-74d9-8877-ac9ba80b5095-0-1-1426", "0196ce3e-b0a0-74d9-8877-ac9ba80b5095-0-1-3554",
		"0196ce3e-b0a0-74d9-8877-ac9ba80b5095-0-2-583", "0196ce3e-b0a0-74d9-8877-ac9ba80b5095-0-2-6446", "0196ce3e-d7b1-7d13-9b0f-9b6c99e42090-0-0-3467",
		"0196ce3e-d7b1-7d13-9b0f-9b6c99e42090-0-1-727", "0196ce3e-d7b1-7d13-9b0f-9b6c99e42090-0-1-913", "0196ce3e-d7b1-7d13-9b0f-9b6c99e42090-0-1-1303",
		"0196ce3e-d7b1-7d13-9b0f-9b6c99e42090-0-1-4806", "0196ce3e-d7b1-7d13-9b0f-9b6c99e42090-0-2-6359", "0196ce3e-d7b1-7d13-9b0f-9b6c99e42090-0-2-8050",
		"0196ce3e-ffc5-739a-8355-b8e70d168d38-0-0-6628", "0196ce3e-ffc5-739a-8355-b8e70d168d38-0-1-3572", "0196ce3e-ffc5-739a-8355-b8e70d168d38-0-1-5991",
		"0196ce3e-ffc5-739a-8355-b8e70d168d38-0-2-6508", "0196ce3f-2612-754c-8335-1a526fd00a59-0-1-2379", "0196ce3f-2612-754c-8335-1a526fd00a59-0-2-6420",
		"0196ce3f-2612-754c-8335-1a526fd00a59-0-2-7775", "0196ce3f-60a5-768f-9e67-1e18eacbb918-0-0-3733", "0196ce3f-60a5-768f-9e67-1e18eacbb918-0-0-6975",
		"0196ce3f-60a5-768f-9e67-1e18eacbb918-0-1-1168", "0196ce3f-60a5-768f-9e67-1e18eacbb918-0-1-2023", "0196ce3f-60a5-768f-9e67-1e18eacbb918-0-1-4751",
		"0196ce3f-60a5-768f-9e67-1e18eacbb918-0-1-5814", "0196ce3f-60a5-768f-9e67-1e18eacbb918-0-2-2615", "0196ce3f-60a5-768f-9e67-1e18eacbb918-0-2-4185",
		"0196ce3f-60a5-768f-9e67-1e18eacbb918-0-2-7743", "0196ce3f-60a5-768f-9e67-1e18eacbb918-0-3-398", "0196ce3f-60a5-768f-9e67-1e18eacbb918-0-3-1063",
		"0196ce3f-60a5-768f-9e67-1e18eacbb918-0-3-1806", "0196ce3f-60a5-768f-9e67-1e18eacbb918-0-3-4806", "0196ce3f-692a-7e0e-b1e5-bb777fc58672-0-0-1648",
		"0196ce3f-6a4a-789f-bec2-1b5688eda51f-0-0-1663", "0196ce3f-8793-7a9b-a384-3afe24029423-0-0-1544", "0196ce3f-8793-7a9b-a384-3afe24029423-0-0-4712",
		"0196ce3f-8793-7a9b-a384-3afe24029423-0-0-6285", "0196ce3f-8793-7a9b-a384-3afe24029423-0-1-4490", "0196ce3f-8793-7a9b-a384-3afe24029423-0-1-8056",
		"0196ce3f-8793-7a9b-a384-3afe24029423-0-2-2599", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-2-5035", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-0-3621",
		"0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-2-4669", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-3339", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-0-2713",
		"0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-8113", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-3719", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-2-7623",
		"0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-0-886", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-2-6583", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-1490",
		"0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-2-8047", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-73", "0196ce3f-aea0-7388-afcb-422369a1adae-0-0-821",
		"0196ce3f-aea0-7388-afcb-422369a1adae-0-0-1865", "0196ce3f-aea0-7388-afcb-422369a1adae-0-0-2561", "0196ce3f-aea0-7388-afcb-422369a1adae-0-0-2670",
		"0196ce3f-aea0-7388-afcb-422369a1adae-0-0-3363", "0196ce3f-aea0-7388-afcb-422369a1adae-0-0-3650", "0196ce3f-aea0-7388-afcb-422369a1adae-0-0-3757",
		"0196ce3f-aea0-7388-afcb-422369a1adae-0-0-4937", "0196ce3f-aea0-7388-afcb-422369a1adae-0-0-6361", "0196ce3f-aea0-7388-afcb-422369a1adae-0-1-590",
		"0196ce3f-aea0-7388-afcb-422369a1adae-0-1-7935", "0196ce3f-aea0-7388-afcb-422369a1adae-0-2-803", "0196ce3f-aea0-7388-afcb-422369a1adae-0-2-1541",
		"0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-0-3218", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-2-4236", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-2143",
		"0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-2-4893", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-2695", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-1580",
		"0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-2-5990", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-2-6571", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-2-970",
		"0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-0-6500", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-1797", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-4671",
		"0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-2-5412", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-2-5577", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-0-7814",
		"0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-1211", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-214", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-3199",
		"0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-0-3973", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-0-3366", "0196ce3f-d5ff-7bea-a650-f9b27cd16e9e-0-1-3491",
		"0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-567", "0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-1303", "0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-1662",
		"0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-1772", "0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-1904", "0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-1912",
		"0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-2815", "0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-4039", "0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-4649",
		"0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-4764", "0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-4913", "0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-5730",
		"0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-6439", "0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-6478", "0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-6527",
		"0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-7115", "0196ce3f-ce26-7a59-8d9f-9336cc0c797e-0-0-7234",
	}

	delRowIdVec := vector.NewVec(types.T_Rowid.ToType())

	for _, rowStr := range rowStrs {
		rowId, err := types.ParseRowIdFromString(rowStr)
		require.NoError(t, err)

		err = vector.AppendFixed[types.Rowid](delRowIdVec, *rowId, false, common.DefaultAllocator)
		require.NoError(t, err)
	}

	rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](delRowIdVec)

	checkRowIdStr := "0196ce3f-aea0-7388-afcb-422369a1adae-0-0-0"
	checkRowId, err := types.ParseRowIdFromString(checkRowIdStr)
	require.NoError(t, err)

	checkBid := checkRowId.CloneBlockID()
	offsets := []int64{719, 1302, 5218, 5619, 5738, 6361, 6394}

	checkRowId.SetRowOffset(6361)
	idx := slices.IndexFunc(rowIds, func(x types.Rowid) bool { return x.EQ(checkRowId) })
	require.NotEqual(t, -1, idx)

	{
		sorted := slices.IsSortedFunc(rowIds, func(a, b types.Rowid) int { return a.Compare(&b) })
		require.False(t, sorted)
	}

	{
		err = mergeutil.SortColumnsByIndex(
			[]*vector.Vector{delRowIdVec}, 0, common.DefaultAllocator)
		require.NoError(t, err)

		rowIds = vector.MustFixedColNoTypeCheck[types.Rowid](delRowIdVec)

		FastApplyDeletesByRowIds(&checkBid, &offsets, nil, rowIds, true)

		idx = slices.Index(offsets, 6361)
		require.Equal(t, -1, idx)
	}
}

func TestFastApplyDeletesByRowIdsRandom(t *testing.T) {
	// obj, bid, offset
	rootRowId := types.RandomRowid()

	const canLen = 1000
	var row types.Rowid
	var off int

	rowIds := make([]types.Rowid, 0, canLen)
	for i := 0; i < canLen; i++ {
		obj := rand.Intn(5) + 1
		blk := rand.Intn(5) + 1

		copy(row[:], rootRowId[:])
		row.SetObjOffset(uint16(obj))
		row.SetBlkOffset(uint16(blk))
		row.SetRowOffset(uint32(off))

		off++

		rowIds = append(rowIds, row)
	}

	slices.SortFunc(rowIds, func(a, b types.Rowid) int { return a.Compare(&b) })
	rowIds = slices.CompactFunc(rowIds, func(a types.Rowid, b types.Rowid) bool { return a.EQ(&b) })

	foo := func(leftRowsLen, deletedRowIdsLen int) {
		require.LessOrEqual(t, deletedRowIdsLen, len(rowIds))

		for i := 0; i < canLen/10; i++ {
			l := rand.Intn(len(rowIds) - deletedRowIdsLen)
			deletedRowIds := rowIds[l : l+deletedRowIdsLen]

			checkBid := deletedRowIds[rand.Intn(len(deletedRowIds))].CloneBlockID()

			leftRows := make([]int64, 0, leftRowsLen)
			for j := 0; j < deletedRowIdsLen; j++ {
				leftRows = append(leftRows, int64(deletedRowIds[j].GetRowOffset()))
			}

			slices.Sort(leftRows)

			if len(leftRows) >= leftRowsLen {
				leftRows = leftRows[:leftRowsLen]
			} else {
				s := leftRows[len(leftRows)-1]
				for j := leftRowsLen - len(leftRows); j >= 0; j-- {
					leftRows = append(leftRows, s+1)
					s++
				}
			}

			old := make([]int64, len(leftRows))
			copy(old, leftRows)

			sorted := slices.IsSortedFunc(deletedRowIds, func(a, b types.Rowid) int { return a.Compare(&b) })
			require.True(t, sorted)

			FastApplyDeletesByRowIds(&checkBid, &leftRows, nil, deletedRowIds, i%2 == 0)

			for j := range leftRows {
				x := types.NewRowid(&checkBid, uint32(leftRows[j]))
				idx := slices.IndexFunc(deletedRowIds, func(a types.Rowid) bool { return x.EQ(&a) })
				if idx != -1 {
					fmt.Println(x.String())
					fmt.Println(len(leftRows), leftRows)
					fmt.Println(len(old), old)
					fmt.Println()

					for k := range deletedRowIds {
						fmt.Println(k, deletedRowIds[k])
					}
				}
				require.Equal(t, -1, idx)
			}
		}
	}

	for i := 0; i < 20; i++ {
		x := rand.Intn(canLen/2) + 10
		y := rand.Intn(canLen/2) + 10

		foo(x, y)
	}
}

func TestFastApplyDeletesByRowOffsets(t *testing.T) {
	foo := func(leftRowsLen, offsetsLen int) {
		var leftRows []int64 = make([]int64, 0, leftRowsLen)
		var offsets []int64 = make([]int64, 0, offsetsLen)

		limit := max(leftRowsLen, offsetsLen)

		mm := make(map[int64]struct{})

		for range 10 {
			for range leftRowsLen {
				leftRows = append(leftRows, int64(rand.Intn(limit)))
			}

			for range offsetsLen {
				x := int64(rand.Intn(limit))
				mm[x] = struct{}{}
				offsets = append(offsets, x)
			}

			slices.Sort(leftRows)

			leftRows = slices.Compact(leftRows)

			ll := len(leftRows)
			for j := len(leftRows) - 1; j > 0; j-- {
				if leftRows[j] == 0 {
					ll--
				}
			}
			leftRows = leftRows[:ll]

			FastApplyDeletesByRowOffsets(&leftRows, nil, offsets)

			for j := range leftRows {
				_, ok := mm[leftRows[j]]
				require.False(t, ok, fmt.Sprintf("\nhit: %v\noffsets: %v\nleftRows: %v;", leftRows[j], offsets, leftRows))
			}
		}
	}

	foo(1, 1)
	foo(100, 100)
	foo(10, 300)
	foo(300, 10)
}
