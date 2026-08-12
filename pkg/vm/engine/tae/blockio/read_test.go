// Copyright 2021 Matrix Origin
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

package blockio

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/stretchr/testify/require"
)

type blockReadTestDataSource struct {
	deleted []uint64
	err     error
}

func (*blockReadTestDataSource) Next(
	context.Context,
	[]string,
	[]types.Type,
	[]uint16,
	int32,
	any,
	*mpool.MPool,
	*batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {
	return nil, engine.End, nil
}

func (d *blockReadTestDataSource) ApplyTombstones(
	_ context.Context,
	_ *objectio.Blockid,
	rows []int64,
	_ engine.TombstoneApplyPolicy,
) ([]int64, error) {
	deleted := make(map[int64]struct{}, len(d.deleted))
	for _, row := range d.deleted {
		deleted[int64(row)] = struct{}{}
	}
	ret := rows[:0]
	for _, row := range rows {
		if _, ok := deleted[row]; !ok {
			ret = append(ret, row)
		}
	}
	return ret, nil
}

func (d *blockReadTestDataSource) GetTombstones(
	context.Context,
	*objectio.Blockid,
) (objectio.Bitmap, error) {
	if d.err != nil {
		return objectio.NullBitmap, d.err
	}
	ret := objectio.GetReusableBitmap()
	for _, row := range d.deleted {
		ret.Add(row)
	}
	return ret, nil
}

func (*blockReadTestDataSource) SetOrderBy([]*plan.OrderBySpec)  {}
func (*blockReadTestDataSource) GetOrderBy() []*plan.OrderBySpec { return nil }
func (*blockReadTestDataSource) SetFilterZM(objectio.ZoneMap)    {}
func (*blockReadTestDataSource) Close()                          {}
func (*blockReadTestDataSource) String() string                  { return "block-read-test" }

func TestBlockDataReadInnerScopedMaterialization(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	typs := []types.Type{types.T_int32.ToType(), types.T_varchar.ToType()}

	writeMP := mpool.MustNewZero()
	input := batch.NewWithSize(len(typs))
	for i := range typs {
		input.Vecs[i] = vector.NewVec(typs[i])
	}
	for i := 0; i < 16; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i*10), false, writeMP))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("row-value-longer-than-inline-"+string(rune('a'+i))), false, writeMP))
	}
	input.SetRowCount(16)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1}, -1, false, false, fs)
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	info := stats.ConstructBlockInfo(0)
	input.Clean(writeMP)
	require.Zero(t, writeMP.CurrNB())
	mpool.DeleteMPool(writeMP)

	queryMP := mpool.MustNewZero()
	defer mpool.DeleteMPool(queryMP)
	columns := []uint16{0, objectio.SEQNUM_ROWID, 1}
	colTypes := []types.Type{typs[0], types.T_Rowid.ToType(), typs[1]}
	output := batch.NewWithSize(len(columns))
	for i := range colTypes {
		output.Vecs[i] = vector.NewOffHeapVecWithType(colTypes[i])
	}
	cacheVectors := containers.NewVectors(len(columns) + 1)
	stale := vector.NewOffHeapVecWithType(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(stale, int32(999), false, queryMP))
	cacheVectors[0] = *stale

	sels := []int64{1, 7, 15}
	require.NoError(t, BlockDataReadInner(
		ctx,
		&info,
		nil,
		columns,
		colTypes,
		1,
		types.TS{},
		sels,
		nil,
		fileservice.Policy(0),
		output,
		cacheVectors,
		queryMP,
		fs,
	))
	require.Zero(t, cacheVectors.Allocated())
	require.Equal(t, []int32{10, 70, 150}, vector.MustFixedColWithTypeCheck[int32](output.Vecs[0]))
	require.Equal(t, "row-value-longer-than-inline-b", output.Vecs[2].GetStringAt(0))
	require.Equal(t, "row-value-longer-than-inline-h", output.Vecs[2].GetStringAt(1))
	require.Equal(t, "row-value-longer-than-inline-p", output.Vecs[2].GetStringAt(2))
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](output.Vecs[1])
	for i, sel := range sels {
		require.Equal(t, uint32(sel), rowids[i].GetRowOffset())
	}

	output.Clean(queryMP)
	empty := batch.NewWithSize(len(columns))
	for i := range colTypes {
		empty.Vecs[i] = vector.NewOffHeapVecWithType(colTypes[i])
	}
	require.NoError(t, BlockDataReadInner(
		ctx,
		&info,
		nil,
		columns,
		colTypes,
		1,
		types.TS{},
		[]int64{},
		nil,
		fileservice.Policy(0),
		empty,
		cacheVectors,
		queryMP,
		fs,
	))
	for i := range empty.Vecs {
		require.Zero(t, empty.Vecs[i].Length())
	}
	empty.Clean(queryMP)

	full := batch.NewWithSize(len(columns))
	for i := range colTypes {
		full.Vecs[i] = vector.NewOffHeapVecWithType(colTypes[i])
	}
	require.NoError(t, BlockDataReadInner(
		ctx,
		&info,
		&blockReadTestDataSource{deleted: []uint64{2, 9}},
		columns,
		colTypes,
		1,
		types.TS{},
		nil,
		nil,
		fileservice.Policy(0),
		full,
		cacheVectors,
		queryMP,
		fs,
	))
	require.Equal(t, 14, full.Vecs[0].Length())
	require.Equal(
		t,
		[]int32{0, 10, 30, 40, 50, 60, 70, 80, 100, 110, 120, 130, 140, 150},
		vector.MustFixedColWithTypeCheck[int32](full.Vecs[0]),
	)
	fullRowids := vector.MustFixedColWithTypeCheck[types.Rowid](full.Vecs[1])
	for i, expected := range []uint32{0, 1, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15} {
		require.Equal(t, expected, fullRowids[i].GetRowOffset())
	}
	full.Clean(queryMP)

	errorOutput := batch.NewWithSize(len(columns))
	for i := range colTypes {
		errorOutput.Vecs[i] = vector.NewOffHeapVecWithType(colTypes[i])
	}
	err = BlockDataReadInner(
		ctx,
		&info,
		&blockReadTestDataSource{err: context.Canceled},
		columns,
		colTypes,
		1,
		types.TS{},
		nil,
		nil,
		fileservice.Policy(0),
		errorOutput,
		cacheVectors,
		queryMP,
		fs,
	)
	require.ErrorIs(t, err, context.Canceled)
	for i := range errorOutput.Vecs {
		require.Zero(t, errorOutput.Vecs[i].Length())
	}
	errorOutput.Clean(queryMP)

	// A legacy/non-appendable object mislabeled appendable has no hidden
	// commit-ts column. It must fail closed with an error rather than panic or
	// expose rows at an unknown snapshot.
	missingCommitInfo := info
	missingCommitInfo.ObjectFlags |= objectio.ObjectFlag_Appendable
	rowidOnly := batch.NewWithSize(1)
	rowidOnly.Vecs[0] = vector.NewOffHeapVecWithType(objectio.RowidType)
	require.Error(t, BlockDataReadInner(
		ctx,
		&missingCommitInfo,
		&blockReadTestDataSource{},
		[]uint16{objectio.SEQNUM_ROWID},
		[]types.Type{objectio.RowidType},
		0,
		types.BuildTS(7, 0),
		nil,
		nil,
		fileservice.Policy(0),
		rowidOnly,
		cacheVectors,
		queryMP,
		fs,
	))
	require.Zero(t, rowidOnly.Vecs[0].Length())
	rowidOnly.Clean(queryMP)
	require.Zero(t, queryMP.CurrNB())
}

func TestBlockDataReadWithFilterDefersPayload(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	colTypes := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}

	writeMP := mpool.MustNewZero()
	input := batch.NewWithSize(len(colTypes))
	for i := range colTypes {
		input.Vecs[i] = vector.NewVec(colTypes[i])
	}
	for i := 0; i < 8; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, writeMP))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("payload-"+string(rune('0'+i))), false, writeMP))
	}
	input.SetRowCount(8)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1}, -1, false, false, fs)
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats()
	info := stats.ConstructBlockInfo(0)
	input.Clean(writeMP)
	require.Zero(t, writeMP.CurrNB())
	mpool.DeleteMPool(writeMP)

	queryMP := mpool.MustNewZero()
	defer mpool.DeleteMPool(queryMP)
	output := batch.NewWithSize(len(colTypes))
	for i := range colTypes {
		output.Vecs[i] = vector.NewOffHeapVecWithType(colTypes[i])
	}
	cacheVectors := containers.NewVectors(len(colTypes) + 2)
	ds := &blockReadTestDataSource{deleted: []uint64{3}}

	read := func(filter engine.ReaderFilter) error {
		_, err := BlockDataReadWithFilter(
			ctx,
			&info,
			ds,
			[]uint16{0, 1},
			colTypes,
			-1,
			timestamp.Timestamp{},
			nil,
			nil,
			objectio.BlockReadFilter{},
			fileservice.Policy(0),
			"late-read-test",
			output,
			cacheVectors,
			queryMP,
			fs,
			[]int{0},
			filter,
		)
		return err
	}

	t.Run("partial survivors map through tombstones", func(t *testing.T) {
		require.NoError(t, read(func(bat *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
			require.Equal(t, []int{0}, loaded)
			require.Zero(t, bat.Vecs[1].Length(), "payload must not be loaded before filtering")
			values := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
			sels := make([]int64, 0, len(values))
			for i, value := range values {
				if value >= 5 {
					sels = append(sels, int64(i))
				}
			}
			bat.Vecs[0].Shrink(sels, false)
			bat.SetRowCount(len(sels))
			return engine.ReaderFilterResult{Sels: sels}, nil
		}))
		require.Equal(t, 3, output.RowCount())
		require.Equal(t, []int32{5, 6, 7}, vector.MustFixedColWithTypeCheck[int32](output.Vecs[0]))
		require.Equal(t, "payload-5", output.Vecs[1].GetStringAt(0))
		require.Equal(t, "payload-6", output.Vecs[1].GetStringAt(1))
		require.Equal(t, "payload-7", output.Vecs[1].GetStringAt(2))
	})

	t.Run("all survivors preserve tombstone mapping", func(t *testing.T) {
		output.CleanOnlyData()
		require.NoError(t, read(func(bat *batch.Batch, _ []int) (engine.ReaderFilterResult, error) {
			require.Zero(t, bat.Vecs[1].Length())
			return engine.ReaderFilterResult{All: true}, nil
		}))
		require.Equal(t, []int32{0, 1, 2, 4, 5, 6, 7}, vector.MustFixedColWithTypeCheck[int32](output.Vecs[0]))
		require.Equal(t, "payload-4", output.Vecs[1].GetStringAt(3))
	})

	t.Run("zero survivors never materialize payload", func(t *testing.T) {
		output.CleanOnlyData()
		require.NoError(t, read(func(bat *batch.Batch, _ []int) (engine.ReaderFilterResult, error) {
			require.Zero(t, bat.Vecs[1].Length())
			bat.Vecs[0].CleanOnlyData()
			bat.SetRowCount(0)
			return engine.ReaderFilterResult{}, nil
		}))
		require.Zero(t, output.RowCount())
		require.Zero(t, output.Vecs[0].Length())
		require.Zero(t, output.Vecs[1].Length())
	})

	t.Run("rejects inconsistent filtered vector lengths", func(t *testing.T) {
		output.CleanOnlyData()
		err := read(func(bat *batch.Batch, _ []int) (engine.ReaderFilterResult, error) {
			bat.SetRowCount(0)
			return engine.ReaderFilterResult{}, nil
		})
		require.ErrorContains(t, err, "left early column 0 with 7 rows")
	})

	t.Run("rejects mismatched column metadata", func(t *testing.T) {
		output.CleanOnlyData()
		_, err := BlockDataReadWithFilter(
			ctx,
			&info,
			ds,
			[]uint16{0, 1},
			colTypes[:1],
			-1,
			timestamp.Timestamp{},
			nil,
			nil,
			objectio.BlockReadFilter{},
			fileservice.Policy(0),
			"late-read-test",
			output,
			cacheVectors,
			queryMP,
			fs,
			[]int{0},
			func(*batch.Batch, []int) (engine.ReaderFilterResult, error) {
				return engine.ReaderFilterResult{All: true}, nil
			},
		)
		require.ErrorContains(t, err, "column count 2 does not match type count 1")
	})

	t.Run("storage selections compose with residual selections", func(t *testing.T) {
		output.CleanOnlyData()
		require.NoError(t, blockDataReadWithFilter(
			ctx,
			&info,
			nil,
			[]uint16{0, 1},
			colTypes,
			-1,
			types.TS{},
			[]int64{1, 4, 7},
			fileservice.Policy(0),
			output,
			cacheVectors,
			queryMP,
			fs,
			[]int{0},
			func(bat *batch.Batch, _ []int) (engine.ReaderFilterResult, error) {
				sels := []int64{1}
				bat.Vecs[0].Shrink(sels, false)
				bat.SetRowCount(1)
				return engine.ReaderFilterResult{Sels: sels}, nil
			},
			nil,
		))
		require.Equal(t, []int32{4}, vector.MustFixedColWithTypeCheck[int32](output.Vecs[0]))
		require.Equal(t, "payload-4", output.Vecs[1].GetStringAt(0))
	})

	output.Clean(queryMP)
	require.Zero(t, queryMP.CurrNB())
}

func TestBlockDataReadInnerAppendableVisibility(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	writeMP := mpool.MustNewZero()

	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	input.Vecs[1] = vector.NewVec(objectio.TSType)
	for i, physical := range []int64{2, 4, 6, 8, 10} {
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte{byte('k'), byte('0' + i)}, false, writeMP))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], types.BuildTS(physical, 0), false, writeMP))
	}
	input.SetRowCount(5)
	writer := ioutil.ConstructWriter(
		0,
		[]uint16{0, objectio.SEQNUM_COMMITTS},
		-1,
		false,
		false,
		fs,
	)
	writer.SetAppendable()
	_, err := writer.WriteBatch(input)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	stats := writer.GetObjectStats(objectio.WithAppendable())
	info := stats.ConstructBlockInfo(0)
	require.True(t, info.IsAppendable())
	input.Clean(writeMP)
	require.Zero(t, writeMP.CurrNB())
	mpool.DeleteMPool(writeMP)

	queryMP := mpool.MustNewZero()
	defer mpool.DeleteMPool(queryMP)
	cacheVectors := containers.NewVectors(3)
	search := objectio.NewReadFilterSearch(types.T_varchar, [][]byte{[]byte("k2")})
	matched, usable, _, err := ioutil.LoadColumnDataBySearchAndCheckTS(
		ctx,
		0,
		types.T_varchar.ToType(),
		fs,
		info.MetaLocation(),
		search,
		false,
		objectio.SEQNUM_COMMITTS,
		types.BuildTS(5, 0),
		types.BuildTS(7, 0),
		queryMP,
		fileservice.Policy(0),
	)
	require.NoError(t, err)
	require.True(t, usable)
	require.True(t, matched)

	pointSearch := objectio.NewReadFilterSearch(types.T_varchar, [][]byte{
		[]byte("k1"), []byte("k2"), []byte("k3"),
	})
	sels, err := ReadDataByFilter(
		ctx,
		"test",
		&info,
		&blockReadTestDataSource{deleted: []uint64{1}},
		[]uint16{0},
		[]types.Type{types.T_varchar.ToType()},
		types.BuildTS(7, 0),
		nil,
		pointSearch,
		false,
		cacheVectors,
		queryMP,
		fs,
	)
	require.NoError(t, err)
	require.Equal(t, []int64{2}, sels)
	selected := batch.NewWithSize(2)
	selected.Vecs[0] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	selected.Vecs[1] = vector.NewOffHeapVecWithType(objectio.RowidType)
	require.NoError(t, BlockDataReadInner(
		ctx,
		&info,
		&blockReadTestDataSource{},
		[]uint16{0, objectio.SEQNUM_ROWID},
		[]types.Type{types.T_varchar.ToType(), objectio.RowidType},
		1,
		types.BuildTS(7, 0),
		sels,
		nil,
		fileservice.Policy(0),
		selected,
		cacheVectors,
		queryMP,
		fs,
	))
	require.Equal(t, "k2", selected.Vecs[0].GetStringAt(0))
	selectedRowid := vector.GetFixedAtNoTypeCheck[types.Rowid](selected.Vecs[1], 0)
	require.Equal(
		t,
		uint32(2),
		selectedRowid.GetRowOffset(),
	)
	selected.Clean(queryMP)

	output := batch.NewWithSize(2)
	output.Vecs[0] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	output.Vecs[1] = vector.NewOffHeapVecWithType(objectio.RowidType)
	require.NoError(t, BlockDataReadInner(
		ctx,
		&info,
		&blockReadTestDataSource{deleted: []uint64{1}},
		[]uint16{0, objectio.SEQNUM_ROWID},
		[]types.Type{types.T_varchar.ToType(), objectio.RowidType},
		1,
		types.BuildTS(7, 0),
		nil,
		nil,
		fileservice.Policy(0),
		output,
		cacheVectors,
		queryMP,
		fs,
	))
	require.Equal(t, []string{"k0", "k2"}, []string{output.Vecs[0].GetStringAt(0), output.Vecs[0].GetStringAt(1)})
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](output.Vecs[1])
	require.Equal(t, uint32(0), rowids[0].GetRowOffset())
	require.Equal(t, uint32(2), rowids[1].GetRowOffset())
	output.Clean(queryMP)

	lateOutput := batch.NewWithSize(2)
	lateOutput.Vecs[0] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	lateOutput.Vecs[1] = vector.NewOffHeapVecWithType(objectio.RowidType)
	_, err = BlockDataReadWithFilter(
		ctx,
		&info,
		&blockReadTestDataSource{deleted: []uint64{1}},
		[]uint16{0, objectio.SEQNUM_ROWID},
		[]types.Type{types.T_varchar.ToType(), objectio.RowidType},
		1,
		timestamp.Timestamp{PhysicalTime: 7},
		nil,
		nil,
		objectio.BlockReadFilter{},
		fileservice.Policy(0),
		"appendable-late-read-test",
		lateOutput,
		cacheVectors,
		queryMP,
		fs,
		[]int{1},
		func(bat *batch.Batch, _ []int) (engine.ReaderFilterResult, error) {
			require.Zero(t, bat.Vecs[0].Length(), "payload must remain deferred")
			visibleRowids := vector.MustFixedColWithTypeCheck[types.Rowid](bat.Vecs[1])
			require.Equal(t, []uint32{0, 2}, []uint32{
				visibleRowids[0].GetRowOffset(),
				visibleRowids[1].GetRowOffset(),
			})
			sels := []int64{1}
			bat.Vecs[1].Shrink(sels, false)
			bat.SetRowCount(1)
			return engine.ReaderFilterResult{Sels: sels}, nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, "k2", lateOutput.Vecs[0].GetStringAt(0))
	lateRowid := vector.GetFixedAtNoTypeCheck[types.Rowid](lateOutput.Vecs[1], 0)
	require.Equal(t, uint32(2), lateRowid.GetRowOffset())
	lateOutput.Clean(queryMP)

	rowidOnly := batch.NewWithSize(1)
	rowidOnly.Vecs[0] = vector.NewOffHeapVecWithType(objectio.RowidType)
	require.NoError(t, BlockDataReadInner(
		ctx,
		&info,
		&blockReadTestDataSource{},
		[]uint16{objectio.SEQNUM_ROWID},
		[]types.Type{objectio.RowidType},
		0,
		types.BuildTS(7, 0),
		nil,
		nil,
		fileservice.Policy(0),
		rowidOnly,
		cacheVectors,
		queryMP,
		fs,
	))
	rowids = vector.MustFixedColWithTypeCheck[types.Rowid](rowidOnly.Vecs[0])
	require.Len(t, rowids, 3)
	for i := range rowids {
		require.Equal(t, uint32(i), rowids[i].GetRowOffset())
	}
	rowidOnly.Clean(queryMP)
	require.Zero(t, queryMP.CurrNB())
}

func TestBlockDataReadBackupCombinesAbortAndTombstoneMasks(t *testing.T) {
	for _, test := range []struct {
		name     string
		seqnums  []uint16
		commitTS []types.TS
		aborts   []bool
	}{
		{
			name:     "v10-abort-column",
			seqnums:  []uint16{0, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT},
			commitTS: []types.TS{types.BuildTS(1, 0), types.BuildTS(1, 0), types.BuildTS(1, 0), types.BuildTS(1, 0)},
			aborts:   []bool{false, true, false, false},
		},
		{
			name:     "v9-uncommitted-sentinel",
			seqnums:  []uint16{0, objectio.SEQNUM_COMMITTS},
			commitTS: []types.TS{types.BuildTS(1, 0), txnif.UncommitTS, types.BuildTS(1, 0), types.BuildTS(1, 0)},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			fs := testutil.NewSharedFS()
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)

			input := batch.NewWithSize(len(test.seqnums))
			input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			input.Vecs[1] = vector.NewVec(objectio.TSType)
			if len(test.aborts) > 0 {
				input.Vecs[2] = vector.NewVec(types.T_bool.ToType())
			}
			for row := range 4 {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(row), false, mp))
				require.NoError(t, vector.AppendFixed(input.Vecs[1], test.commitTS[row], false, mp))
				if len(test.aborts) > 0 {
					require.NoError(t, vector.AppendFixed(input.Vecs[2], test.aborts[row], false, mp))
				}
			}
			input.SetRowCount(4)
			writer := ioutil.ConstructWriter(0, test.seqnums, -1, false, false, fs)
			writer.SetAppendable()
			_, err := writer.WriteBatch(input)
			require.NoError(t, err)
			_, _, err = writer.Sync(ctx)
			require.NoError(t, err)
			stats := writer.GetObjectStats(objectio.WithAppendable())
			info := stats.ConstructBlockInfo(0)
			input.Clean(mp)

			for _, idxes := range [][]uint16{nil, {0}} {
				for _, cutoff := range []types.TS{types.BuildTS(2, 0), {}} {
					loaded, _, err := BlockDataReadBackup(
						ctx,
						&info,
						&blockReadTestDataSource{deleted: []uint64{2}},
						idxes,
						cutoff,
						fs,
					)
					require.NoError(t, err)
					if len(idxes) > 0 {
						require.Len(t, loaded.Vecs, len(idxes))
					}
					require.Equal(t, []int32{0, 3}, vector.MustFixedColWithTypeCheck[int32](loaded.Vecs[0]))
					loaded.Clean(common.DebugAllocator)
				}
			}
		})
	}
}

func TestFillOutputBatchBySelectedRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	// Test case 1: Basic case without phyAddr and without orderByLimit
	t.Run("basic_no_phyaddr_no_orderbylimit", func(t *testing.T) {
		vec0 := vector.NewVec(types.T_int32.ToType())
		vec1 := vector.NewVec(types.T_varchar.ToType())

		for i := 0; i < 10; i++ {
			vector.AppendFixed(vec0, int32(i), false, mp)
			vector.AppendBytes(vec1, []byte("test"), false, mp)
		}

		cacheVectors := make(containers.Vectors, 2)
		cacheVectors[0] = *vec0
		cacheVectors[1] = *vec1

		outputBat := batch.NewWithSize(2)
		outputBat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		outputBat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())

		selectRows := []int64{1, 3, 5}
		columns := []uint16{0, 1}
		info := &objectio.BlockInfo{}

		err := fillOutputBatchBySelectedRows(
			info, columns, -1, outputBat, cacheVectors, selectRows, nil, nil, mp,
		)

		require.NoError(t, err)
		require.Equal(t, 3, outputBat.Vecs[0].Length())
		require.Equal(t, 3, outputBat.Vecs[1].Length())
	})

	// Test case 2: With orderByLimit (distVec needs to be appended)
	t.Run("with_orderbylimit", func(t *testing.T) {
		vec0 := vector.NewVec(types.T_int32.ToType())
		vec1 := vector.NewVec(types.T_array_float32.ToType())

		for i := 0; i < 10; i++ {
			vector.AppendFixed(vec0, int32(i), false, mp)
			vector.AppendBytes(vec1, types.ArrayToBytes[float32]([]float32{0.1, 0.2}), false, mp)
		}

		cacheVectors := make(containers.Vectors, 2)
		cacheVectors[0] = *vec0
		cacheVectors[1] = *vec1

		outputBat := batch.NewWithSize(2)
		outputBat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		outputBat.Vecs[1] = vector.NewVec(types.T_array_float32.ToType())

		selectRows := []int64{1, 3}
		columns := []uint16{0, 1}
		dists := []float64{0.5, 0.8}

		orderByLimit := &objectio.IndexReaderTopOp{ColPos: 1, Limit: 2}
		info := &objectio.BlockInfo{}

		err := fillOutputBatchBySelectedRows(
			info, columns, -1, outputBat, cacheVectors, selectRows, orderByLimit, dists, mp,
		)

		require.NoError(t, err)
		require.Equal(t, 2, outputBat.Vecs[0].Length())
		require.Equal(t, 3, len(outputBat.Vecs))
		require.Equal(t, 2, outputBat.Vecs[2].Length())
	})

	// Test case 3: Empty selectRows with phyAddr column
	t.Run("empty_selectrows_with_phyaddr", func(t *testing.T) {
		vec0 := vector.NewVec(types.T_int32.ToType())
		for i := 0; i < 10; i++ {
			vector.AppendFixed(vec0, int32(i), false, mp)
		}

		cacheVectors := make(containers.Vectors, 1)
		cacheVectors[0] = *vec0

		outputBat := batch.NewWithSize(2)
		outputBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		outputBat.Vecs[1] = vector.NewVec(types.T_int32.ToType())

		selectRows := []int64{}
		columns := []uint16{0, 1}
		info := &objectio.BlockInfo{}

		err := fillOutputBatchBySelectedRows(
			info, columns, 0, outputBat, cacheVectors, selectRows, nil, nil, mp,
		)

		require.NoError(t, err)
		require.Equal(t, 0, outputBat.Vecs[0].Length())
	})

	// Test case 4: With orderByLimit and distVec already exists
	t.Run("with_orderbylimit_distvec_exists", func(t *testing.T) {
		vec0 := vector.NewVec(types.T_int32.ToType())
		vec1 := vector.NewVec(types.T_array_float32.ToType())

		for i := 0; i < 10; i++ {
			vector.AppendFixed(vec0, int32(i), false, mp)
			vector.AppendBytes(vec1, types.ArrayToBytes[float32]([]float32{0.1, 0.2}), false, mp)
		}

		cacheVectors := make(containers.Vectors, 2)
		cacheVectors[0] = *vec0
		cacheVectors[1] = *vec1

		outputBat := batch.NewWithSize(3)
		outputBat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		outputBat.Vecs[1] = vector.NewVec(types.T_array_float32.ToType())
		outputBat.Vecs[2] = vector.NewVec(types.T_float64.ToType())

		selectRows := []int64{1, 3}
		columns := []uint16{0, 1}
		dists := []float64{0.5, 0.8}

		orderByLimit := &objectio.IndexReaderTopOp{ColPos: 1, Limit: 2}
		info := &objectio.BlockInfo{}

		err := fillOutputBatchBySelectedRows(
			info, columns, -1, outputBat, cacheVectors, selectRows, orderByLimit, dists, mp,
		)

		require.NoError(t, err)
		require.Equal(t, 2, outputBat.Vecs[0].Length())
		require.Equal(t, 3, len(outputBat.Vecs))
		require.Equal(t, 2, outputBat.Vecs[2].Length())
	})

	// Test case 5: With phyAddr column and non-empty selectRows
	t.Run("with_phyaddr_and_selectrows", func(t *testing.T) {
		vec0 := vector.NewVec(types.T_int32.ToType())
		for i := 0; i < 10; i++ {
			vector.AppendFixed(vec0, int32(i*10), false, mp)
		}

		cacheVectors := make(containers.Vectors, 1)
		cacheVectors[0] = *vec0

		outputBat := batch.NewWithSize(2)
		outputBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		outputBat.Vecs[1] = vector.NewVec(types.T_int32.ToType())

		selectRows := []int64{2, 5, 7}
		columns := []uint16{0, 1}

		info := &objectio.BlockInfo{}

		err := fillOutputBatchBySelectedRows(
			info, columns, 0, outputBat, cacheVectors, selectRows, nil, nil, mp,
		)

		require.NoError(t, err)
		require.Equal(t, 3, outputBat.Vecs[0].Length())
		require.Equal(t, 3, outputBat.Vecs[1].Length())
	})
}

func TestHandleOrderByLimitOnSelectRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ctx := context.Background()

	vec0 := vector.NewVec(types.T_int32.ToType())
	vec1 := vector.NewVec(types.T_array_float32.ToType())

	for i := 0; i < 3; i++ {
		vector.AppendFixed(vec0, int32(i), false, mp)
	}

	vector.AppendBytes(vec1, types.ArrayToBytes[float32]([]float32{1.0, 1.0}), false, mp) // dist: 2
	vector.AppendBytes(vec1, types.ArrayToBytes[float32]([]float32{0.1, 0.2}), false, mp) // dist: 0.05
	vector.AppendBytes(vec1, types.ArrayToBytes[float32]([]float32{0.5, 0.5}), false, mp) // dist: 0.5

	cacheVectors := make(containers.Vectors, 2)
	cacheVectors[0] = *vec0
	cacheVectors[1] = *vec1

	selectRows := []int64{0, 1, 2}

	orderByLimit := &objectio.IndexReaderTopOp{
		ColPos:     1,
		Limit:      2,
		Typ:        types.T_array_float32,
		NumVec:     types.ArrayToBytes[float32]([]float32{0.0, 0.0}),
		MetricType: metric.Metric_L2Distance,
		DistHeap:   make(objectio.Float64Heap, 0, 2),
	}

	resSels, resDists, err := handleOrderByLimitOnSelectRows(ctx, selectRows, orderByLimit, nil, -1, cacheVectors)
	require.NoError(t, err)
	require.Equal(t, 2, len(resSels))
	require.Equal(t, 2, len(resDists))

	// Closest should be index 1 (0.1, 0.2), then index 2 (0.5, 0.5)
	require.Equal(t, int64(1), resSels[0])
	require.Equal(t, int64(2), resSels[1])
}

// TestTopInputRowsConstruction tests the code section at lines 674-684
// This tests the construction of topInputRows by filtering out deleted rows
func TestBuildTopInputRows(t *testing.T) {
	// empty deleteMask returns nil
	t.Run("empty_mask", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		require.Nil(t, buildTopInputRows(100, mask))
	})

	// all rows deleted returns empty slice
	t.Run("all_deleted", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		for i := 0; i < 10; i++ {
			mask.Add(uint64(i))
		}
		rows := buildTopInputRows(10, mask)
		require.NotNil(t, rows)
		require.Equal(t, 0, len(rows))
		require.Equal(t, 0, cap(rows))
	})

	// some rows deleted
	t.Run("some_deleted", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		mask.Add(1)
		mask.Add(3)
		mask.Add(5)
		mask.Add(7)
		rows := buildTopInputRows(10, mask)
		require.Equal(t, []int64{0, 2, 4, 6, 8, 9}, rows)
		require.Equal(t, 6, cap(rows))
	})

	// capHint clamped to 0 when all deleted
	t.Run("caphint_clamped", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		for i := 0; i < 5; i++ {
			mask.Add(uint64(i))
		}
		rows := buildTopInputRows(5, mask)
		require.NotNil(t, rows)
		require.Equal(t, 0, len(rows))
	})

	// single row not deleted -> empty mask -> nil
	t.Run("single_row_no_delete", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		require.Nil(t, buildTopInputRows(1, mask))
	})

	// single row deleted
	t.Run("single_row_deleted", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		mask.Add(0)
		rows := buildTopInputRows(1, mask)
		require.NotNil(t, rows)
		require.Equal(t, 0, len(rows))
	})

	// large sparse deletes
	t.Run("large_sparse", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		for i := 0; i < 1000; i += 10 {
			mask.Add(uint64(i))
		}
		rows := buildTopInputRows(1000, mask)
		require.Equal(t, 900, len(rows))
		require.Equal(t, 900, cap(rows))
		for _, r := range rows {
			require.False(t, mask.Contains(uint64(r)))
		}
	})

	// deletes at start
	t.Run("deletes_at_start", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		for i := 0; i < 5; i++ {
			mask.Add(uint64(i))
		}
		rows := buildTopInputRows(20, mask)
		require.Equal(t, 15, len(rows))
		require.Equal(t, int64(5), rows[0])
	})

	// deletes at end
	t.Run("deletes_at_end", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		for i := 15; i < 20; i++ {
			mask.Add(uint64(i))
		}
		rows := buildTopInputRows(20, mask)
		require.Equal(t, 15, len(rows))
		require.Equal(t, int64(14), rows[14])
	})

	// alternating deletes
	t.Run("alternating", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		for i := 0; i < 20; i += 2 {
			mask.Add(uint64(i))
		}
		rows := buildTopInputRows(20, mask)
		require.Equal(t, 10, len(rows))
		for _, r := range rows {
			require.Equal(t, int64(1), r%2)
		}
	})

	// length 0
	t.Run("zero_length", func(t *testing.T) {
		mask := objectio.GetReusableBitmap()
		defer mask.Release()
		mask.Add(0)
		rows := buildTopInputRows(0, mask)
		require.Nil(t, rows)
	})
}

func TestShouldFallbackOrderedLimitToFullBlockRead(t *testing.T) {
	sortedInfo := &objectio.BlockInfo{ObjectFlags: objectio.ObjectFlag_Sorted}
	unsortedInfo := &objectio.BlockInfo{}

	require.False(t, shouldFallbackOrderedLimitToFullBlockRead(nil, sortedInfo))
	require.False(t, shouldFallbackOrderedLimitToFullBlockRead(
		&objectio.IndexReaderTopOp{Limit: 2},
		unsortedInfo,
	))
	require.False(t, shouldFallbackOrderedLimitToFullBlockRead(
		&objectio.IndexReaderTopOp{Limit: 2, OrderedLimit: true},
		sortedInfo,
	))
	require.True(t, shouldFallbackOrderedLimitToFullBlockRead(
		&objectio.IndexReaderTopOp{Limit: 2, OrderedLimit: true},
		unsortedInfo,
	))
	require.True(t, shouldFallbackOrderedLimitToFullBlockRead(
		&objectio.IndexReaderTopOp{Limit: 2, OrderedLimit: true},
		nil,
	))
}

// TestHandleOrderByLimitAllNullVectors verifies that HandleOrderByLimitOnIVFFlatIndex
// returns empty sels/dists when all vector rows are NULL.
// This is the root cause of the IVF-Flat entries table panic: when the InMem
// path gets all-NULL vectors, empty sels caused Shuffle to be a no-op, leaving
// the batch with a stale row count while the distance vector had 0 elements.
func TestHandleOrderByLimitAllNullVectors(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ctx := context.Background()

	// Create a vector column where ALL rows are NULL.
	vecCol := vector.NewVec(types.T_array_float32.ToType())
	for i := 0; i < 5; i++ {
		vector.AppendBytes(vecCol, nil, true, mp) // null = true
	}

	orderByLimit := &objectio.IndexReaderTopOp{
		ColPos:     0,
		Limit:      2,
		Typ:        types.T_array_float32,
		NumVec:     types.ArrayToBytes[float32]([]float32{0.0, 0.0}),
		MetricType: metric.Metric_L2Distance,
	}

	sels, dists, err := HandleOrderByLimitOnIVFFlatIndex(ctx, nil, vecCol, orderByLimit)
	require.NoError(t, err)
	require.Empty(t, sels, "sels should be empty when all vectors are NULL")
	require.Empty(t, dists, "dists should be empty when all vectors are NULL")

	// Verify that Shuffle with empty sels is a no-op (the bug scenario).
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 5; i++ {
		vector.AppendFixed(bat.Vecs[0], int32(i), false, mp)
	}
	bat.SetRowCount(5)

	// Shuffle with empty sels does nothing — batch retains row count.
	err = bat.Shuffle(sels, mp)
	require.NoError(t, err)
	require.Equal(t, 5, bat.RowCount(), "Shuffle with empty sels should NOT reset row count")

	// The fix: caller must explicitly set row count to 0 when sels is empty.
	if len(sels) == 0 {
		bat.SetRowCount(0)
	}
	require.Equal(t, 0, bat.RowCount())
}

func TestHandleOrderByLimitOnSelectRowsRejectsOverflowLimit(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	vec := vector.NewVec(types.T_array_float32.ToType())
	vector.AppendBytes(vec, types.ArrayToBytes[float32]([]float32{0.1, 0.2}), false, mp)

	cacheVectors := make(containers.Vectors, 1)
	cacheVectors[0] = *vec

	orderByLimit := &objectio.IndexReaderTopOp{
		ColPos:     0,
		Limit:      ^uint64(0),
		Typ:        types.T_array_float32,
		NumVec:     types.ArrayToBytes[float32]([]float32{0.0, 0.0}),
		MetricType: metric.Metric_L2Distance,
	}

	_, _, err := handleOrderByLimitOnSelectRows(context.Background(), []int64{0}, orderByLimit, nil, -1, cacheVectors)
	require.Error(t, err)
	require.Contains(t, err.Error(), "overflows int")
}

func TestHandleOrderByLimitOnSelectRowsForOrderedLimit(t *testing.T) {
	ctx := context.Background()
	selectRows := []int64{2, 4, 6, 8}
	info := &objectio.BlockInfo{ObjectFlags: objectio.ObjectFlag_Sorted}

	descLimit := &objectio.IndexReaderTopOp{Limit: 2, OrderedLimit: true, Desc: true}
	descRows, descDists, err := handleOrderByLimitOnSelectRows(ctx, selectRows, descLimit, info, -1, nil)
	require.NoError(t, err)
	require.Nil(t, descDists)
	require.Equal(t, []int64{6, 8}, descRows)

	ascLimit := &objectio.IndexReaderTopOp{Limit: 2, OrderedLimit: true}
	ascRows, ascDists, err := handleOrderByLimitOnSelectRows(ctx, selectRows, ascLimit, info, -1, nil)
	require.NoError(t, err)
	require.Nil(t, ascDists)
	require.Equal(t, []int64{2, 4}, ascRows)
}

func TestHandleOrderByLimitOnLiveRowsForOrderedLimit(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	vec0 := vector.NewVec(types.T_int32.ToType())
	for i := 0; i < 6; i++ {
		vector.AppendFixed(vec0, int32(i), false, mp)
	}
	cacheVectors := make(containers.Vectors, 1)
	cacheVectors[0] = *vec0

	descLimit := &objectio.IndexReaderTopOp{
		Typ:          types.T_int32,
		ColPos:       0,
		Limit:        2,
		OrderedLimit: true,
		Desc:         true,
	}
	info := &objectio.BlockInfo{ObjectFlags: objectio.ObjectFlag_Sorted}

	rows, dists, err := handleOrderByLimitOnLiveRows(context.Background(), descLimit, info, -1, objectio.Bitmap{}, cacheVectors)
	require.NoError(t, err)
	require.Nil(t, dists)
	require.Equal(t, []int64{4, 5}, rows)

	ascLimit := &objectio.IndexReaderTopOp{
		Typ:          types.T_int32,
		ColPos:       0,
		Limit:        2,
		OrderedLimit: true,
	}
	rows, dists, err = handleOrderByLimitOnLiveRows(context.Background(), ascLimit, info, -1, objectio.Bitmap{}, cacheVectors)
	require.NoError(t, err)
	require.Nil(t, dists)
	require.Equal(t, []int64{0, 1}, rows)

	deleteMask := objectio.GetReusableBitmap()
	defer deleteMask.Release()
	deleteMask.Add(4)
	rows, dists, err = handleOrderByLimitOnLiveRows(context.Background(), descLimit, info, -1, deleteMask, cacheVectors)
	require.NoError(t, err)
	require.Nil(t, dists)
	require.Equal(t, []int64{3, 5}, rows)

	deleteMask.Add(0)
	rows, dists, err = handleOrderByLimitOnLiveRows(context.Background(), ascLimit, info, -1, deleteMask, cacheVectors)
	require.NoError(t, err)
	require.Nil(t, dists)
	require.Equal(t, []int64{1, 2}, rows)

	unsortedInfo := &objectio.BlockInfo{}
	rows, dists, err = handleOrderByLimitOnLiveRows(context.Background(), ascLimit, unsortedInfo, -1, deleteMask, cacheVectors)
	require.NoError(t, err)
	require.Nil(t, dists)
	require.Equal(t, []int64{1, 2, 3, 5}, rows)
}

// TestHandleOrderByLimitOnSelectRows_Narrow exercises the narrow (bf16/f16/int8)
// branch of the optimized vector top-k scan (the merged distOf path). Same data
// in each type: rows [10,10],[1,2],[5,5] vs query [0,0] -> dists 200,5,50, so the
// top-2 are row 1 then row 2.
func TestHandleOrderByLimitOnSelectRows_Narrow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	ctx := context.Background()

	cases := []struct {
		name string
		oid  types.T
		rows [][]byte
		num  []byte
	}{
		{"int8", types.T_array_int8, [][]byte{
			types.ArrayToBytes([]int8{10, 10}),
			types.ArrayToBytes([]int8{1, 2}),
			types.ArrayToBytes([]int8{5, 5}),
		}, types.ArrayToBytes([]int8{0, 0})},
		{"bf16", types.T_array_bf16, [][]byte{
			types.ArrayToBytes(types.Float32ToBF16Slice([]float32{10, 10})),
			types.ArrayToBytes(types.Float32ToBF16Slice([]float32{1, 2})),
			types.ArrayToBytes(types.Float32ToBF16Slice([]float32{5, 5})),
		}, types.ArrayToBytes(types.Float32ToBF16Slice([]float32{0, 0}))},
		{"f16", types.T_array_float16, [][]byte{
			types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{10, 10})),
			types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{1, 2})),
			types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{5, 5})),
		}, types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{0, 0}))},
	}

	for _, c := range cases {
		vec0 := vector.NewVec(types.T_int32.ToType())
		vec1 := vector.NewVec(c.oid.ToType())
		for i := 0; i < 3; i++ {
			vector.AppendFixed(vec0, int32(i), false, mp)
		}
		for _, b := range c.rows {
			vector.AppendBytes(vec1, b, false, mp)
		}
		cacheVectors := make(containers.Vectors, 2)
		cacheVectors[0] = *vec0
		cacheVectors[1] = *vec1

		orderByLimit := &objectio.IndexReaderTopOp{
			ColPos:     1,
			Limit:      2,
			Typ:        c.oid,
			NumVec:     c.num,
			MetricType: metric.Metric_L2Distance,
			DistHeap:   make(objectio.Float64Heap, 0, 2),
		}
		resSels, resDists, err := handleOrderByLimitOnSelectRows(ctx, []int64{0, 1, 2}, orderByLimit, nil, -1, cacheVectors)
		require.NoErrorf(t, err, c.name)
		require.Lenf(t, resSels, 2, c.name)
		require.Lenf(t, resDists, 2, c.name)
		require.Equalf(t, int64(1), resSels[0], "%s closest", c.name)
		require.Equalf(t, int64(2), resSels[1], "%s next", c.name)
	}
}
