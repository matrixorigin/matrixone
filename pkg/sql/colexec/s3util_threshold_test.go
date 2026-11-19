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

package colexec

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync/atomic"
	"testing"

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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCNS3DataWriterMemoryThresholdAndSyncAndFillBlockInfoBat(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	fs, err := fileservice.Get[fileservice.FileService](proc.Base.FileService, defines.SharedFileServiceName)
	require.NoError(t, err)

	tableDef := testCNS3WriterTableDef()

	t.Run("custom threshold", func(t *testing.T) {
		writer := NewCNS3DataWriter(proc.Mp(), fs, tableDef, 1, false)
		defer writer.Close()

		require.Equal(t, 1, writer.MemorySizeThreshold())

		bat := &batch.Batch{
			Attrs: []string{"a", "b"},
			Vecs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp()),
				testutil.MakeVarcharVector([]string{"x"}, nil, proc.Mp()),
			},
		}
		bat.SetRowCount(1)
		defer bat.Clean(proc.Mp())

		err = writer.Write(proc.Ctx, bat)
		require.NoError(t, err)

		blockInfoBat, err := writer.SyncAndFillBlockInfoBat(proc.Ctx)
		require.NoError(t, err)
		require.NotNil(t, blockInfoBat)
		require.Greater(t, blockInfoBat.RowCount(), 0)

		blockInfoBat, err = writer.SyncAndFillBlockInfoBat(proc.Ctx)
		require.NoError(t, err)
		require.NotNil(t, blockInfoBat)
		require.Equal(t, 0, blockInfoBat.RowCount())
	})

	t.Run("flush on sync", func(t *testing.T) {
		writer := NewCNS3DataWriter(proc.Mp(), fs, tableDef, -1, true)
		defer writer.Close()
		require.Equal(t, math.MaxInt, writer.MemorySizeThreshold())
	})

	t.Run("large input is split into readable objects", func(t *testing.T) {
		writer := NewCNS3DataWriter(proc.Mp(), fs, tableDef, 128, false)
		defer writer.Close()
		ids := []int64{7, 6, 5, 4, 3, 2, 1, 0}
		values := make([]string, len(ids))
		for i := range values {
			values[i] = strings.Repeat("x", 64)
		}
		bat := &batch.Batch{
			Attrs: []string{"a", "b"},
			Vecs: []*vector.Vector{
				testutil.MakeInt64Vector(ids, nil, proc.Mp()),
				testutil.MakeVarcharVector(values, nil, proc.Mp()),
			},
		}
		bat.SetRowCount(len(ids))
		defer bat.Clean(proc.Mp())
		require.NoError(t, writer.Write(proc.Ctx, bat))
		infos, err := writer.SyncAndFillBlockInfoBat(proc.Ctx)
		require.NoError(t, err)
		require.Greater(t, infos.RowCount(), 1)
		rows := 0
		for i := 0; i < infos.Vecs[0].Length(); i++ {
			info := objectio.DecodeBlockInfo(infos.Vecs[0].GetBytesAt(i))
			rows += int(objectio.Location(info.MetaLoc[:]).Rows())
		}
		require.Equal(t, len(ids), rows)
	})
}

func TestCNSTombstoneWriterBoundsVariableLengthRuns(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	fs, err := fileservice.Get[fileservice.FileService](proc.Base.FileService, defines.SharedFileServiceName)
	require.NoError(t, err)

	input, pkType, typs, want := newVarlenTombstoneInput(t, proc.Mp())
	defer input.Clean(proc.Mp())

	const threshold = 400
	writer := NewCNS3TombstoneWriter(proc.Mp(), fs, pkType, threshold)
	defer func() { require.NoError(t, writer.Close()) }()
	require.NoError(t, writer.Write(proc.Ctx, input))
	stats, err := writer.Sync(proc.Ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(stats), "the threshold crossing must start a new tombstone object")
	blockInfoBat, err := writer.FillBlockInfoBat()
	require.NoError(t, err)
	require.Equal(t, len(stats), blockInfoBat.RowCount(), "every tombstone object must reach the caller")

	seen := make(map[types.Rowid]bool, len(want))
	for _, stat := range stats {
		objectio.ForeachBlkInObjStatsList(false, nil, func(info objectio.BlockInfo, _ objectio.BlockObject) bool {
			loc := objectio.Location(info.MetaLoc[:])
			reader, err := ioutil.NewObjectReader(fs, loc)
			require.NoError(t, err)
			got, release, err := reader.LoadColumns(proc.Ctx, []uint16{0, 1}, typs, loc.ID(), proc.Mp())
			require.NoError(t, err)
			defer release()
			rowIDs := vector.MustFixedColNoTypeCheck[types.Rowid](got.Vecs[objectio.TombstoneAttr_Rowid_Idx])
			for i, rowID := range rowIDs {
				require.Equal(t, want[rowID], string(got.Vecs[objectio.TombstoneAttr_PK_Idx].GetBytesAt(i)))
				require.False(t, seen[rowID])
				seen[rowID] = true
			}
			return true
		}, stat)
	}
	require.Len(t, seen, len(want))
}

func TestCNSTombstoneWriterCleansEarlierRunOnFailure(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	base := testutil.NewSharedFS()
	defer base.Close(context.Background())
	writeErr := errors.New("injected second tombstone object failure")
	fs := &cnTombstoneFailureFS{FileService: base, failAt: 2, writeErr: writeErr}

	input, pkType, _, _ := newVarlenTombstoneInput(t, proc.Mp())
	defer input.Clean(proc.Mp())

	writer := NewCNS3TombstoneWriter(proc.Mp(), fs, pkType, 400)
	defer func() { require.NoError(t, writer.Close()) }()
	require.NoError(t, writer.Write(proc.Ctx, input))
	_, err := writer.Sync(proc.Ctx)
	require.ErrorIs(t, err, writeErr)

	files, err := fileservice.SortedList(base.List(context.Background(), ""))
	require.NoError(t, err)
	require.Empty(t, files, "a failed second run must remove the earlier unpublished object")
}

type cnTombstoneFailureFS struct {
	fileservice.FileService
	writes   atomic.Int32
	failAt   int32
	writeErr error
}

func (fs *cnTombstoneFailureFS) Write(ctx context.Context, vec fileservice.IOVector) error {
	if fs.writes.Add(1) == fs.failAt {
		return fs.writeErr
	}
	return fs.FileService.Write(ctx, vec)
}

func newVarlenTombstoneInput(
	t *testing.T,
	mp *mpool.MPool,
) (*batch.Batch, types.Type, []types.Type, map[types.Rowid]string) {
	t.Helper()
	pkType := types.T_varchar.ToType()
	attrs, typs := objectio.GetTombstoneSchema(pkType, objectio.HiddenColumnSelection_None)
	input := batch.NewWithSchema(false, attrs, typs)
	blockID := types.BuildTestBlockid(1, 2)
	want := make(map[types.Rowid]string, 3)
	for i := uint32(0); i < 3; i++ {
		rowID := types.NewRowid(&blockID, i)
		value := string(rune('a'+i)) + strings.Repeat("x", 128)
		require.NoError(t, vector.AppendFixed(input.Vecs[objectio.TombstoneAttr_Rowid_Idx], rowID, false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[objectio.TombstoneAttr_PK_Idx], []byte(value), false, mp))
		want[rowID] = value
	}
	input.SetRowCount(len(want))
	return input, pkType, typs, want
}

func testCNS3WriterTableDef() *plan.TableDef {
	return &plan.TableDef{
		Name: "t1",
		Cols: []*plan.ColDef{
			{ColId: 0, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64)}, NotNull: true, Primary: true, Default: &plan.Default{NullAbility: false}},
			{ColId: 1, Name: "b", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_varchar), Width: 8192}, NotNull: true},
			{ColId: 2, Name: catalog.Row_ID, Seqnum: 2, Typ: plan.Type{Id: int32(types.T_Rowid)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			Cols:        []uint64{0},
			PkeyColId:   0,
			PkeyColName: "a",
			Names:       []string{"a"},
		},
		Name2ColIndex: map[string]int32{
			"a":            0,
			"b":            1,
			catalog.Row_ID: 2,
		},
	}
}

type closeFailureFS struct {
	fileservice.FileService
	writes     int
	failDelete bool
}

func (f *closeFailureFS) Write(ctx context.Context, v fileservice.IOVector) error {
	f.writes++
	if f.writes == 2 {
		return errors.New("injected object write failure")
	}
	return f.FileService.Write(ctx, v)
}

func (f *closeFailureFS) Delete(ctx context.Context, names ...string) error {
	if f.failDelete {
		return errors.New("injected delete failure")
	}
	return f.FileService.Delete(ctx, names...)
}

func TestCNS3WriterCloseFreesBlockInfoWhenDeleteFails(t *testing.T) {
	ctx := context.Background()
	base := testutil.NewSharedFS()
	defer base.Close(ctx)
	fs := &closeFailureFS{FileService: base, failDelete: true}
	mp := mpool.MustNewZero()
	writer := NewCNS3DataWriter(mp, fs, testCNS3WriterTableDef(), 128, false)
	require.NoError(t, vector.AppendBytes(writer.blockInfoBat.Vecs[0], []byte(strings.Repeat("m", 128)), false, mp))
	writer.blockInfoBat.SetRowCount(1)
	input := batch.NewWithSchema(false, []string{"a", "b"}, []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()})
	for i := 0; i < 8; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(i), false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 64)), false, mp))
	}
	input.SetRowCount(8)
	require.ErrorContains(t, writer.Write(ctx, input), "injected object write failure")
	input.Clean(mp)
	require.ErrorContains(t, writer.Close(), "injected delete failure")
	require.Nil(t, writer.blockInfoBat)
	require.NotNil(t, writer.sinker, "the caller can retry cleanup")
	require.Zero(t, mp.CurrNB(), "a discarded writer must not retain mpool buffers")
	onHeapBytes, onHeapObjects := mp.OnHeapOutstanding()
	require.Zero(t, onHeapBytes, "failed cleanup must release on-heap buffers")
	require.Zero(t, onHeapObjects, "failed cleanup must release on-heap allocations")
	fs.failDelete = false
	require.NoError(t, writer.Close())
	require.Nil(t, writer.sinker)
	files, err := fileservice.SortedList(base.List(ctx, ""))
	require.NoError(t, err)
	require.Empty(t, files)
}
