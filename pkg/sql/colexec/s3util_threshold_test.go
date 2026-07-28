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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
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
