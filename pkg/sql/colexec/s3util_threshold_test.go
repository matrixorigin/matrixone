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
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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

func TestCNS3DataWriterChunkedColumnProtocolGateIsLive(t *testing.T) {
	previousObjectSizeLimit := objectio.ObjectSizeLimit
	objectio.SetObjectSizeLimit(3 * mpool.GB)
	t.Cleanup(func() { objectio.SetObjectSizeLimit(previousObjectSizeLimit) })

	proc := testutil.NewProc(t)
	defer proc.Free()
	fs, err := fileservice.Get[fileservice.FileService](
		proc.Base.FileService, defines.SharedFileServiceName,
	)
	require.NoError(t, err)

	serviceID := "chunked-column-" + t.Name()
	rt := moruntime.DefaultRuntime()
	originalVersion, hadOriginalVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadOriginalVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, originalVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	moruntime.SetupServiceBasedRuntime(serviceID, rt)

	tableDef := &plan.TableDef{
		Name: "wide_column",
		Cols: []*plan.ColDef{
			{ColId: 0, Name: "payload", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_text)}},
			{ColId: 1, Name: catalog.Row_ID, Seqnum: 1, Typ: plan.Type{Id: int32(types.T_Rowid)}},
		},
		Pkey: &plan.PrimaryKeyDef{},
	}
	bat := batch.NewWithSize(1)
	bat.Attrs = []string{"payload"}
	bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
	payload := make([]byte, 20<<10)
	for i := range 512 {
		payload[0] = byte(i)
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], payload, false, proc.Mp()))
	}
	bat.SetRowCount(bat.Vecs[0].Length())
	defer bat.Clean(proc.Mp())

	writeAndColumnAlgorithm := func(writer *CNS3Writer) uint8 {
		t.Helper()
		defer writer.Close()
		require.NoError(t, writer.Write(proc.Ctx, bat))
		stats, syncErr := writer.Sync(proc.Ctx)
		require.NoError(t, syncErr)
		require.Len(t, stats, 1)
		location := stats[0].ObjectLocation()
		meta, loadErr := objectio.FastLoadObjectMeta(proc.Ctx, &location, false, fs)
		require.NoError(t, loadErr)
		dataMeta, ok := meta.DataMeta()
		require.True(t, ok)
		return dataMeta.GetBlockMeta(0).MustGetColumn(0).Location().Alg()
	}

	// The latest pre-feature protocol must remain on the legacy extent format.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion17)
	enabledAfterConstruction := NewCNS3DataWriterForService(
		serviceID, proc.Mp(), fs, tableDef, -1, true,
	)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion20)
	require.Equal(t, uint8(compress.Lz4Chunked), writeAndColumnAlgorithm(enabledAfterConstruction))

	disabledAfterConstruction := NewCNS3DataWriterForService(
		serviceID, proc.Mp(), fs, tableDef, -1, true,
	)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion17)
	require.Equal(t, uint8(compress.Lz4), writeAndColumnAlgorithm(disabledAfterConstruction))

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion16)
	withoutServiceOwner := NewCNS3DataWriter(proc.Mp(), fs, tableDef, -1, true)
	require.Equal(t, uint8(compress.Lz4), writeAndColumnAlgorithm(withoutServiceOwner))
}

func TestChunkedColumnPolicyProtocolThreshold(t *testing.T) {
	require.Nil(t, chunkedColumnPolicyForService(""))

	serviceID := fmt.Sprintf("chunked-policy-%s", t.Name())
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	policy := chunkedColumnPolicyForService(serviceID)
	require.NotNil(t, policy)

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion19)
	require.False(t, policy())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion20)
	require.True(t, policy())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int32(defines.MORPCVersion20))
	require.False(t, policy())

	missingPolicy := chunkedColumnPolicyForService("missing-" + serviceID)
	require.NotNil(t, missingPolicy)
	require.False(t, missingPolicy())
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
