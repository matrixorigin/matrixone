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

package external

// Run:
//   CGO_CFLAGS="-I$(pwd)/cgo -I$(pwd)/thirdparties/install/include" \
//   CGO_LDFLAGS="-L$(pwd)/cgo -L$(pwd)/thirdparties/install/lib -lmo \
//     -Wl,-rpath,$(pwd)/cgo -Wl,-rpath,$(pwd)/thirdparties/install/lib" \
//   go test ./pkg/sql/colexec/external -run 'TestLoomZonemapHiddenColumnVectorLeak' -count=1 -v
//
// Expected behavior: every mpool-backed allocation that
// ZonemapReader.getBatchFromZonemapFile creates inside its per-column loop
// must be released by the time the read returns and the caller cleaned the
// output batch. Only the final `vecTmp` is freed by the deferred cleanup; a
// freshly allocated __mo_filepath (or all-null) vector in a non-final
// position is overwritten by the next iteration and leaks. This test asserts
// the pool returns to its baseline (rows can be re-read indefinitely), which
// fails on current main for the hidden-column-first projection.

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	objectioioutil "github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func loomWriteZonemapResultFile(t *testing.T, fs fileservice.FileService) string {
	t.Helper()
	wmp := mpool.MustNewZero()
	defer mpool.DeleteMPool(wmp)

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	defer bat.Clean(wmp)
	for i := 0; i < 64; i++ {
		require.NoError(t, vector.AppendBytes(
			bat.Vecs[0], []byte(fmt.Sprintf("value-%03d", i)), false, wmp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int32(i), false, wmp))
	}
	bat.SetRowCount(64)

	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	writer, err := objectioioutil.NewBlockWriter(fs, name.String())
	require.NoError(t, err)
	_, err = writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(context.Background())
	require.NoError(t, err)
	return name.String()
}

// loomReadViaZonemap drives one full ZonemapReader read over path with the
// given attribute projection, cleans everything the caller owns and reports
// how many on-heap bytes/objects the process mpool still has outstanding.
func loomReadViaZonemap(
	t *testing.T,
	fs fileservice.FileService,
	proc *process.Process,
	path string,
	attrs []plan.ExternAttr,
	cols []*plan.ColDef,
) (leftBytes int64, leftObjects int64) {
	t.Helper()
	mp := proc.GetMPool()

	buf := batch.NewWithSize(len(attrs))
	for i := range attrs {
		typ := types.T(cols[i].Typ.Id).ToType()
		buf.Vecs[i] = vector.NewVec(typ)
	}

	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Attrs: attrs,
			Cols:  cols,
			Ctx:   context.Background(),
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{Filepath: path},
				ExParam: tree.ExParam{
					FileService: fs,
					Ctx:         context.Background(),
				},
			},
		},
		ExParam: ExParam{
			Fileparam: &ExFileparam{Filepath: path},
			Filter:    &FilterParam{},
		},
	}

	r := NewZonemapReader(param, proc)
	_, err := r.Open(param, proc)
	require.NoError(t, err)

	beforeBytes, beforeObjects := mp.OnHeapOutstanding()

	analyzer := process.NewAnalyzer(0, false, false, "loom")
	finished, err := r.ReadBatch(context.Background(), buf, proc, analyzer)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 64, buf.RowCount())

	// Return everything the caller owns before measuring.
	buf.Clean(mp)
	require.NoError(t, r.Close())

	afterBytes, afterObjects := mp.OnHeapOutstanding()
	return afterBytes - beforeBytes, afterObjects - beforeObjects
}

func TestLoomZonemapHiddenColumnVectorLeak(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("loom-zonemap", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })

	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(fs))

	path := loomWriteZonemapResultFile(t, fs)

	// Projection [__mo_filepath, v]: the per-column loop materializes the
	// __mo_filepath vector first (AllocVectorOfRows, mpool-backed), then
	// overwrites vecTmp with the borrowed file vector of the second column.
	// Expected: no mpool debt remains once the output batch was cleaned.
	// Actual on current main: the hidden vector is abandoned, the pool keeps
	// outstanding on-heap bytes/objects for every block read.
	hiddenFirstAttrs := []plan.ExternAttr{
		{ColName: catalog.ExternalFilePath, ColIndex: 0, ColFieldIndex: 0},
		{ColName: "v", ColIndex: 1, ColFieldIndex: 1},
	}
	hiddenFirstCols := []*plan.ColDef{
		{Name: catalog.ExternalFilePath, Typ: plan.Type{Id: int32(types.T_varchar)}},
		{Name: "v", Typ: plan.Type{Id: int32(types.T_int32)}},
	}
	leftBytes, leftObjects := loomReadViaZonemap(t, fs, proc, path, hiddenFirstAttrs, hiddenFirstCols)
	require.Zero(t, leftBytes,
		"ZonemapReader left %d on-heap bytes in the process mpool after the caller cleaned its batch: the __mo_filepath vector allocated in a non-final column position is never freed", leftBytes)
	require.Zero(t, leftObjects,
		"ZonemapReader left %d on-heap objects outstanding in the process mpool", leftObjects)
}

// Control: the same projection with the hidden column last. The final vecTmp
// is exactly the vector the deferred cleanup frees, so the pool must be at
// baseline on current main. This guards the measurement against unrelated
// reader-internal retention and passes today.
func TestLoomZonemapHiddenColumnLastIsClean(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("loom-zonemap", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })

	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	proc := testutil.NewProcess(t, testutil.WithMPool(mp), testutil.WithFileService(fs))

	path := loomWriteZonemapResultFile(t, fs)

	hiddenLastAttrs := []plan.ExternAttr{
		{ColName: "v", ColIndex: 0, ColFieldIndex: 0},
		{ColName: catalog.ExternalFilePath, ColIndex: 1, ColFieldIndex: 1},
	}
	hiddenLastCols := []*plan.ColDef{
		{Name: "v", Typ: plan.Type{Id: int32(types.T_int32)}},
		{Name: catalog.ExternalFilePath, Typ: plan.Type{Id: int32(types.T_varchar)}},
	}
	ctrlBytes, ctrlObjects := loomReadViaZonemap(t, fs, proc, path, hiddenLastAttrs, hiddenLastCols)
	require.Zero(t, ctrlBytes)
	require.Zero(t, ctrlObjects)
}
