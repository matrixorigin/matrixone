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

package multi_update

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type importWorkspaceOwner struct {
	client.Workspace
	admitted, retained []string
}

func (w *importWorkspaceOwner) SinkerCapacityOption() ioutil.SinkerOption {
	return func(s *ioutil.Sinker) {
		ioutil.WithWorkspaceOwnedCleanup()(s)
		ioutil.WithUncommittedCapacityHook(func(name string) error { w.admitted = append(w.admitted, name); return nil })(s)
	}
}

func (w *importWorkspaceOwner) RetainUnrecordedS3Objects(names []string, _ string) {
	w.retained = append(w.retained, names...)
}

type importWorkspaceTxn struct {
	client.TxnOperator
	workspace client.Workspace
}

func (t *importWorkspaceTxn) GetWorkspace() client.Workspace { return t.workspace }

func TestWorkspaceImportFailureTransfersRatherThanDeletesObjects(t *testing.T) {
	ctx := context.Background()
	enabled := fault.Enable()
	defer func() {
		_, _ = fault.RemoveFaultPoint(ctx, objectio.FJ_CNFlushSmallObjs)
		if enabled {
			fault.Disable()
		}
	}()
	require.NoError(t, fault.AddFaultPoint(ctx, objectio.FJ_CNFlushSmallObjs, ":::", "ECHO", 0, "ownership_db", false))
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	failing := &ownershipFailFS{FileService: fs, allow: 1}
	proc := testutil.NewProcess(t, testutil.WithFileService(failing))
	defer proc.Free()
	owner := &importWorkspaceOwner{}
	proc.Base.TxnOperator = &importWorkspaceTxn{workspace: owner}
	input := batch.NewOffHeap([]string{"v"})
	input.Vecs[0] = vector.NewOffHeapVecWithType(types.New(types.T_varchar, 256, 0))
	for range objectio.BlockMaxRows {
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(strings.Repeat("x", 40)), false, proc.Mp()))
	}
	input.SetRowCount(int(objectio.BlockMaxRows))
	second, err := input.Dup(proc.Mp())
	require.NoError(t, err)
	bats := []*batch.Batch{input, second}
	table := &planpb.TableDef{DbName: "ownership_db", Name: "ownership_table", Pkey: &planpb.PrimaryKeyDef{PkeyColName: "absent"}, Cols: []*planpb.ColDef{{Name: "v", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 256}}}}
	writer := &s3WriterDelegate{cleanupUnpublished: true}
	err = writer.sortAndSyncOneTable(proc, table, process.NewTempAnalyzer(), 0, false, bats, true)
	require.ErrorContains(t, err, "injected object write failure")
	require.Len(t, failing.writes, 1)
	require.NoError(t, writer.free(proc))
	require.Contains(t, owner.retained, failing.writes[0], "the workspace must own the successful prefix")
	_, err = fs.StatFile(ctx, failing.writes[0])
	require.NoError(t, err, "the import cleanup must not delete a workspace-owned object")
}
