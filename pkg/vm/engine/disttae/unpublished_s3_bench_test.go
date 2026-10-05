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

package disttae

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type spillBenchLockService struct {
	lockservice.LockService
	id string
}

func (s spillBenchLockService) GetConfig() lockservice.Config {
	return lockservice.Config{ServiceID: s.id}
}

type spillBenchTxnOperator struct {
	client.TxnOperator
	workspace client.Workspace
}

func (op *spillBenchTxnOperator) GetWorkspace() client.Workspace { return op.workspace }

func newSpillBenchProcess(b *testing.B) (*process.Process, *colexec.Server, fileservice.FileService) {
	b.Helper()
	proc := testutil.NewProc(b)
	b.Cleanup(proc.Free)
	b.Cleanup(func() { proc.Base.FileService.Close(context.Background()) })
	id := b.Name()
	moruntime.SetupServiceBasedRuntime(id, moruntime.DefaultRuntime())
	proc.Base.LockService = spillBenchLockService{id: id}
	server := colexec.NewServer(id)
	fs, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(b, err)
	b.Cleanup(func() { fs.Close(context.Background()) })
	return proc, server, fs
}

// This exact benchmark also compiles on main: the optional interfaces select
// the ownership handoff when it exists, not an artificial disabled PR path.
// Each operation is four real spill/Sync/metadata/accept cycles; input setup
// and physical deletion of accepted output are excluded on both revisions.
func BenchmarkCNS3RepeatedSpill(b *testing.B) {
	for _, mib := range []int{1, 128} {
		b.Run(fmt.Sprintf("%dMiB", mib), func(b *testing.B) {
			proc, _, fs := newSpillBenchProcess(b)
			txn := &Transaction{}
			proc.Base.TxnOperator = &spillBenchTxnOperator{workspace: txn}
			table := &plan.TableDef{
				Name: "spill",
				Cols: []*plan.ColDef{
					{Name: "id", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64)}, Primary: true},
					{Name: "payload", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_varchar), Width: 120}},
					{Name: catalog.Row_ID, Seqnum: 2, Typ: plan.Type{Id: int32(types.T_Rowid)}},
				},
				Pkey:          &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
				Name2ColIndex: map[string]int32{"id": 0, "payload": 1, catalog.Row_ID: 2},
			}
			input := batch.NewWithSize(2)
			input.Attrs = []string{"id", "payload"}
			input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			input.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			b.Cleanup(func() { input.Clean(proc.Mp()) })
			for i := range 8192 {
				require.NoError(b, vector.AppendFixed(input.Vecs[0], int64(i), false, proc.Mp()))
				require.NoError(b, vector.AppendBytes(input.Vecs[1], []byte(fmt.Sprintf("%0120d", i)), false, proc.Mp()))
			}
			input.SetRowCount(8192)
			writer := colexec.NewCNS3DataWriterForService(proc.GetService(), proc.Mp(), fs, table, mib<<20, false)
			b.Cleanup(func() { require.NoError(b, writer.Close()) })
			transfer, hasTransfer := any(writer).(interface{ TransferPersistedObjects(*process.Process) error })
			accept, hasAccept := any(txn).(interface{ AcceptUnpublishedS3ObjectNames(...string) })
			require.Equal(b, hasTransfer, hasAccept)
			b.SetBytes(int64(4 * mib * 8192 * 128))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for range 4 {
					for range mib {
						if err := writer.Write(proc.Ctx, input); err != nil {
							b.Fatal(err)
						}
					}
					info, err := writer.SyncAndFillBlockInfoBat(proc.Ctx)
					if err != nil {
						b.Fatal(err)
					}
					data, area := vector.MustVarlenaRawData(info.Vecs[1])
					names := make([]string, len(data))
					for i := range data {
						stats := objectio.ObjectStats(data[i].GetByteSlice(area))
						names[i] = stats.ObjectName().String()
					}
					if len(names) == 0 {
						b.Fatal("spill did not persist an object")
					}
					if hasTransfer {
						if err := transfer.TransferPersistedObjects(proc); err != nil {
							b.Fatal(err)
						}
						accept.AcceptUnpublishedS3ObjectNames(names...)
					}
					writer.Reset()
					b.StopTimer()
					require.NoError(b, fs.Delete(proc.Ctx, names...))
					b.StartTimer()
				}
			}
		})
	}
}
