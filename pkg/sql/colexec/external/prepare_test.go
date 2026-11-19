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

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type prepareWorkerKey struct{}

func prepareTemplate() *tree.ExternParam {
	return &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INFILE,
			Filepath: "prepare:input.jsonl",
			Format:   tree.JSONLINE,
			Tail:     &tree.TailParameter{},
			Option:   []string{"comment", "#"},
		},
		ExParam: tree.ExParam{
			ExternType: int32(plan.ExternType_LOAD),
			Parallel:   true,
			JsonData:   tree.OBJECT,
			NullMap:    map[string][]string{"v": {"null"}},
			S3Param:    &tree.S3Parameter{Bucket: "unchanged"},
			Ctx:        context.WithValue(context.Background(), prepareWorkerKey{}, "template"),
		},
	}
}

func newPrepareWorker(t *testing.T, shared *tree.ExternParam, index int) (*External, *process.Process) {
	t.Helper()
	fs, err := fileservice.NewMemoryFS("prepare", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	proc := testutil.NewProcess(t, testutil.WithFileService(fs))
	proc.Base.SessionInfo.Account = "sys"
	proc.Base.SessionInfo.Role = "moadmin"
	arg := NewArgument().WithEs(&ExternalParam{
		ExParamConst: ExParamConst{
			Extern:        shared,
			Idx:           index,
			StrictSqlMode: true,
			ColumnListLen: 1,
			Attrs:         []plan.ExternAttr{{ColName: "v", ColIndex: 0, ColFieldIndex: 0}},
			Cols:          []*plan.ColDef{{Name: "v", Typ: plan.Type{Id: int32(types.T_int64)}}},
			FileList:      []string{shared.Filepath},
			FileSize:      []int64{8}, // {"v":1}\n
			FileOffsetTotal: []*pipeline.FileOffset{
				{Offset: []int64{0, -1, 0, -1}},
			},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{}, Filter: &FilterParam{}},
	})
	t.Cleanup(func() {
		arg.Free(proc, false, nil)
		arg.Release()
	})
	return arg, proc
}

// Snapshot nested configuration as well as scalar options, excluding opaque
// runtime interfaces whose bindings are checked separately.
func prepareConfigSnapshot(t *testing.T, param *tree.ExternParam) string {
	t.Helper()
	config := *param
	config.Ctx, config.FileService = nil, nil
	data, err := json.Marshal(config)
	require.NoError(t, err)
	return string(data)
}

func TestExternalPrepareIsolatesWorkers(t *testing.T) {
	for _, explicitService := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit-service=%t", explicitService), func(t *testing.T) {
			shared := prepareTemplate()
			const workers = 2
			var args [workers]*External
			var procs [workers]*process.Process
			var cancels [workers]context.CancelFunc
			for i := range workers {
				args[i], procs[i] = newPrepareWorker(t, shared, i)
				proc := procs[i]
				proc.Ctx, cancels[i] = context.WithCancel(context.WithValue(proc.Ctx, prepareWorkerKey{}, i))
				t.Cleanup(cancels[i])
				data := []byte(fmt.Sprintf("{\"v\":%d}\n", i+1))
				require.NoError(t, proc.Base.FileService.Write(proc.Ctx, fileservice.IOVector{
					FilePath: shared.Filepath,
					Entries:  []fileservice.IOEntry{{Size: int64(len(data)), Data: data}},
				}))
			}
			if explicitService {
				shared.FileService = procs[0].Base.FileService
			}
			originalCtx, originalFS := shared.Ctx, shared.FileService
			configBefore := prepareConfigSnapshot(t, shared)
			assertTemplate := func() {
				require.Same(t, originalCtx, shared.Ctx)
				require.Equal(t, originalFS, shared.FileService)
				require.Equal(t, configBefore, prepareConfigSnapshot(t, shared))
			}

			// Compile gives each parallel duplicate its own ExternalParam but the
			// same Extern pointer. Join all workers before checking or cleaning up.
			start := make(chan struct{})
			var wg sync.WaitGroup
			var errs [workers]error
			for i := range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					errs[i] = args[i].Prepare(procs[i])
				}()
			}
			close(start)
			wg.Wait()
			for _, err := range errs {
				require.NoError(t, err)
			}
			assertTemplate()

			for i := range workers {
				param := args[i].Es
				require.NotSame(t, shared, param.Extern)
				require.Equal(t, i, param.Extern.Ctx.Value(prepareWorkerKey{}))
				expectedFS := procs[i].Base.FileService
				expectedValue := int64(i + 1)
				if explicitService {
					expectedFS, expectedValue = originalFS, 1
				}
				require.Same(t, expectedFS, param.Extern.FileService)
				// Call opens a server-side file through crt.GetIOReadCloser, which
				// also writes Extern.Ctx, then parses and converts a real JSON row.
				result, err := args[i].Call(procs[i])
				require.NoError(t, err)
				require.NotNil(t, result.Batch)
				require.Equal(t, 1, result.Batch.RowCount())
				require.Equal(t, expectedValue, vector.GetFixedAtWithTypeCheck[int64](result.Batch.Vecs[0], 0))
				require.Equal(t, i, param.Extern.Ctx.Value(prepareWorkerKey{}))
			}
			assertTemplate()

			// Bind both parsers before cancellation. A small block makes Read
			// reach the parser's existing context check deterministically.
			parser0, err := newCSVParserFromReaderWithBlockSize(args[0].Es.Extern, strings.NewReader("{\"v\":1}\n"), 2)
			require.NoError(t, err)
			parser1, err := newCSVParserFromReaderWithBlockSize(args[1].Es.Extern, strings.NewReader("{\"v\":2}\n"), 2)
			require.NoError(t, err)
			cancels[0]()
			_, err = parser0.Read(nil)
			require.ErrorIs(t, err, context.Canceled)
			row, err := parser1.Read(nil)
			require.NoError(t, err)
			require.Len(t, row, 1)
			require.Equal(t, "{\"v\":2}", row[0].Val)

			previous := args[0].Es.Extern
			siblingCtx := args[1].Es.Extern.Ctx
			args[0].Reset(procs[0], false, nil)
			procs[0].Ctx = context.WithValue(context.Background(), prepareWorkerKey{}, "reprepared")
			require.NoError(t, args[0].Prepare(procs[0]))
			require.NotSame(t, previous, args[0].Es.Extern)
			require.ErrorIs(t, previous.Ctx.Err(), context.Canceled)
			require.Equal(t, "reprepared", args[0].Es.Extern.Ctx.Value(prepareWorkerKey{}))
			require.NoError(t, args[0].Es.Extern.Ctx.Err())
			require.Same(t, previous.FileService, args[0].Es.Extern.FileService)
			require.Same(t, siblingCtx, args[1].Es.Extern.Ctx)
			assertTemplate()
		})
	}
}

func TestExternalPrepareKeepsTemplateOnError(t *testing.T) {
	for _, invalidFormat := range []bool{true, false} {
		t.Run(fmt.Sprintf("invalid-format=%t", invalidFormat), func(t *testing.T) {
			shared := prepareTemplate()
			if invalidFormat {
				shared.Format = "unsupported"
			}
			arg, proc := newPrepareWorker(t, shared, 0)
			if !invalidFormat {
				arg.Es.Attrs[0].ColIndex = 2
			}
			before := prepareConfigSnapshot(t, shared)
			ctx := shared.Ctx
			err := arg.Prepare(proc)
			if invalidFormat {
				require.ErrorContains(t, err, "load format 'unsupported'")
			} else {
				require.ErrorContains(t, err, "external output column index 2 is invalid")
			}
			require.Nil(t, shared.FileService)
			require.Same(t, ctx, shared.Ctx)
			require.Equal(t, before, prepareConfigSnapshot(t, shared))
			require.Nil(t, arg.reader)
			require.Nil(t, arg.ctr.buf)
		})
	}
}

func TestExternalPrepareDecodedConfig(t *testing.T) {
	shared := prepareTemplate()
	shared.Parallel = false
	arg, proc := newPrepareWorker(t, shared, 0)
	arg.Es.CreateSql = prepareConfigSnapshot(t, shared)
	arg.Es.Extern = nil
	require.NoError(t, arg.Prepare(proc))
	require.Same(t, proc.Base.FileService, arg.Es.Extern.FileService)
	require.Same(t, arg.Es.Ctx, arg.Es.Extern.Ctx)
	require.Equal(t, tree.JSONLINE, arg.Es.Extern.Format)
	require.Equal(t, tree.OBJECT, arg.Es.Extern.JsonData)
	require.Equal(t, []string{"comment", "#"}, arg.Es.Extern.Option)
}
