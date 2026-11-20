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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
	"testing"
)

type backportSinkerWorkspace struct {
	client.Workspace
	option ioutil.SinkerOption
}

func (w *backportSinkerWorkspace) SinkerCapacityOption() ioutil.SinkerOption { return w.option }

type backportSinkerTxn struct {
	client.TxnOperator
	workspace client.Workspace
}

type backportHandoffWorkspace struct {
	client.Workspace
	scope ioutil.RemoteWorkspaceHandoffScope
}

func (w *backportHandoffWorkspace) RemoteWorkspaceHandoffScope() (ioutil.RemoteWorkspaceHandoffScope, bool) {
	return w.scope, true
}

func (*backportHandoffWorkspace) MarkRemoteWorkspaceOutputReady([]string) error { return nil }

func (t *backportSinkerTxn) GetWorkspace() client.Workspace { return t.workspace }

func TestWorkspaceHandoffScopeTransientBatch(t *testing.T) {
	proc := testutil.NewProc(t)
	scope := ioutil.RemoteWorkspaceHandoffScope{
		Version: ioutil.RemoteWorkspaceHandoffVersion1, TxnID: "txn",
		ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, ProducerCN: "cn:1",
	}
	proc.Base.TxnOperator = &backportSinkerTxn{workspace: &backportHandoffWorkspace{scope: scope}}
	bat := batch.NewWithSchema(false, []string{"v"}, []types.Type{types.T_int64.ToType()})
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, proc.Mp()))
	bat.SetRowCount(1)
	require.NoError(t, AttachWorkspaceHandoffScope(proc, bat))
	require.NoError(t, AttachWorkspaceHandoffScope(proc, bat))
	data, err := bat.MarshalBinary()
	require.NoError(t, err)
	decoded := &batch.Batch{}
	require.NoError(t, decoded.UnmarshalBinary(data))
	t.Cleanup(func() { decoded.Clean(proc.Mp()) })
	got, present, err := ioutil.ParseRemoteWorkspaceHandoffScope(decoded.ExtraBuf)
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, scope, got)
	bat.ExtraBuf = []byte("unrelated metadata")
	require.Error(t, AttachWorkspaceHandoffScope(proc, bat))
	proc.Base.TxnOperator = &backportSinkerTxn{}
	require.NoError(t, AttachWorkspaceHandoffScope(proc, bat))
	bat.ExtraBuf = decoded.ExtraBuf
	require.Error(t, AttachWorkspaceHandoffScope(proc, bat), "a reused batch cannot transfer stale provenance")
}
func TestBackportS3WorkspaceHookRebind(t *testing.T) {
	proc := testutil.NewProc(t)
	fs := testutil.NewSharedFS()
	t.Cleanup(func() { fs.Close(context.Background()) })
	firstErr := errors.New("first workspace full")
	secondErr := errors.New("second workspace full")
	firstCalls, secondCalls := 0, 0
	first := &backportSinkerWorkspace{option: func(s *ioutil.Sinker) {
		ioutil.WithWorkspaceOwnedCleanup()(s)
		ioutil.WithUncommittedCapacityHook(func(string) error { firstCalls++; return firstErr })(s)
	}}
	proc.Base.TxnOperator = &backportSinkerTxn{workspace: first}
	def := &plan.TableDef{Pkey: &plan.PrimaryKeyDef{}, Cols: []*plan.ColDef{{Name: "v", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64)}}}}
	w := NewCNS3DataWriter(proc.Mp(), fs, def, 1, false, WorkspaceSinkerCapacityOption(proc))
	t.Cleanup(func() { _ = w.Close() })
	require.True(t, w.HasUncommittedCapacityHook())
	require.True(t, w.WorkspaceOwnsCleanup())
	src := batch.NewWithSchema(false, []string{"v"}, []types.Type{types.T_int64.ToType()})
	t.Cleanup(func() { src.Clean(proc.Mp()) })
	require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(1), false, proc.Mp()))
	src.SetRowCount(1)
	require.ErrorIs(t, w.Write(context.Background(), src), firstErr)
	require.Equal(t, 1, firstCalls)
	require.Empty(t, w.AbandonAndRetainableNames())
	w.Reset()
	w.ClearWorkspaceSinkerCapacity()
	require.False(t, w.HasUncommittedCapacityHook())
	require.False(t, w.WorkspaceOwnsCleanup())
	second := &backportSinkerWorkspace{option: ioutil.WithUncommittedCapacityHook(func(string) error { secondCalls++; return secondErr })}
	proc.Base.TxnOperator = &backportSinkerTxn{workspace: second}
	w.RebindWorkspaceSinkerCapacity(proc)
	require.True(t, w.HasUncommittedCapacityHook())
	require.False(t, w.WorkspaceOwnsCleanup())
	require.ErrorIs(t, w.Write(context.Background(), src), secondErr)
	require.Equal(t, 1, firstCalls)
	require.Equal(t, 1, secondCalls)
	w.Reset()
	w.RebindWorkspaceSinkerCapacity(nil)
	require.False(t, w.HasUncommittedCapacityHook())
	require.False(t, w.WorkspaceOwnsCleanup())
}
