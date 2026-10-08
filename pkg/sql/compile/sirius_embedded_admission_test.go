// Copyright 2026 Matrix Origin
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

package compile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

type embeddedAdmissionRecorder struct {
	SiriusBackend
	request   SiriusPrepareRequest
	err       error
	execution SiriusExecution
}

func (b *embeddedAdmissionRecorder) Prepare(_ context.Context, request SiriusPrepareRequest) (SiriusExecution, error) {
	b.request = request
	return b.execution, b.err
}

func TestEmbeddedSiriusAdmissionBindsWithoutStartingReaders(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctrl := gomock.NewController(t)
	tx := mock_frontend.NewMockTxnOperator(ctrl)
	tx.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	tx.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	proc.Base.TxnOperator = tx
	query := &planpb.Query{StmtType: planpb.Query_SELECT, Steps: []int32{0}, Headings: []string{"n"}, Nodes: []*planpb.Node{{
		NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{Obj: 42, ObjName: "t", SchemaName: "db"},
		TableDef: &planpb.TableDef{DbId: 7, TblId: 42, Name: "t", Version: 3, Cols: []*planpb.ColDef{{Name: "n", ColId: 11, Seqnum: 5, Typ: planpb.Type{Id: int32(types.T_int64)}}}},
	}}}
	plan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: query}}
	eng := &readerPathCaptureEngine{}
	execution := &siriusExecutionStub{}
	backend := &embeddedAdmissionRecorder{execution: execution}
	runtime := &SiriusRuntime{EmbeddedMO: true, Backend: backend, CleanupTimeout: time.Second, RequestTimeout: time.Minute}
	c := &Compile{proc: proc, e: eng, ncpu: 2, TxnOffset: 3}
	ctx, cancel := context.WithTimeout(defines.AttachAccountId(t.Context(), 7), time.Second)
	defer cancel()
	offloaded, err := c.compileEmbeddedSiriusRead(ctx, plan, runtime)
	require.NoError(t, err)
	require.True(t, offloaded)
	require.Same(t, execution, c.siriusRead.execution)
	require.Len(t, backend.request.Reads, 1)
	require.NotNil(t, backend.request.Reads[0].Producer)
	require.Equal(t, uint64(7), backend.request.AccountID)
	require.Equal(t, query.Headings, backend.request.Headings)
	deadline, _ := ctx.Deadline()
	require.Equal(t, deadline, backend.request.Deadline)
	require.Equal(t, 0, eng.buildBlockReadersCalls)
	// A nil database makes an eager relation open panic, so successful Prepare
	// also proves that no relation/readers were created during admission.
	var wire spb.Plan
	require.NoError(t, proto.Unmarshal(backend.request.Plan, &wire))
	require.Equal(t, "__sirius_embedded_v1", wire.Relations[0].GetRoot().Input.GetRead().GetNamedTable().Names[0])
	c.siriusRead = nil
	backend.err = errors.New("native prepare rejected")
	offloaded, err = c.compileEmbeddedSiriusRead(ctx, plan, runtime)
	require.ErrorIs(t, err, backend.err)
	require.False(t, offloaded)
	require.Nil(t, c.siriusRead)
	proc.Base.TxnOperator = nil
	_, err = c.compileEmbeddedSiriusRead(ctx, plan, runtime)
	require.ErrorContains(t, err, "statement transaction")
}
