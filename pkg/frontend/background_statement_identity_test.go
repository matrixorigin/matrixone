// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestBackgroundStatementsHaveIndependentIdentity(t *testing.T) {
	server, conn, ses, proto := newTestServerWithRoutine(t, 0)
	rt := NewRoutine(context.Background(), proto, getPu("").SV)
	rt.setSession(ses)
	ses.setRoutine(rt)
	server.rm.setRoutine(conn, 0, rt)
	t.Cleanup(func() { server.rm.Closed(conn) })
	parentID := uuid.New()
	ses.SetStmtId(parentID)
	stop := errors.New("stop before compilation")
	var profiles []*process.StmtProfile
	var ids []uuid.UUID
	stub := gostub.Stub(&GetComputationWrapperInBack, func(_ *ExecCtx, _ string, _ *UserInput, _ string, _ engine.Engine, proc *process.Process, fses FeSession) ([]ComputationWrapper, error) {
		profile := proc.GetStmtProfile()
		profiles = append(profiles, profile)
		ids = append(ids, profile.GetStmtId())
		require.Equal(t, profile.GetStmtId(), fses.GetStmtId(), "session and pipeline must identify the same background statement")
		return nil, stop
	})
	t.Cleanup(stub.Reset)
	ctx := defines.AttachAccountId(context.Background(), 0)
	first := ses.GetBackgroundExec(ctx)
	t.Cleanup(first.Close)
	second := ses.GetBackgroundExec(ctx)
	t.Cleanup(second.Close)
	for _, bh := range []BackgroundExec{first, first, second} {
		require.ErrorContains(t, bh.Exec(ctx, "select 1"), stop.Error())
	}
	require.Len(t, ids, 3)
	seen := map[uuid.UUID]bool{parentID: true, uuid.Nil: true}
	for _, id := range ids {
		require.False(t, seen[id], "background statements must not share zero, parent, reused-executor or concurrent-executor identity")
		seen[id] = true
	}
	require.Equal(t, parentID, ses.GetStmtId(), "background execution must not mutate its parent's identity")
	require.Equal(t, ids[0], profiles[0].GetStmtId(), "a later background statement must not mutate a prior process profile")

	// Exercise the remote pipeline's actual statement-to-board rendezvous with
	// equal filter tags: payloads and cleanup must remain query-local.
	center := &message.MessageCenter{StmtIDToBoard: make(map[uuid.UUID]*message.MessageBoard), RwMutex: &sync.Mutex{}}
	boards := make([]*message.MessageBoard, len(ids))
	for i, id := range ids {
		boards[i] = message.NewMessageBoard().SetMultiCN(center, id)
		board := boards[i]
		t.Cleanup(func() { board.Reset() })
		message.SendMessage(message.RuntimeFilterMessage{Tag: 1, Typ: message.RuntimeFilter_IN, Data: []byte{byte(i)}}, board)
	}
	boards[0].Reset()
	for i := 1; i < len(boards); i++ {
		receiver := message.NewMessageReceiver([]int32{1}, message.AddrBroadCastOnCurrentCN(), boards[i])
		msgs, canceled, err := receiver.ReceiveMessage(false, ctx)
		require.NoError(t, err)
		require.False(t, canceled)
		require.Len(t, msgs, 1)
		require.Equal(t, []byte{byte(i)}, msgs[0].(message.RuntimeFilterMessage).Data)
	}
}
