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

package insert

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

type controlledHandoffWorkspace struct {
	client.Workspace
	scope             ioutil.RemoteWorkspaceHandoffScope
	refusal           error
	exposed, retained []string
}

func (w *controlledHandoffWorkspace) SinkerCapacityOption() ioutil.SinkerOption {
	return func(s *ioutil.Sinker) {
		ioutil.WithWorkspaceOwnedCleanup()(s)
		ioutil.WithUncommittedCapacityHook(func(string) error { return w.refusal })(s)
	}
}
func (w *controlledHandoffWorkspace) RemoteWorkspaceHandoffScope() (ioutil.RemoteWorkspaceHandoffScope, bool) {
	return w.scope, true
}
func (w *controlledHandoffWorkspace) MarkRemoteWorkspaceOutputReady(names []string) error {
	w.exposed = append(w.exposed, names...)
	return nil
}
func (w *controlledHandoffWorkspace) RetainUnrecordedS3Objects(names []string, _ string) {
	w.retained = append(w.retained, names...)
}

func TestControlledInsertWorkspaceHandoffAndRefusal(t *testing.T) {
	for _, refuse := range []bool{false, true} {
		t.Run(map[bool]string{false: "handoff", true: "refusal"}[refuse], func(t *testing.T) {
			throttler := &controlledTestThrottler{t: t}
			proc, op, _ := controlledInsertFixture(t, throttler)
			workspace := &controlledHandoffWorkspace{scope: ioutil.RemoteWorkspaceHandoffScope{Version: 1, TxnID: "txn", ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, ProducerCN: "cn"}}
			if refuse {
				workspace.refusal = errors.New("workspace object capacity refusal")
			}
			ctrl := gomock.NewController(t)
			txnOp := mock_frontend.NewMockTxnOperator(ctrl)
			txnOp.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
			txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{Autocommit: true}).AnyTimes()
			proc.Base.TxnOperator = txnOp
			op.AppendChild(controlledInput(t, proc, 2, 8192))
			var outputs int
			for calls := 0; calls < 8; calls++ {
				result, err := op.Call(proc)
				if refuse {
					require.ErrorIs(t, err, workspace.refusal)
					break
				}
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
				scope, present, err := ioutil.ParseRemoteWorkspaceHandoffScope(result.Batch.ExtraBuf)
				require.NoError(t, err)
				require.True(t, present)
				require.Equal(t, workspace.scope, scope)
				outputs++
			}
			if !refuse {
				require.Positive(t, outputs)
				require.Greater(t, len(workspace.exposed), 1, "exercise completed results taken before the final flush")
				op.Reset(proc, false, nil)
				for _, name := range workspace.exposed {
					require.Contains(t, workspace.retained, name, "every emitted object keeps a cleanup owner")
				}
			}
		})
	}
}
