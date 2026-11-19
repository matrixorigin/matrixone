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

//go:build receiving_cn_acceptance

package multi_update

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type receivingCNTestTxn struct {
	client.TxnOperator
	id []byte
}

func (t *receivingCNTestTxn) Txn() txn.TxnMeta { return txn.TxnMeta{ID: t.id} }

func TestReceivingCNPlacementScope(t *testing.T) {
	t.Setenv("MO_RECEIVING_CN_TARGETED_FORCE", "1")
	target, other := [16]byte{1}, [16]byte{2}
	stop, err := InstallReceivingCNProbe("scope", target, func(context.Context, ReceivingCNEvent) error { return nil })
	require.NoError(t, err)
	defer stop()
	matching := &process.Process{Base: &process.BaseProcess{TxnOperator: &receivingCNTestTxn{id: target[:]}}}
	nonmatching := &process.Process{Base: &process.BaseProcess{TxnOperator: &receivingCNTestTxn{id: other[:]}}}
	require.True(t, ReceivingCNForceMultiCN(matching))
	require.False(t, ReceivingCNForceMultiCN(nonmatching))
	require.False(t, ReceivingCNForceMultiCN(nil))
	t.Setenv("MO_RECEIVING_CN_TARGETED_FORCE", "0")
	require.False(t, ReceivingCNForceMultiCN(matching))
}

func TestReceivingCNProbeAdmissionAndCancel(t *testing.T) {
	var target, other [16]byte
	target[0], other[0] = 1, 2
	_, err := InstallReceivingCNProbe("", target, func(context.Context, ReceivingCNEvent) error { return nil })
	require.Error(t, err)
	_, err = InstallReceivingCNProbe("run", [16]byte{}, func(context.Context, ReceivingCNEvent) error { return nil })
	require.Error(t, err)
	_, err = InstallReceivingCNProbe("run", target, nil)
	require.Error(t, err)

	var calls atomic.Int32
	reached := make(chan struct{}, 1)
	stop, err := InstallReceivingCNProbe("run-a", target, func(ctx context.Context, e ReceivingCNEvent) error {
		calls.Add(1)
		if e.RunID != "run-a" || e.TxnID != target {
			return errors.New("wrong scope")
		}
		reached <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	require.NoError(t, err)
	defer stop()
	oldGeneration := currentReceivingCNProbe.Load()
	_, err = InstallReceivingCNProbe("run-b", other, func(context.Context, ReceivingCNEvent) error { return nil })
	require.Error(t, err)
	require.NoError(t, receivingCNEmit(context.Background(), other, ReceivingCNEvent{Stage: "sync"}))
	require.Zero(t, calls.Load())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	joined := false
	defer func() {
		cancel()
		if !joined {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("probe callback did not terminate during cleanup")
			}
		}
	}()
	go func() { done <- receivingCNEmit(ctx, target, ReceivingCNEvent{Stage: "sync"}) }()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("target did not reach the barrier")
	}
	cancel()
	select {
	case err = <-done:
		joined = true
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not release the barrier")
	}
	stop()
	stop2, err := InstallReceivingCNProbe("run-b", other, func(context.Context, ReceivingCNEvent) error { calls.Add(1); return nil })
	require.NoError(t, err)
	defer stop2()
	stop() // A stale cleanup must not unregister the newer generation.
	require.NoError(t, receivingCNEmitForProbe(context.Background(), oldGeneration, ReceivingCNEvent{Stage: "sync"}))
	require.EqualValues(t, 1, calls.Load(), "a stale event must not enter the new generation")
	require.NoError(t, receivingCNEmit(context.Background(), other, ReceivingCNEvent{Stage: "sync"}))
	require.EqualValues(t, 2, calls.Load())
}

func TestReceivingCNReceiverLifecycleScope(t *testing.T) {
	target, other := [16]byte{1}, [16]byte{2}
	var events []ReceivingCNEvent
	stop, err := InstallReceivingCNProbe("receiver", target, func(_ context.Context, event ReceivingCNEvent) error {
		events = append(events, event)
		return nil
	})
	require.NoError(t, err)
	defer stop()
	unrelated := &process.Process{Base: &process.BaseProcess{TxnOperator: &receivingCNTestTxn{id: other[:]}}}
	require.Nil(t, ObserveReceivingCNReceiver(unrelated))

	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = &receivingCNTestTxn{id: target[:]}
	finish := ObserveReceivingCNReceiver(proc)
	require.NotNil(t, finish)
	require.Len(t, events, 1)
	require.Equal(t, "receiver-open", events[0].Stage)
	require.NotZero(t, events[0].ReceiverID)
	proc.BuildPipelineContext(proc.GetTopContext())
	defer proc.Cancel(nil)
	child := proc.NewContextChildProc(0)
	defer child.Cancel(nil)
	require.Equal(t, events[0].ReceiverID, receivingCNEvent(child, "sync").ReceiverID)
	finish()
	require.Len(t, events, 2)
	require.Equal(t, "receiver-closed", events[1].Stage)
	require.Equal(t, events[0].ReceiverID, events[1].ReceiverID)

	stop()
	stopNext, err := InstallReceivingCNProbe("next", other, func(_ context.Context, event ReceivingCNEvent) error {
		events = append(events, event)
		return nil
	})
	require.NoError(t, err)
	defer stopNext()
	finish() // The prior generation cannot publish to the replacement probe.
	require.Len(t, events, 2)
}
