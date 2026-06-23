// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pSpool

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// TestPipelineSpoolCloseDoesNotBlockOnEarlyExitedReceiver reproduces issue #25025:
// when a consumer receiver bails early (ctx cancelled) without draining the
// End-message from the spool, PipelineSpool.Close() must NOT block forever
// waiting for its csDoneSignal.
//
// The deadlock chain from the production goroutine dump:
//
//	pSpool.PipelineSpool.Close()        <-csDoneSignal 永久阻塞
//	  ← dispatch.Dispatch.Reset()
//	  ← pipeline.CleanRootOperator
//	  ← Scope.RemoteRun (cleanup)
//	  ← MergeRun wg.Wait (never returns)
//	  ← runOnce (query hangs forever)
//
// This test simulates the leaf cause: one receiver drains the End-message
// while the other has already exited (ctx cancelled) without draining.
func TestPipelineSpoolCloseDoesNotBlockOnEarlyExitedReceiver(t *testing.T) {
	mp := mpool.MustNewZero()

	// 2 receivers — one will drain, one simulates early bail (ctx cancelled).
	sp := InitMyPipelineSpool(mp, 2)

	// Send one real batch first, then the End batch (nil data).
	// Both are sent to all receivers via SendToAllLocal.
	_, err := sp.SendBatch(context.Background(), SendToAllLocal, nil, nil)
	if err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}

	// Receiver 0 drains normally: receives the End batch → triggers csDoneSignal.
	data, info := sp.ReceiveBatch(0)
	if data != nil || info != nil {
		t.Fatalf("expected nil End batch, got data=%v err=%v", data, info)
	}

	// Receiver 1 is NEVER called to ReceiveBatch.
	// This simulates a consumer whose ctx was cancelled:
	// in process_spoolr.go GetNextBatch(), usrCtx.Done() fires,
	// the receiver returns nil,nil WITHOUT calling ReceiveBatch,
	// and csDoneSignal is never sent for this receiver.

	// Close(ctx) must NOT block forever.
	// Use a context with a generous timeout to distinguish "slow" from "stuck".
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	closeReturned := make(chan struct{}, 1)
	go func() {
		sp.Close(ctx)
		close(closeReturned)
	}()

	select {
	case <-closeReturned:
		// Success — Close() returned within the timeout.
	case <-time.After(5 * time.Second):
		// Close() is still blocking — the deadlock is reproduced.
		// Dump goroutines to confirm we're stuck on csDoneSignal.
		t.Fatal("PipelineSpool.Close() blocked for 5s — " +
			"deadlock reproduced (issue #25025): Close() is stuck waiting for " +
			"csDoneSignal from a receiver that will never send it.\n" +
			"This is the leaf cause of the production hang.")
	}
}

// TestPipelineSpoolCloseAllReceiversDrain confirms the normal path:
// when all receivers drain the End-message, Close() returns immediately.
func TestPipelineSpoolCloseAllReceiversDrain(t *testing.T) {
	mp := mpool.MustNewZero()

	sp := InitMyPipelineSpool(mp, 2)

	// Send End batch to all.
	_, err := sp.SendBatch(context.Background(), SendToAllLocal, nil, nil)
	if err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}

	// Both receivers drain.
	for i := 0; i < 2; i++ {
		data, info := sp.ReceiveBatch(i)
		if data != nil || info != nil {
			t.Fatalf("receiver %d: expected nil End batch, got data=%v err=%v", i, data, info)
		}
	}

	// Close should return quickly — all signals sent.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	closeReturned := make(chan struct{}, 1)
	go func() {
		sp.Close(ctx)
		close(closeReturned)
	}()

	select {
	case <-closeReturned:
		// Expected.
	case <-time.After(3 * time.Second):
		t.Fatal("PipelineSpool.Close() blocked even though all receivers drained")
	}
}

// TestPipelineSpoolCloseOneEarlyBailMultiReceiver verifies the fix for
// the asymmetric teardown scenario: 3 receivers, 1 bails early, 2 drain.
func TestPipelineSpoolCloseOneEarlyBailMultiReceiver(t *testing.T) {
	mp := mpool.MustNewZero()

	const numReceivers = 3
	sp := InitMyPipelineSpool(mp, numReceivers)

	// Send End batch to all.
	_, err := sp.SendBatch(context.Background(), SendToAllLocal, nil, nil)
	if err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}

	// Only receivers 0 and 2 drain. Receiver 1 simulates early bail.
	for _, idx := range []int{0, 2} {
		data, info := sp.ReceiveBatch(idx)
		if data != nil || info != nil {
			t.Fatalf("receiver %d: expected nil End batch, got data=%v err=%v", idx, data, info)
		}
	}
	// Receiver 1 never drains.

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	closeReturned := make(chan struct{}, 1)
	go func() {
		sp.Close(ctx)
		close(closeReturned)
	}()

	select {
	case <-closeReturned:
		// Success.
	case <-time.After(5 * time.Second):
		t.Fatal("PipelineSpool.Close() blocked when 1 of 3 receivers bailed early — " +
			"asymmetric teardown deadlock (issue #25025)")
	}
}
