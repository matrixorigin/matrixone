// Copyright 2021 Matrix Origin
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

package updates

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitForCommitWaiter(t *testing.T, gate *priorityWriterGate) {
	require.Eventually(t, func() bool {
		gate.mu.Lock()
		waiting := gate.commitWaiters != 0
		gate.mu.Unlock()
		runtime.Gosched()
		return waiting
	}, 5*time.Second, time.Millisecond)
}

func waitForAppendWaiter(t *testing.T, gate *priorityWriterGate) {
	require.Eventually(t, func() bool {
		gate.mu.Lock()
		waiting := gate.appendWaiters != 0
		gate.mu.Unlock()
		runtime.Gosched()
		return waiting
	}, 5*time.Second, time.Millisecond)
}

func TestPriorityWriterGateCommitOvertakesAppend(t *testing.T) {
	handle := NewAppendMVCCHandle(nil)
	handle.LockForAppend()

	order := make(chan string, 2)
	commitDone := make(chan struct{})
	go func() {
		handle.LockForCommit()
		order <- "commit"
		handle.UnlockForCommit()
		close(commitDone)
	}()
	waitForCommitWaiter(t, &handle.writerGate)

	appendDone := make(chan struct{})
	go func() {
		handle.LockForAppend()
		order <- "append"
		handle.UnlockForAppend()
		close(appendDone)
	}()

	handle.UnlockForAppend()
	<-commitDone
	<-appendDone
	require.Equal(t, "commit", <-order)
	require.Equal(t, "append", <-order)
}

func TestPriorityWriterGateBoundsCommitPriority(t *testing.T) {
	handle := NewAppendMVCCHandle(nil)
	handle.LockForAppend()

	const commits = maxConsecutiveCommitWriters * 2
	order := make(chan string, commits+1)
	done := make(chan struct{}, commits+1)
	for range commits {
		go func() {
			handle.LockForCommit()
			order <- "commit"
			handle.UnlockForCommit()
			done <- struct{}{}
		}()
	}
	require.Eventually(t, func() bool {
		handle.writerGate.mu.Lock()
		waiting := handle.writerGate.commitWaiters == commits
		handle.writerGate.mu.Unlock()
		return waiting
	}, 5*time.Second, time.Millisecond)

	go func() {
		handle.LockForAppend()
		order <- "append"
		handle.UnlockForAppend()
		done <- struct{}{}
	}()
	waitForAppendWaiter(t, &handle.writerGate)
	handle.UnlockForAppend()

	for range commits + 1 {
		<-done
	}
	close(order)
	position := 0
	for kind := range order {
		if kind == "append" {
			break
		}
		position++
	}
	require.Equal(t, maxConsecutiveCommitWriters, position)
}
