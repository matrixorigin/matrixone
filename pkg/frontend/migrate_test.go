// Copyright 2021 - 2024 Matrix Origin
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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMigrateController(t *testing.T) {
	mc := newMigrateController()
	assert.NotNil(t, mc)
}

func TestCloseOnly(t *testing.T) {
	mc := newMigrateController()
	assert.NotNil(t, mc)
	mc.waitAndClose()
}

func TestFirstCloseThenMigrate(t *testing.T) {
	mc := newMigrateController()
	assert.NotNil(t, mc)
	mc.waitAndClose()
	assert.Equal(t, mc.beginOperation(), false)
}

func TestFirstMigrateThenClose(t *testing.T) {
	mc := newMigrateController()
	assert.NotNil(t, mc)
	assert.Equal(t, mc.beginOperation(), true)
	mc.endOperation()
	mc.waitAndClose()
}

func TestLifecycleControllerSerializesOperations(t *testing.T) {
	mc := newMigrateController()
	assert.True(t, mc.beginOperation())

	started := make(chan struct{})
	result := make(chan bool)
	go func() {
		close(started)
		ok := mc.beginOperation()
		result <- ok
		if ok {
			mc.endOperation()
		}
	}()
	<-started
	select {
	case <-result:
		t.Fatal("second operation must wait for the active operation")
	case <-time.After(50 * time.Millisecond):
	}
	mc.endOperation()
	assert.True(t, <-result)

	mc.waitAndClose()
	assert.False(t, mc.beginOperation())
}

func TestLifecycleControllerRejectsBusyTryOperation(t *testing.T) {
	mc := newMigrateController()
	assert.True(t, mc.tryBeginOperation())
	assert.False(t, mc.tryBeginOperation())
	mc.endOperation()
	assert.True(t, mc.tryBeginOperation())
	mc.endOperation()
}

func TestLifecycleControllerCloseRejectsQueuedOperation(t *testing.T) {
	mc := newMigrateController()
	assert.True(t, mc.beginOperation())

	closed := make(chan struct{})
	go func() {
		mc.waitAndClose()
		close(closed)
	}()
	assert.Eventually(t, func() bool {
		mc.Lock()
		defer mc.Unlock()
		return mc.closed
	}, time.Second, 10*time.Millisecond)

	assert.False(t, mc.beginOperation())
	mc.endOperation()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close did not finish after the active operation ended")
	}
}

func TestLifecycleControllerCloseCancelsActiveOperation(t *testing.T) {
	mc := newMigrateController()
	operationCtx, ok := mc.beginOperationWithContext(context.Background())
	assert.True(t, ok)

	operationDone := make(chan struct{})
	go func() {
		<-operationCtx.Done()
		mc.endOperation()
		close(operationDone)
	}()

	closeDone := make(chan struct{})
	go func() {
		mc.waitAndClose()
		close(closeDone)
	}()

	select {
	case <-operationCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("close did not cancel the active lifecycle operation")
	}
	select {
	case <-operationDone:
	case <-time.After(time.Second):
		t.Fatal("active lifecycle operation did not finish after cancellation")
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("close did not finish after the active lifecycle operation exited")
	}
	assert.False(t, mc.beginOperation())
}

func TestLifecycleControllerWaitingCallerHonorsContext(t *testing.T) {
	mc := newMigrateController()
	assert.True(t, mc.beginOperation())

	ctx, cancel := context.WithCancel(context.Background())
	waiterReady := make(chan struct{})
	result := make(chan bool)
	go func() {
		close(waiterReady)
		_, ok := mc.beginOperationWithContext(ctx)
		result <- ok
	}()
	<-waiterReady
	cancel()

	select {
	case ok := <-result:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("waiting lifecycle operation ignored caller cancellation")
	}

	mc.endOperation()
	mc.waitAndClose()
}

func TestCloseWaitMigrate(t *testing.T) {
	mc := newMigrateController()
	assert.NotNil(t, mc)
	assert.Equal(t, mc.beginOperation(), true)
	go func() {
		<-time.After(time.Second)
		mc.endOperation()
	}()
	mc.waitAndClose()
}
