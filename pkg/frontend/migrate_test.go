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
