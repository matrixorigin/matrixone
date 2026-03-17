// Copyright 2024 Matrix Origin
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

package publication

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ---- PublicationTaskExecutor state methods ----

func TestPublicationTaskExecutor_Resume_NotRunning(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.running = true
	// Resume calls Start, which returns nil immediately when already running
	err := exec.Resume()
	assert.NoError(t, err)
}

func TestPublicationTaskExecutor_Pause(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	// Pause calls Stop, which is a no-op when not running
	err := exec.Pause()
	assert.NoError(t, err)
}

func TestPublicationTaskExecutor_Cancel(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	err := exec.Cancel()
	assert.NoError(t, err)
}

func TestPublicationTaskExecutor_Restart(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	// When not running, Stop is a no-op, then Start hits initStateLocked with nil deps
	// Just verify Stop part works when not running
	exec.Stop()
	assert.False(t, exec.IsRunning())
}

func TestPublicationTaskExecutor_IsRunning_Default(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	assert.False(t, exec.IsRunning())
}

func TestPublicationTaskExecutor_Stop_NotRunning(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.Stop() // should be no-op
	assert.False(t, exec.IsRunning())
}

// ---- fillDefaultOption ----

func TestFillDefaultOption_Nil(t *testing.T) {
	opt := fillDefaultOption(nil)
	assert.NotNil(t, opt)
	assert.True(t, opt.GCInterval > 0)
	assert.True(t, opt.GCTTL > 0)
	assert.True(t, opt.SyncTaskInterval > 0)
	assert.NotNil(t, opt.RetryOption)
	assert.NotNil(t, opt.SQLExecutorRetryOpt)
}

func TestFillDefaultOption_Partial(t *testing.T) {
	opt := fillDefaultOption(&PublicationExecutorOption{
		GCInterval: 1,
	})
	assert.Equal(t, 1, int(opt.GCInterval))
	assert.True(t, opt.GCTTL > 0)
}

// ---- taskEntryLess ----

func TestTaskEntryLess(t *testing.T) {
	a := TaskEntry{TaskID: "aaa"}
	b := TaskEntry{TaskID: "bbb"}
	assert.True(t, taskEntryLess(a, b))
	assert.False(t, taskEntryLess(b, a))
	assert.False(t, taskEntryLess(a, a))
}

// ---- DefaultExecutorRetryOption ----

func TestDefaultExecutorRetryOption(t *testing.T) {
	opt := DefaultExecutorRetryOption()
	assert.NotNil(t, opt)
	assert.True(t, opt.RetryTimes > 0 || opt.RetryTimes == -1)
	assert.True(t, opt.RetryInterval > 0)
	assert.True(t, opt.RetryDuration > 0)
}
