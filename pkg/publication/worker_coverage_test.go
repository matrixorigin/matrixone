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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- filterObjectWorker.SubmitFilterObject closed ----

func TestFilterObjectWorker_SubmitClosed(t *testing.T) {
	w := &filterObjectWorker{
		jobChan: make(chan Job, 10),
	}
	w.closed.Store(true)
	err := w.SubmitFilterObject(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// ---- getChunkWorker.SubmitGetChunk closed ----

func TestGetChunkWorker_SubmitClosed(t *testing.T) {
	w := &getChunkWorker{
		jobChan: make(chan Job, 10),
	}
	w.closed.Store(true)
	err := w.SubmitGetChunk(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// ---- simpleJobWorker.submit closed ----

func TestSimpleJobWorker_SubmitClosed(t *testing.T) {
	w := &simpleJobWorker{
		name:    "TestWorker",
		jobChan: make(chan Job, 10),
	}
	w.closed.Store(true)
	err := w.submit(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// ---- writeObjectWorker.SubmitWriteObject closed ----

func TestWriteObjectWorker_SubmitClosed(t *testing.T) {
	w := &writeObjectWorker{
		simpleJobWorker: &simpleJobWorker{
			name:    "WriteObjectWorker",
			jobChan: make(chan Job, 10),
		},
	}
	w.closed.Store(true)
	err := w.SubmitWriteObject(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// ---- mock job for testing ----

type mockJob struct{}

func (m *mockJob) Execute()      {}
func (m *mockJob) WaitDone() any { return nil }
func (m *mockJob) GetType() int8 { return 0 }

// ---- Worker pool submit success tests ----

func TestFilterObjectWorker_SubmitSuccess(t *testing.T) {
	w := &filterObjectWorker{
		jobChan: make(chan Job, 10),
	}
	err := w.SubmitFilterObject(&mockJob{})
	assert.NoError(t, err)
}

func TestGetChunkWorker_SubmitSuccess(t *testing.T) {
	w := &getChunkWorker{
		jobChan: make(chan Job, 10),
	}
	err := w.SubmitGetChunk(&mockJob{})
	assert.NoError(t, err)
}

func TestWriteObjectWorker_SubmitSuccess(t *testing.T) {
	w := &writeObjectWorker{
		simpleJobWorker: &simpleJobWorker{
			name:      "WriteObjectWorker",
			jobChan:   make(chan Job, 10),
			onPending: func() {},
		},
	}
	err := w.SubmitWriteObject(&mockJob{})
	assert.NoError(t, err)
}

func TestSimpleJobWorker_SubmitSuccess(t *testing.T) {
	w := &simpleJobWorker{
		name:      "TestWorker",
		jobChan:   make(chan Job, 10),
		onPending: func() {},
	}
	err := w.submit(&mockJob{})
	assert.NoError(t, err)
}

// ---- syncProtection tests via worker ----

func TestWorkerSyncProtection_RegisterGetUnregister(t *testing.T) {
	w := &worker{
		syncProtectionJobs: make(map[string]*syncProtectionEntry),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	defer w.cancel()

	// Register
	w.RegisterSyncProtection("job-1", 1000)
	ttl := w.GetSyncProtectionTTL("job-1")
	assert.Equal(t, int64(1000), ttl)

	// Get non-existent
	ttl = w.GetSyncProtectionTTL("job-missing")
	assert.Equal(t, int64(0), ttl)

	// Unregister
	w.UnregisterSyncProtection("job-1")
	ttl = w.GetSyncProtectionTTL("job-1")
	assert.Equal(t, int64(0), ttl)

	// Unregister non-existent should not panic
	w.UnregisterSyncProtection("job-missing")
}

func TestWorkerSyncProtection_UpdateTTL(t *testing.T) {
	w := &worker{
		syncProtectionJobs: make(map[string]*syncProtectionEntry),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	defer w.cancel()

	w.RegisterSyncProtection("job-1", 1000)
	w.RegisterSyncProtection("job-1", 2000) // update
	ttl := w.GetSyncProtectionTTL("job-1")
	assert.Equal(t, int64(2000), ttl)
}

// ---- Worker Submit tests ----

func TestWorker_SubmitClosed(t *testing.T) {
	w := &worker{
		taskChan: make(chan *TaskContext, 10),
	}
	w.closed.Store(true)
	err := w.Submit("task-1", 1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestWorker_SubmitSuccess(t *testing.T) {
	w := &worker{
		taskChan: make(chan *TaskContext, 10),
	}
	err := w.Submit("task-1", 1, 0)
	assert.NoError(t, err)
}

// ---- GetJobStats singleton ----

func TestGetJobStats_NotNil(t *testing.T) {
	stats := GetJobStats()
	assert.NotNil(t, stats)
	stats2 := GetJobStats()
	assert.Same(t, stats, stats2) // singleton
}
