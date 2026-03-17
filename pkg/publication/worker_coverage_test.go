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
