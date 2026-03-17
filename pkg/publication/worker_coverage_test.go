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
