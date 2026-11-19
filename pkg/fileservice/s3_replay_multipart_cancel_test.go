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

package fileservice

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReplayWriteAWSMultipartCancelDoesNotCache(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()
	state.uploadID = "replay-cancel"
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	state.blockUploadPart = release
	state.uploadPartStarted = make(chan struct{})
	fs := newReplayWriteTestFS(t)
	fs.storage = &replaySmallPartsAWS{AwsSDKv2: newTestAWSClient(t, server)}
	fs.parallelMode = ParallelForce

	data := bytes.Repeat([]byte("abcdefgh"), int((minMultipartPartSize+(128<<10))/8))
	vector := IOVector{FilePath: "multipart-cancel", Entries: []IOEntry{{Offset: 0, Size: int64(len(data)), Data: data}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- writeReplayableData(ctx, fs, vector) }()
	select {
	case <-state.uploadPartStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("multipart upload did not reach the local server")
	}
	cancel()
	unblock()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("multipart cancellation did not return")
	}
	_, err := os.Stat(fs.diskCache.pathForFile("multipart-cancel"))
	require.True(t, os.IsNotExist(err), "canceled upload must not publish a disk-cache entry")
}
