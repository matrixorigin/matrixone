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
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// Reduce the part size only for this local-server test so two real AWS SDK
// multipart requests do not require a 64 MiB object.
type replaySmallPartsAWS struct{ *AwsSDKv2 }

func (s *replaySmallPartsAWS) WriteMultipartParallel(ctx context.Context, key string, r io.Reader, size *int64, options *ParallelMultipartOption) error {
	local := *options
	local.PartSize = minMultipartPartSize
	local.Concurrency = 2
	return s.AwsSDKv2.WriteMultipartParallel(ctx, key, r, size, &local)
}

func TestReplayWriteAWSMultipartAndCache(t *testing.T) {
	server, state := newMockAWSServer(t, 0)
	defer server.Close()
	state.uploadID = "replay-multipart"
	fs := newReplayWriteTestFS(t)
	fs.storage = &replaySmallPartsAWS{AwsSDKv2: newTestAWSClient(t, server)}
	fs.parallelMode = ParallelForce

	data := bytes.Repeat([]byte("abcdefgh"), int((minMultipartPartSize+(128<<10))/8))
	want := bytes.Clone(data)
	boundary := int(minMultipartPartSize)
	vector := IOVector{FilePath: "multipart-real", Entries: []IOEntry{
		{Offset: 0, Size: int64(boundary - 7), Data: data[:boundary-7]},
		{Offset: int64(boundary - 7), Size: 9, Data: data[boundary-7 : boundary+2]},
		{Offset: int64(boundary + 2), Size: int64(len(data) - boundary - 2), Data: data[boundary+2:]},
	}}
	require.NoError(t, writeReplayableData(context.Background(), fs, vector))
	clear(data)

	state.mu.Lock()
	count := len(state.parts)
	first := bytes.Clone(state.parts[1])
	second := bytes.Clone(state.parts[2])
	completed := len(state.completeBody) > 0
	state.mu.Unlock()
	require.Equal(t, 2, count)
	require.True(t, completed)
	require.EqualValues(t, minMultipartPartSize, len(first))
	require.Equal(t, want, append(first, second...))
	require.Equal(t, want, readReplayCache(t, fs, "multipart-real"))
}
