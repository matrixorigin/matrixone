// Copyright 2025 Matrix Origin
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

package fileservice

import (
	"context"
	"net/http/httptrace"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestObjectStorageHTTPTraceWriteMultipartParallel(t *testing.T) {
	upstream := &mockParallelObjectStorage{supports: true}
	wrapped := newObjectStorageHTTPTrace(upstream)

	err := wrapped.WriteMultipartParallel(context.Background(), "key", strings.NewReader("data"), nil, nil)
	require.NoError(t, err)
	require.NotNil(t, upstream.ctx)
	require.NotNil(t, httptrace.ContextClientTrace(upstream.ctx))
	require.Equal(t, "key", upstream.key)
}

func TestObjectStorageHTTPTraceWriteMultipartParallelUnsupported(t *testing.T) {
	wrapped := newObjectStorageHTTPTrace(dummyObjectStorage{})

	err := wrapped.WriteMultipartParallel(context.Background(), "key", strings.NewReader("data"), nil, nil)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
}

func TestObjectStorageHTTPTraceDelegates(t *testing.T) {
	upstream := &recordingObjectStorage{}
	wrapped := newObjectStorageHTTPTrace(upstream)
	ctx := context.Background()

	require.NoError(t, wrapped.Delete(ctx, "a"))
	exists, err := wrapped.Exists(ctx, "b")
	require.NoError(t, err)
	require.True(t, exists)
	iterSeq := wrapped.List(ctx, "c")
	var listed []string
	iterSeq(func(entry *DirEntry, err error) bool {
		require.NoError(t, err)
		listed = append(listed, entry.Name)
		return true
	})
	reader, err := wrapped.Read(ctx, "d", nil, nil)
	require.NoError(t, err)
	defer reader.Close()
	buf := make([]byte, 4)
	_, _ = reader.Read(buf)
	size, err := wrapped.Stat(ctx, "e")
	require.NoError(t, err)
	require.Equal(t, int64(3), size)
	require.NoError(t, wrapped.Write(ctx, "f", strings.NewReader("payload"), nil, nil))

	require.Len(t, upstream.calls, 6)
	for _, ctx := range upstream.ctxs {
		require.NotNil(t, httptrace.ContextClientTrace(ctx))
	}
	require.ElementsMatch(t, []string{"delete", "exists", "list", "read", "stat", "write"}, upstream.calls)
	require.Equal(t, []string{"one"}, listed)
}
