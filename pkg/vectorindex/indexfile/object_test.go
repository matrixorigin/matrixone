// Copyright 2026 Matrix Origin
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

package indexfile

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

// An object the cache does not hold opens as nil; one it holds opens held, with its size,
// and stays readable after Close releases the hold.
func TestOpenCachedObject(t *testing.T) {
	cache := newCache(t, t.TempDir())
	ctx := context.Background()
	f, err := OpenCachedObject(ctx, cache, "object")
	require.NoError(t, err)
	require.Nil(t, f)

	data := bytes.Repeat([]byte("entries"), 1000)
	_, err = cache.Put(ctx, fileservice.IndexObjectEntry("object"), []fileservice.IndexCacheFile{{
		Name: fileservice.IndexObjectFile,
		Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil },
	}})
	require.NoError(t, err)

	f, err = OpenCachedObject(ctx, cache, "object")
	require.NoError(t, err)
	require.NotNil(t, f)
	require.Equal(t, int64(len(data)), f.Size())
	require.Equal(t, data, readFile(t, f))
	require.NoError(t, f.Close())

	f, err = OpenCachedObject(ctx, cache, "object")
	require.NoError(t, err)
	require.NotNil(t, f, "the entry stays cached after its hold is released")
	require.NoError(t, f.Close())
}

// Opening a cached object stops with a canceled context.
func TestOpenCachedObjectCanceled(t *testing.T) {
	cache := newCache(t, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f, err := OpenCachedObject(ctx, cache, "object")
	require.Nil(t, f)
	if err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}
}
