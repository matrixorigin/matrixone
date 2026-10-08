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

package spillio

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSequentialWriteCacheBoundsRetainedTail(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "spill-cache-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	cache := SequentialWriteCache{}
	payload := make([]byte, 64<<10)
	for range 8 {
		written, writeErr := file.Write(payload)
		require.NoError(t, writeErr)
		require.NoError(t, cache.RecordWrite(file, written))
	}
	require.Equal(t, int64(512<<10), cache.written)
	require.Equal(t, int64(256<<10), cache.dropped)

	cache.Finish(file)
	require.Equal(t, cache.written, cache.dropped)
	cache.Finish(file)
}

func TestSequentialWriteCacheRejectsInvalidProgress(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "spill-cache-invalid-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	cache := SequentialWriteCache{}
	require.ErrorIs(t, cache.RecordWrite(nil, 1), errInvalidSequentialWrite)
	require.ErrorIs(t, cache.RecordWrite(file, 0), errInvalidSequentialWrite)
}
