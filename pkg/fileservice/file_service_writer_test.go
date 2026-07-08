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

package fileservice

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

// TestFileServiceWriterCloseAndAbort: Close finalizes the file; Abort discards
// the partial output instead of persisting it, and is idempotent / safe after
// Close as documented.
func TestFileServiceWriterCloseAndAbort(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Close persists what was written.
	closed := filepath.Join(dir, "closed.csv")
	w, err := NewFileServiceWriter(closed, ctx)
	require.NoError(t, err)
	_, err = w.Write([]byte("a,b\n"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	data, err := os.ReadFile(closed)
	require.NoError(t, err)
	require.Equal(t, "a,b\n", string(data))

	// Abort discards the partial output: the target file never appears.
	aborted := filepath.Join(dir, "aborted.csv")
	w2, err := NewFileServiceWriter(aborted, ctx)
	require.NoError(t, err)
	_, err = w2.Write([]byte("partial"))
	require.NoError(t, err)
	w2.Abort(moerr.NewInternalErrorNoCtx("test abort"))
	_, statErr := os.Stat(aborted)
	require.True(t, os.IsNotExist(statErr), "aborted file must not be persisted")

	// Idempotent, and safe after the fields were nilled.
	w2.Abort(moerr.NewInternalErrorNoCtx("again"))

	// Safe after Close too.
	w3, err := NewFileServiceWriter(filepath.Join(dir, "both.csv"), ctx)
	require.NoError(t, err)
	_, err = w3.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, w3.Close())
	w3.Abort(moerr.NewInternalErrorNoCtx("after close"))
}
