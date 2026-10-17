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

package spillio

import (
	"context"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

type missingSpillFS struct {
	fileservice.MutableFileService
	calls int
}

func (fs *missingSpillFS) RemoveFile(context.Context, string) error {
	fs.calls++
	return &os.PathError{Op: "remove", Path: "already-gone", Err: os.ErrNotExist}
}

func TestRemoveFileWithRetryAcceptsAlreadyRemovedFile(t *testing.T) {
	fs := &missingSpillFS{}
	require.NoError(t, RemoveFileWithRetry(fs, "already-gone"))
	require.Equal(t, 1, fs.calls)
}
