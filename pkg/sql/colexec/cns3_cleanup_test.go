// Copyright 2021 Matrix Origin
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

package colexec

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

type spillCleanupPartialDeleteFS struct {
	fileservice.FileService
	failed map[string]struct{}
}

func (fs *spillCleanupPartialDeleteFS) Delete(ctx context.Context, paths ...string) error {
	for _, path := range paths {
		if _, ok := fs.failed[path]; ok {
			return errors.New("injected delete failure")
		}
	}
	return fs.FileService.Delete(ctx, paths...)
}

func TestCNSpillCleanupReplaysPastFailedPage(t *testing.T) {
	ctx := t.Context()
	base, err := fileservice.NewMemoryFS("shared", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { base.Close(context.Background()) })
	fs := &spillCleanupPartialDeleteFS{FileService: base, failed: make(map[string]struct{})}
	cleanup := &cnSpillCleanup{shared: fs}

	var lastKey string
	for index := range spillRecordPageSize + 1 {
		id := fmt.Sprintf("00000000-0000-4000-8000-%012x", index)
		key := id + "_00000"
		require.NoError(t, base.Write(ctx, fileservice.IOVector{
			FilePath: key,
			Entries:  []fileservice.IOEntry{{Size: 1, Data: []byte{1}}},
		}))
		require.NoError(t, writeSpillCleanupRecord(ctx, base, spillCleanupDir, spillCleanupRecord{
			name: id + ".names", data: []byte(key + "\n"),
		}))
		if index < spillRecordPageSize {
			fs.failed[key] = struct{}{}
		}
		lastKey = key
	}

	cleanup.sweepListedRecords(ctx, fs, spillCleanupDir, &cleanup.sharedAfter, false)
	_, err = base.StatFile(ctx, lastKey)
	require.NoError(t, err, "a sweep processes one bounded page")

	cleanup.sweepListedRecords(ctx, fs, spillCleanupDir, &cleanup.sharedAfter, false)
	_, err = base.StatFile(ctx, lastKey)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	records, err := fileservice.SortedList(base.List(ctx, spillCleanupDir))
	require.NoError(t, err)
	require.Len(t, records, spillRecordPageSize, "failed delete records remain replayable")
}
