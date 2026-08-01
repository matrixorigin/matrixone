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

package lockservice

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLockServiceTestSocketDirectoriesAreIsolated(t *testing.T) {
	const count = 64

	dirs := make(chan string, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dir, err := createTestSocketDir()
			if err != nil {
				errs <- err
				return
			}
			dirs <- dir
		}()
	}
	wg.Wait()
	close(dirs)
	close(errs)

	seen := make(map[string]struct{}, count)
	for dir := range dirs {
		t.Cleanup(func() {
			_ = removeTestSocketDir(dir)
		})
		_, exists := seen[dir]
		require.False(t, exists, "duplicate test socket directory: %s", dir)
		seen[dir] = struct{}{}
	}
	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, seen, count)

	for dir := range seen {
		require.NoError(t, removeTestSocketDir(dir))
		_, err := os.Stat(dir)
		require.ErrorIs(t, err, os.ErrNotExist)
	}
}
