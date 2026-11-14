// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embed

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func rebaseCrashLogTail(path string, limit int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return "", err
	}
	start := max(int64(0), size-limit)
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return "", err
	}
	data, err := io.ReadAll(io.LimitReader(file, limit))
	return string(data), err
}

func logRebaseCrashChild(t *testing.T, root, phase string) {
	t.Helper()
	tail, err := rebaseCrashLogTail(filepath.Join(root, phase+".log"), 64<<10)
	if err != nil {
		t.Logf("could not read %s child log: %v", phase, err)
		return
	}
	t.Logf("%s child log tail:\n%s", phase, tail)
}

func TestRebaseCrashLogTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "child.log")
	require.NoError(t, os.WriteFile(path, []byte("start-end"), 0600))
	tail, err := rebaseCrashLogTail(path, 4)
	require.NoError(t, err)
	require.Equal(t, "-end", tail)
	tail, err = rebaseCrashLogTail(path, 20)
	require.NoError(t, err)
	require.Equal(t, "start-end", tail)
	_, err = rebaseCrashLogTail(path+"-missing", 20)
	require.Error(t, err)
}
