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

package toolfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenFromConfigSelectsRequestedFileService(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeConfig(t, `
[[fileservice]]
backend = "DISK"
data-dir = "`+dir+`"
name = "LOCAL"
`)

	fs, display, err := Open(context.Background(), StorageOptions{
		FSConfig: cfgPath,
		FSName:   "LOCAL",
	})
	require.NoError(t, err)
	defer fs.Close(context.Background())

	assert.Contains(t, display, "LOCAL")
	assert.Equal(t, "LOCAL", fs.Name())
}

func TestOpenFromConfigUsesTNStorageFileServiceByDefault(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeConfig(t, `
[[fileservice]]
backend = "DISK"
data-dir = "`+dir+`"
name = "SHARED"

[tn.Txn.Storage]
fileservice = "SHARED"
`)

	fs, _, err := Open(context.Background(), StorageOptions{FSConfig: cfgPath})
	require.NoError(t, err)
	defer fs.Close(context.Background())

	assert.Equal(t, "SHARED", fs.Name())
}

func TestOpenFromConfigFallsBackToDataDir(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeConfig(t, `data-dir = "`+dir+`"`)

	fs, display, err := Open(context.Background(), StorageOptions{FSConfig: cfgPath})
	require.NoError(t, err)
	defer fs.Close(context.Background())

	assert.Equal(t, "SHARED", fs.Name())
	assert.Contains(t, display, dir)
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tn.toml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))
	return path
}
