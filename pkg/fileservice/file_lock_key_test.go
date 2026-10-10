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

package fileservice

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileLockKeyLocalAliases(t *testing.T) {
	root := t.TempDir()
	fs, err := NewLocalETLFS("one", root)
	require.NoError(t, err)
	defer fs.Close(context.Background())
	alias := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(root, alias))
	other, err := NewLocalETLFS("two", alias)
	require.NoError(t, err)
	defer other.Close(context.Background())
	first, err := FileLockKey(SubPath(fs, "new/dump"), "manifest.json")
	require.NoError(t, err)
	second, err := FileLockKey(SubPath(other, "new/./dump/"), "manifest.json")
	require.NoError(t, err)
	require.Equal(t, first, second)
	third, err := FileLockKey(SubPath(fs, "new/other"), "manifest.json")
	require.NoError(t, err)
	require.NotEqual(t, first, third)
	// A symlink below the ETL root is also an alias of the same destination.
	require.NoError(t, os.MkdirAll(filepath.Join(root, "new/dump"), 0755))
	require.NoError(t, os.Symlink(filepath.Join(root, "new"), filepath.Join(root, "linked")))
	linked, err := FileLockKey(SubPath(fs, "linked/dump"), "manifest.json")
	require.NoError(t, err)
	require.Equal(t, first, linked)
}

func TestFileLockKeyObjectAliases(t *testing.T) {
	fs := &S3FS{name: "one", bucket: "bucket", keyPrefix: "prefix"}
	alias := &S3FS{name: "two", bucket: "bucket", keyPrefix: "prefix/new"}
	first, err := FileLockKey(SubPath(fs, "new/dump/"), "manifest.json")
	require.NoError(t, err)
	second, err := FileLockKey(SubPath(alias, "./dump"), "manifest.json")
	require.NoError(t, err)
	require.Equal(t, first, second)
	other, err := FileLockKey(SubPath(fs, "new/other"), "manifest.json")
	require.NoError(t, err)
	require.NotEqual(t, first, other)
	alias.bucket = "other-bucket"
	other, err = FileLockKey(SubPath(alias, "dump"), "manifest.json")
	require.NoError(t, err)
	require.NotEqual(t, first, other)
}

func TestFileLockKeyHDFSAliases(t *testing.T) {
	fs := &S3FS{name: "one", rawStorage: &HDFS{rootPath: "/root"}, keyPrefix: "new"}
	alias := &S3FS{name: "two", rawStorage: &HDFS{rootPath: "/root/new"}}
	first, err := FileLockKey(SubPath(fs, "dump"), "manifest.json")
	require.NoError(t, err)
	second, err := FileLockKey(SubPath(alias, "dump"), "manifest.json")
	require.NoError(t, err)
	require.Equal(t, first, second)
}
