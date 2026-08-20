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

package memory

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

// A nil or LOCAL-less fileservice must yield "" -- os.MkdirTemp reads that as
// $TMPDIR, so the fallback is the pre-change behaviour and no caller needs a branch.
func TestHostSpillDirFallsBackWithoutLocalFS(t *testing.T) {
	require.Equal(t, "", HostSpillDir(context.Background(), nil))
}

// With a LOCAL fileservice the directory is created under its root, so the tars
// land on the provisioned data volume instead of /tmp.
func TestHostSpillDirCreatesUnderLocalRoot(t *testing.T) {
	root := t.TempDir()
	local, err := fileservice.NewLocalFS(context.Background(),
		defines.LocalFileServiceName, root, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	got := HostSpillDir(context.Background(), local)
	require.Equal(t, filepath.Join(root, localSpillSubdir), got)

	fi, err := os.Stat(got)
	require.NoError(t, err, "the directory must be created, not just named")
	require.True(t, fi.IsDir())

	// Idempotent: a second build must not fail on an existing directory.
	require.Equal(t, got, HostSpillDir(context.Background(), local))
}
