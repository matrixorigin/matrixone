// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func TestLegacyDumpTableGCOnUpgrade(t *testing.T) {
	root := t.TempDir()
	dumpDir := "42_" +
		time.Now().UTC().Add(-legacyDumpTableFileTTL-time.Hour).Format(legacyDumpTableTimeLayout) +
		"_1-0"
	legacyDir := filepath.Join(root, legacyDumpTableDir, dumpDir)
	require.NoError(t, os.MkdirAll(legacyDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(legacyDir, "object"), []byte("data"), 0o644))

	fs, err := fileservice.NewTestTmpFileService("tmp", root, 10*time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(func() {
		fs.Close(context.Background())
	})

	require.Eventually(t, func() bool {
		_, err = os.Stat(legacyDir)
		return os.IsNotExist(err)
	}, 5*time.Second, 10*time.Millisecond)
}
