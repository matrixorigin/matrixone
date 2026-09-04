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

package iscp

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// NewSync resolves a LOCAL-fileservice spill dir before handing off to
// hnsw.NewHnswSync. No registered ISCP executor, and a registered executor with no
// LOCAL fileservice, both leave the spill dir empty (meaning $TMPDIR) and reach the
// hand-off. The hand-off reads index metadata over SQL, so these drive it with a
// malformed index def, which NewHnswSync rejects before any SQL runs.
func TestHnswSqlWriterNewSync_SpillDirResolution(t *testing.T) {
	sqlproc := &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{Ctx: context.Background()}}
	service := sqlproc.GetService()

	// No executor: nothing publishes a root FS, so the sync falls back to $TMPDIR.
	_, ok := GetExecutorRuntime(service)
	require.False(t, ok)
	require.Empty(t, resolveHostSpillDir(sqlproc), "no executor means no LOCAL route")

	// An executor with no LOCAL fileservice attached is the same fallback, not a crash.
	iscpExecutors.Store(service, &ISCPTaskExecutor{})
	require.Empty(t, resolveHostSpillDir(sqlproc), "an executor with a nil rootFS still falls back")
	iscpExecutors.Delete(service)

	// The case the routing exists for: a LOCAL fileservice is attached, so the multi-GB model
	// lands under it instead of a possibly-tmpfs $TMPDIR. Asserting only that NewSync fails --
	// which it does on the malformed index def, before the spill dir is read -- would stay green
	// with the whole GetExecutorRuntime block deleted.
	root := t.TempDir()
	local, err := fileservice.NewLocalFS(context.Background(),
		defines.LocalFileServiceName, root, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	iscpExecutors.Store(service, &ISCPTaskExecutor{rootFS: local})
	t.Cleanup(func() { iscpExecutors.Delete(service) })

	dir := resolveHostSpillDir(sqlproc)
	require.NotEmpty(t, dir, "an attached LOCAL fileservice must produce a spill dir")
	require.True(t, strings.HasPrefix(dir, root),
		"and it must sit under the LOCAL root, not $TMPDIR: got %q, root %q", dir, root)
}
