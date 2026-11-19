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

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func TestPublishCatalogUpgradeDecisionsOnDefaultDiskService(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "cn.toml")
	config := []byte("service-type = \"CN\"\ndata-dir = \"" + dir + "\"\n[cn]\nuuid = \"test-cn\"\n")
	require.NoError(t, os.WriteFile(cfg, config, 0600))
	input := filepath.Join(dir, "decisions.ndjson")
	valid := []byte("{\"format\":\"matrixone-catalog-snapshot-decisions-v1\",\"target\":\"4.0.7\"}\n")
	require.NoError(t, os.WriteFile(input, valid, 0600))
	require.NoError(t, publishCatalogUpgradeDecisions(t.Context(), cfg, input))
	fs, err := fileservice.NewLocalFS(context.Background(), "SHARED", filepath.Join(dir, "shared"), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(context.Background())
	read := func() []byte {
		v := fileservice.IOVector{FilePath: bootstrap.SnapshotUpgradeDecisionPath, Policy: fileservice.SkipAllCache, Entries: []fileservice.IOEntry{{Size: -1}}}
		require.NoError(t, fs.Read(t.Context(), &v))
		defer v.Release()
		return append([]byte(nil), v.Entries[0].Data...)
	}
	require.Equal(t, valid, read())
	onDisk, err := os.ReadFile(filepath.Join(dir, "shared", "catalog-upgrade", "4.0.7", "decisions.ndjson"))
	require.NoError(t, err)
	require.NotEqual(t, valid, onDisk, "legacy disk storage includes checksums")
	require.NoError(t, os.WriteFile(input, []byte("invalid json\n"), 0600))
	require.Error(t, publishCatalogUpgradeDecisions(t.Context(), cfg, input))
	require.Equal(t, valid, read(), "invalid input cannot replace the existing decision file")
	updated := append(append([]byte(nil), valid...), []byte(`{"catalog_account_id":"0","snapshot_id":"01a17d34-05f1-752c-acbd-52c7a8fe04fe","name":"__mo_branch_42","timestamp":null,"level":"table","account_name":"sys","database_name":"db","table_name":"t","object_id":"9","prior_kind":"user","edge":null,"decision":"user"}`+"\n")...)
	require.NoError(t, os.WriteFile(input, updated, 0600))
	require.NoError(t, publishCatalogUpgradeDecisions(t.Context(), cfg, input))
	require.Equal(t, updated, read())
	for _, fields := range []string{
		`"decision":"user","Decision":"branch"`,
		`"Decision":"branch","decision":"user"`,
	} {
		bad := strings.Replace(string(updated), `"decision":"user"`, fields, 1)
		require.NoError(t, os.WriteFile(input, []byte(bad), 0600))
		require.ErrorContains(t, publishCatalogUpgradeDecisions(t.Context(), cfg, input), "noncanonical JSON key")
		require.Equal(t, updated, read(), "an ambiguous decision must not replace the prior file")
	}
}
