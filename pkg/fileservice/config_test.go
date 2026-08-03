// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"context"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	const text = `
bucket = "mo-test"
endpoint = "http://minio:9000"
key-prefix = "server/data"
cert-files = ['/etc/ssl/cert.pem']
  `
	var config Config
	_, err := toml.Decode(text, &config.S3)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(config.S3.CertFiles))
}

func TestNewFileServiceBackendDispatch(t *testing.T) {
	ctx := context.Background()

	t.Run("DISK -> checksummed LocalFS", func(t *testing.T) {
		fs, err := NewFileService(ctx, Config{
			Name:    "LOCAL",
			Backend: "DISK",
			DataDir: t.TempDir(),
			Cache:   DisabledCacheConfig,
		}, nil)
		require.NoError(t, err)
		local, ok := fs.(*LocalFS)
		require.True(t, ok)
		require.False(t, local.noChecksum)
	})

	t.Run("DISK-V2 -> checksum-free LocalFS", func(t *testing.T) {
		fs, err := NewFileService(ctx, Config{
			Name:    "LOCAL",
			Backend: "DISK-V2",
			DataDir: t.TempDir(),
			Cache:   DisabledCacheConfig,
		}, nil)
		require.NoError(t, err)
		local, ok := fs.(*LocalFS)
		require.True(t, ok)
		require.True(t, local.noChecksum)
	})

	t.Run("lowercase disk-v2 also dispatches", func(t *testing.T) {
		fs, err := NewFileService(ctx, Config{
			Name:    "LOCAL",
			Backend: "disk-v2",
			DataDir: t.TempDir(),
			Cache:   DisabledCacheConfig,
		}, nil)
		require.NoError(t, err)
		local, ok := fs.(*LocalFS)
		require.True(t, ok)
		require.True(t, local.noChecksum)
	})

	t.Run("DISK-V2 with empty data dir panics", func(t *testing.T) {
		// matches the DISK backend guard: a disk-backed fs needs a data dir
		require.Panics(t, func() {
			_, _ = NewFileService(ctx, Config{
				Name:    "LOCAL",
				Backend: "DISK-V2",
				Cache:   DisabledCacheConfig,
			}, nil)
		})
	})
}
