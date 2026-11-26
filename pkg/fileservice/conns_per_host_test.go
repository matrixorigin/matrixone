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

package fileservice

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestMaxConnsPerHostBelowMinimumIsRejected(t *testing.T) {
	noProbe := func(context.Context, string) (string, error) { return "", nil }
	for _, tc := range []struct {
		conns int
		ok    bool
	}{{0, true}, {minMaxConnsPerHost, true}, {256, true}, {1, false}, {2, false}, {minMaxConnsPerHost - 1, false}, {-1, false}} {
		args := ObjectStorageArguments{Endpoint: "disk", Bucket: t.TempDir(), MaxConnsPerHost: tc.conns}
		err := args.validateWithRegionProbe(noProbe)
		if tc.ok {
			require.NoError(t, err, "max-conns-per-host=%d", tc.conns)
		} else {
			require.ErrorContains(t, err, "max-conns-per-host", "max-conns-per-host=%d", tc.conns)
		}
	}
	_, err := NewS3FS(context.Background(), ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir(), MaxConnsPerHost: 2},
		CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](0)}, nil, false, true)
	require.ErrorContains(t, err, "max-conns-per-host")
}

func TestDownloadConnsAreHalfTheConfiguredLimit(t *testing.T) {
	ctx := context.Background()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir(), MaxConnsPerHost: 100},
		CacheConfig{DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](64 << 20), MemoryCapacity: ptrTo[toml.ByteSize](0)}, nil, false, false)
	require.NoError(t, err)
	defer fs.Close(ctx)
	require.Equal(t, 100, fs.governor.capacity)
	require.Equal(t, 50, fs.governor.backgroundMax)
}
