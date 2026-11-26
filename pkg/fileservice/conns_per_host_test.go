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

// A max-conns-per-host or a concurrency below minRequestRoom is raised to it: the
// governor has room for minRequestRoom requests, half of them background; a negative one
// is rejected.
func TestRoomBelowMinimumIsRaised(t *testing.T) {
	noProbe := func(context.Context, string) (string, error) { return "", nil }
	for _, conns := range []int{-1, 0, 1, 2, minRequestRoom - 1, minRequestRoom, 256} {
		args := ObjectStorageArguments{Endpoint: "disk", Bucket: t.TempDir(), MaxConnsPerHost: conns}
		err := args.validateWithRegionProbe(noProbe)
		if conns < 0 {
			require.ErrorContains(t, err, "max-conns-per-host", "max-conns-per-host=%d", conns)
		} else {
			require.NoError(t, err, "max-conns-per-host=%d", conns)
		}
	}
	for _, tc := range []struct {
		conns       int
		concurrency int64
		room        int
	}{
		{1, 0, minRequestRoom},
		{2, 0, minRequestRoom},
		{minRequestRoom - 1, 0, minRequestRoom},
		{0, 1, minRequestRoom},
		{0, minRequestRoom - 1, minRequestRoom},
		{3, 5, minRequestRoom},
		{256, 7, minRequestRoom},
		{minRequestRoom, 0, minRequestRoom},
		{0, 0, maxConnsPerHost},
	} {
		args := ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir(), MaxConnsPerHost: tc.conns, Concurrency: tc.concurrency}
		raised := args
		raised.raiseRequestRoom()
		if tc.conns > 0 {
			require.Equal(t, max(tc.conns, minRequestRoom), raised.MaxConnsPerHost, "conns %d", tc.conns)
		}
		if tc.concurrency > 0 {
			require.Equal(t, max(tc.concurrency, minRequestRoom), raised.Concurrency, "concurrency %d", tc.concurrency)
		}
		fs, err := NewS3FS(context.Background(), args, CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](0)}, nil, false, true)
		require.NoError(t, err, "conns %d concurrency %d", tc.conns, tc.concurrency)
		require.Equal(t, tc.room, fs.governor.capacity, "conns %d concurrency %d", tc.conns, tc.concurrency)
		require.Equal(t, tc.room/2, fs.governor.backgroundMax, "conns %d concurrency %d", tc.conns, tc.concurrency)
		fs.Close(context.Background())
	}
	_, err := NewS3FS(context.Background(), ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir(), MaxConnsPerHost: -1},
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
