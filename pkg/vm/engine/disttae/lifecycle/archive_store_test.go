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

package lifecycle

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

func TestFileServiceArchiveStoreUsesImmutableKeysAndBoundedList(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("lifecycle-test", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	store := FileServiceArchiveStore{FileService: fs, MaxListEntries: 2}
	require.NoError(t, store.Put(ctx, "root/attempt/payload-0-a", []byte("a")))
	size, err := store.Stat(ctx, "root/attempt/payload-0-a")
	require.NoError(t, err)
	require.Equal(t, int64(1), size)
	value, err := store.GetExact(ctx, "root/attempt/payload-0-a", size)
	require.NoError(t, err)
	require.Equal(t, []byte("a"), value)
	_, err = store.GetExact(ctx, "root/attempt/payload-0-a", size+1)
	require.Error(t, err)
	require.NoError(t, store.Put(ctx, "root/attempt/payload-0-a", []byte("a")))
	require.Error(t, store.Put(ctx, "root/attempt/payload-0-a", []byte("changed")))
	require.NoError(t, store.Put(ctx, "root/attempt/nested/payload-1-b", []byte("b")))
	keys, err := store.List(ctx, "root/attempt")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		"root/attempt/payload-0-a",
		"root/attempt/nested/payload-1-b",
	}, keys)
	require.NoError(t, store.Delete(ctx, keys[0]))
	require.NoError(t, store.Delete(ctx, keys[0]))
}

func TestFrozenArchiveTargetContainsOnlyStableDeploymentIdentity(t *testing.T) {
	target := FrozenArchiveTarget{
		FormatVersion:     frozenArchiveTargetVersion,
		StageID:           12,
		Provider:          "amazon",
		CanonicalEndpoint: "https://s3.example.com",
		Region:            "me-south-1",
		BucketOrContainer: "archive",
		ImmutablePrefix:   "tenant-17",
		CredentialHandle:  "role-arn:arn:aws:iam::17:role/archive",
	}
	encoded, err := target.Marshal()
	require.NoError(t, err)
	decoded, err := ParseFrozenArchiveTarget(encoded)
	require.NoError(t, err)
	require.Equal(t, target, decoded)
	require.NotContains(t, string(encoded), "secret")
}

func TestFrozenArchiveTargetRejectsStorageClassNotImplementedByFileService(
	t *testing.T,
) {
	target := FrozenArchiveTarget{
		FormatVersion:     frozenArchiveTargetVersion,
		StageID:           12,
		Provider:          "amazon",
		CanonicalEndpoint: "https://s3.example.com",
		Region:            "me-south-1",
		BucketOrContainer: "archive",
		ImmutablePrefix:   "tenant-17",
		StorageClass:      "GLACIER",
		CredentialHandle:  "default",
	}
	require.ErrorContains(t, target.Validate(), "STANDARD")
}
