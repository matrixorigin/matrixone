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

func TestFrozenArchiveTargetFailsClosedOnIncompleteOrUnknownIdentity(t *testing.T) {
	valid := FrozenArchiveTarget{
		FormatVersion:     frozenArchiveTargetVersion,
		StageID:           12,
		Provider:          "amazon",
		CanonicalEndpoint: "https://s3.example.com",
		Region:            "me-south-1",
		BucketOrContainer: "archive",
		ImmutablePrefix:   "tenant-17",
		CredentialHandle:  "default",
	}
	tests := []struct {
		name   string
		mutate func(*FrozenArchiveTarget)
	}{
		{name: "format", mutate: func(target *FrozenArchiveTarget) { target.FormatVersion++ }},
		{name: "stage", mutate: func(target *FrozenArchiveTarget) { target.StageID = 0 }},
		{name: "provider", mutate: func(target *FrozenArchiveTarget) { target.Provider = "" }},
		{name: "region", mutate: func(target *FrozenArchiveTarget) { target.Region = "" }},
		{name: "bucket", mutate: func(target *FrozenArchiveTarget) { target.BucketOrContainer = "" }},
		{name: "credential", mutate: func(target *FrozenArchiveTarget) { target.CredentialHandle = "inline-secret" }},
		{name: "prefix traversal", mutate: func(target *FrozenArchiveTarget) { target.ImmutablePrefix = "tenant/../other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := valid
			test.mutate(&target)
			require.Error(t, target.Validate())
			_, err := target.Marshal()
			require.Error(t, err)
		})
	}

	_, err := ParseFrozenArchiveTarget([]byte(`{"format_version":1,"unknown":true}`))
	require.Error(t, err)
	_, err = NewArchiveFileService(context.Background(), FrozenArchiveTarget{})
	require.Error(t, err)
}

func TestArchiveCredentialHandlesMatchSupportedResolvers(t *testing.T) {
	for _, handle := range []string{
		"default",
		"role-arn:arn:aws:iam::17:role/archive",
		"shared-profile:archive",
	} {
		require.NoError(t, ValidateArchiveCredentialHandle(handle))
	}
	for _, handle := range []string{
		"",
		"role-arn:",
		"shared-profile:",
		"inline-secret",
	} {
		require.Error(t, ValidateArchiveCredentialHandle(handle))
	}
}

func TestFileServiceArchiveStoreFailsClosedAtProviderBoundaries(t *testing.T) {
	ctx := context.Background()
	nilStore := FileServiceArchiveStore{}
	require.Error(t, nilStore.Put(ctx, "key", []byte("value")))
	_, err := nilStore.Get(ctx, "key")
	require.Error(t, err)
	_, err = nilStore.Stat(ctx, "key")
	require.Error(t, err)
	_, err = nilStore.GetExact(ctx, "key", 1)
	require.Error(t, err)
	_, err = nilStore.List(ctx, "prefix")
	require.Error(t, err)
	require.Error(t, nilStore.Delete(ctx, "key"))

	fs, err := fileservice.NewMemoryFS(
		"lifecycle-boundary-test",
		fileservice.CacheConfig{},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	store := FileServiceArchiveStore{FileService: fs, MaxListEntries: 1}
	require.NoError(t, store.Put(ctx, "root/a", []byte("a")))
	require.NoError(t, store.Put(ctx, "root/b", []byte("b")))
	value, err := store.Get(ctx, "root/a")
	require.NoError(t, err)
	require.Equal(t, []byte("a"), value)
	_, err = store.GetExact(ctx, "root/a", 0)
	require.Error(t, err)
	_, err = store.List(ctx, "root")
	require.ErrorContains(t, err, "list limit")
}
