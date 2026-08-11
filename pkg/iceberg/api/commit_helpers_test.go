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

package api

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestCommitSnapshotHelpersCloneMutableSummary(t *testing.T) {
	summary := map[string]string{"operation": "append", "added-records": "10"}
	snapshot := NewCommitSnapshot(11, 10, 4, 7, 123456, "s3://warehouse/metadata/snap.avro", summary)
	require.Equal(t, int64(10), *snapshot.ParentSnapshotID)
	require.Equal(t, 7, *snapshot.SchemaID)
	summary["operation"] = "mutated"
	require.Equal(t, "append", snapshot.Summary["operation"])

	update := NewAddSnapshotUpdate(snapshot)
	require.Equal(t, "add-snapshot", update.Type)
	require.NotNil(t, update.Snapshot)
	update.Snapshot.Summary["operation"] = "changed-again"
	require.Equal(t, "append", snapshot.Summary["operation"])

	emptyParent := NewCommitSnapshot(12, 0, 5, 8, 123457, "s3://warehouse/metadata/snap-2.avro", nil)
	require.Nil(t, emptyParent.ParentSnapshotID)
	require.Nil(t, emptyParent.Summary)
}

func TestSetSnapshotRefUpdateDefaultsToBranch(t *testing.T) {
	update := NewSetSnapshotRefUpdate("main", "", 42)
	require.Equal(t, "set-snapshot-ref", update.Type)
	require.Equal(t, "main", update.Ref)
	require.Equal(t, "branch", update.RefType)
	require.Equal(t, int64(42), update.SnapshotID)
	require.Zero(t, update.MinSnapshotsToKeep)

	retained := NewSetSnapshotRefUpdateWithRetention("main", "", 44, 2)
	require.Equal(t, "branch", retained.RefType)
	require.Equal(t, int64(44), retained.SnapshotID)
	require.Equal(t, 2, retained.MinSnapshotsToKeep)

	tag := NewSetSnapshotRefUpdate("release", "tag", 43)
	require.Equal(t, "tag", tag.RefType)

	tagWithRetention := NewSetSnapshotRefUpdateWithRetention("release", "tag", 45, 2)
	require.Equal(t, "tag", tagWithRetention.RefType)
	require.Zero(t, tagWithRetention.MinSnapshotsToKeep)

	preserved := NewSetSnapshotRefUpdatePreservingRetention("main", "branch", 46, SnapshotRef{
		MinSnapshotsToKeep: 3,
		MaxSnapshotAgeMS:   1_000,
		MaxRefAgeMS:        2_000,
	})
	require.Equal(t, 3, preserved.MinSnapshotsToKeep)
	require.Equal(t, int64(1_000), preserved.MaxSnapshotAgeMS)
	require.Equal(t, int64(2_000), preserved.MaxRefAgeMS)

	preservedTag := NewSetSnapshotRefUpdatePreservingRetention("release", "tag", 47, SnapshotRef{
		MinSnapshotsToKeep: 3,
		MaxSnapshotAgeMS:   1_000,
		MaxRefAgeMS:        2_000,
	})
	require.Zero(t, preservedTag.MinSnapshotsToKeep)
	require.Zero(t, preservedTag.MaxSnapshotAgeMS)
	require.Equal(t, int64(2_000), preservedTag.MaxRefAgeMS)
}

func TestRetryPolicyNormalize(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 0, BaseBackoff: -time.Second, MaxBackoff: -time.Second}.Normalize()
	require.Equal(t, 3, policy.MaxAttempts)
	require.Zero(t, policy.BaseBackoff)
	require.Zero(t, policy.MaxBackoff)

	policy = RetryPolicy{MaxAttempts: 2, BaseBackoff: 10 * time.Second, MaxBackoff: time.Second}.Normalize()
	require.Equal(t, 2, policy.MaxAttempts)
	require.Equal(t, time.Second, policy.BaseBackoff)
	require.Equal(t, time.Second, policy.MaxBackoff)
}

func TestErrorWrappingCauseAndMOConversionEdges(t *testing.T) {
	cause := moerr.NewInternalErrorNoCtx("root cause")
	wrapped := WrapError(ErrObjectIO, "object read failed", map[string]string{"path": "s3://bucket/data.parquet"}, cause)
	require.ErrorIs(t, wrapped, cause)

	require.NoError(t, ToMOErr(context.Background(), nil))
	mo := moerr.NewInvalidInputNoCtx("already moerr")
	require.Same(t, mo, ToMOErr(context.Background(), mo))

	plain := ToMOErr(context.Background(), context.Canceled)
	moPlain, ok := plain.(*moerr.Error)
	require.True(t, ok)
	require.Equal(t, moerr.ErrInternal, moPlain.ErrorCode())

	for _, code := range []ErrorCode{ErrMetadataIOTimeout, ErrCommitConflict, ErrRemoteSigningDenied, ErrUnsupportedFeature, ErrInternal} {
		require.NotNil(t, CauseForCode(code), code)
	}
}
