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
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestSQLReleaseConfigResolvesCertifiedStageAndRejectsDrift(t *testing.T) {
	mp := mpool.MustNewZero()
	const scope = `{"archive_stages":[{"account_id":17,"stage_id":12,` +
		`"canonical_url":"s3://archive/history","provider":"amazon",` +
		`"endpoint":"https://s3.me-south-1.amazonaws.com","region":"me-south-1",` +
		`"credential_handle":"role-arn:arn:aws:iam::17:role/mo-archive",` +
		`"storage_class":"STANDARD","encryption_identity":"kms/archive",` +
		`"versioning_disabled":true,"abort_incomplete_multipart":true}]}`
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{
			{
				contains:  "from mo_catalog.mo_feature_registry",
				accountID: 0,
				result:    lifecycleReleaseResult(t, mp, true, scope),
			},
			{
				contains:  "from mo_catalog.mo_stages",
				accountID: 17,
				result: lifecycleStageResult(
					t,
					mp,
					"s3://archive/history",
					"provider=amazon,endpoint=https://s3.me-south-1.amazonaws.com,aws_region=me-south-1",
					"in_use",
				),
			},
		},
	}
	resolver := SQLReleaseConfig{Executor: fake}
	identity := ArchiveStageIdentity{
		StageID:            12,
		CanonicalURL:       "s3://archive/history",
		Provider:           "amazon",
		CanonicalEndpoint:  "https://s3.me-south-1.amazonaws.com",
		Region:             "me-south-1",
		BucketOrContainer:  "archive",
		ImmutablePrefix:    "history",
		StorageClass:       "STANDARD",
		EncryptionIdentity: "kms/archive",
		CredentialHandle:   "role-arn:arn:aws:iam::17:role/mo-archive",
	}
	digest := ArchiveStageIdentityDigest(identity)
	target, err := resolver.ResolveArchiveTarget(
		context.Background(),
		17,
		12,
		hex.EncodeToString(digest[:]),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(12), target.StageID)
	require.Equal(t, "archive", target.BucketOrContainer)
	require.Equal(t, "history", target.ImmutablePrefix)
	require.Equal(t, identity.CredentialHandle, target.CredentialHandle)
	require.Equal(t, 2, fake.offset)
}

func TestSQLReleaseConfigAcceptsDisabledBootstrapScope(t *testing.T) {
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "from mo_catalog.mo_feature_registry",
			accountID: 0,
			result: lifecycleReleaseResult(
				t,
				mp,
				false,
				`{"archive_stages":[]}`,
			),
		}},
	}
	enabled, err := (SQLReleaseConfig{Executor: fake}).Enabled(
		context.Background(),
	)
	require.NoError(t, err)
	require.False(t, enabled)
}

func TestSQLReleaseConfigRejectsArchiveResolutionWhileDisabledBeforeStageRead(t *testing.T) {
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "from mo_catalog.mo_feature_registry",
			accountID: 0,
			result: lifecycleReleaseResult(
				t,
				mp,
				false,
				`{"archive_stages":[]}`,
			),
		}},
	}
	_, err := (SQLReleaseConfig{Executor: fake}).ResolveArchiveTarget(
		context.Background(),
		17,
		12,
		strings.Repeat("00", sha256.Size),
	)
	require.ErrorContains(t, err, "Lifecycle release is disabled")
	// A disabled release must fail before touching a tenant Stage or its
	// credentials. This keeps the global kill switch free of external I/O.
	require.Equal(t, 1, fake.offset)
}

func TestSQLReleaseConfigRejectsUnknownReleaseScopeFields(t *testing.T) {
	mp := mpool.MustNewZero()
	fake := &scriptedLifecycleSQLExecutor{
		t: t,
		steps: []lifecycleSQLStep{{
			contains:  "from mo_catalog.mo_feature_registry",
			accountID: 0,
			result: lifecycleReleaseResult(
				t,
				mp,
				true,
				`{"archive_stages":[],"unexpected":true}`,
			),
		}},
	}
	_, err := (SQLReleaseConfig{Executor: fake}).Enabled(context.Background())
	require.ErrorContains(t, err, "invalid Lifecycle release scope")
	require.Equal(t, 1, fake.offset)
}

func lifecycleReleaseResult(
	t *testing.T,
	mp *mpool.MPool,
	enabled bool,
	scope string,
) executor.Result {
	t.Helper()
	value := batch.NewWithSize(2)
	value.Vecs[0] = vector.NewVec(types.T_bool.ToType())
	value.Vecs[1] = vector.NewVec(types.T_json.ToType())
	require.NoError(t, vector.AppendFixed(value.Vecs[0], enabled, false, mp))
	encoded, err := types.ParseStringToByteJson(scope)
	require.NoError(t, err)
	require.NoError(t, vector.AppendByteJson(value.Vecs[1], encoded, false, mp))
	value.SetRowCount(1)
	return executor.Result{Batches: []*batch.Batch{value}, Mp: mp}
}

func lifecycleStageResult(
	t *testing.T,
	mp *mpool.MPool,
	stageURL string,
	credentials string,
	status string,
) executor.Result {
	t.Helper()
	value := batch.NewWithSize(3)
	for column, field := range []string{stageURL, credentials, status} {
		value.Vecs[column] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(
			value.Vecs[column],
			[]byte(field),
			false,
			mp,
		))
	}
	value.SetRowCount(1)
	return executor.Result{Batches: []*batch.Batch{value}, Mp: mp}
}
