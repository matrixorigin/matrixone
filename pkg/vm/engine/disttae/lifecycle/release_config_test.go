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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

func TestSQLReleaseConfigRejectsInvalidIdentityBeforeCatalogRead(t *testing.T) {
	ctx := context.Background()
	validDigest := strings.Repeat("00", sha256.Size)

	_, err := (SQLReleaseConfig{}).ResolveArchiveTarget(ctx, 17, 12, validDigest)
	require.ErrorContains(t, err, "release configuration is incomplete")

	fake := &scriptedLifecycleSQLExecutor{t: t}
	resolver := SQLReleaseConfig{Executor: fake}
	_, err = resolver.ResolveArchiveTarget(ctx, 0, 12, validDigest)
	require.ErrorContains(t, err, "release configuration is incomplete")
	_, err = resolver.ResolveArchiveTarget(ctx, 17, 0, validDigest)
	require.ErrorContains(t, err, "release configuration is incomplete")
	_, err = resolver.ResolveArchiveTarget(ctx, 17, 12, "not-a-sha256")
	require.ErrorContains(t, err, "Stage identity digest is invalid")
	require.Zero(t, fake.offset, "invalid identity must not read Catalog or Stage credentials")
}

func TestSQLReleaseConfigFailsClosedOnInvalidRegistryRows(t *testing.T) {
	ctx := context.Background()
	_, err := (SQLReleaseConfig{}).Enabled(ctx)
	require.ErrorContains(t, err, "SQL executor is nil")

	expected := moerr.NewInternalErrorNoCtx("feature registry unavailable")
	_, err = (SQLReleaseConfig{Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) {
			return executor.Result{}, expected
		},
	)}).Enabled(ctx)
	require.ErrorIs(t, err, expected)

	enabled, err := (SQLReleaseConfig{Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) {
			return executor.Result{}, nil
		},
	)}).Enabled(ctx)
	require.NoError(t, err)
	require.False(t, enabled, "an absent bootstrap row must keep Lifecycle disabled")

	mp := mpool.MustNewZero()
	invalid := batch.NewWithSize(1)
	invalid.Vecs[0] = vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixed(invalid.Vecs[0], true, false, mp))
	invalid.SetRowCount(1)
	_, err = (SQLReleaseConfig{Executor: executor.NewMemExecutor(
		func(string) (executor.Result, error) {
			return executor.Result{Batches: []*batch.Batch{invalid}, Mp: mp}, nil
		},
	)}).Enabled(ctx)
	require.ErrorContains(t, err, "feature registry row is invalid")
}

func TestSQLReleaseConfigFailsClosedOnStageContractViolations(t *testing.T) {
	const (
		accountID   = uint32(17)
		stageID     = uint64(12)
		stageURL    = "s3://archive/history"
		credentials = "provider=amazon,endpoint=https://s3.me-south-1.amazonaws.com,aws_region=me-south-1"
		certified   = `{"archive_stages":[{"account_id":17,"stage_id":12,` +
			`"canonical_url":"s3://archive/history","provider":"amazon",` +
			`"endpoint":"https://s3.me-south-1.amazonaws.com","region":"me-south-1",` +
			`"credential_handle":"role-arn:archive","versioning_disabled":true,` +
			`"abort_incomplete_multipart":true}]}`
	)
	identity := ArchiveStageIdentity{
		StageID:           stageID,
		CanonicalURL:      stageURL,
		Provider:          "amazon",
		CanonicalEndpoint: "https://s3.me-south-1.amazonaws.com",
		Region:            "me-south-1",
		BucketOrContainer: "archive",
		ImmutablePrefix:   "history",
		CredentialHandle:  "role-arn:archive",
	}
	digest := ArchiveStageIdentityDigest(identity)
	digestHex := hex.EncodeToString(digest[:])

	tests := []struct {
		name       string
		scope      string
		stageURL   string
		status     string
		digestHex  string
		stageEmpty bool
		stageErr   error
		want       string
	}{
		{
			name:       "stage removed",
			scope:      certified,
			stageEmpty: true,
			want:       "Stage no longer exists",
		},
		{
			name:     "stage read failure",
			scope:    certified,
			stageErr: moerr.NewInternalErrorNoCtx("stage catalog unavailable"),
			want:     "stage catalog unavailable",
		},
		{
			name:     "stage no longer active",
			scope:    certified,
			stageURL: stageURL,
			status:   "disabled",
			want:     "Stage is no longer in use",
		},
		{
			name:     "non s3 stage",
			scope:    certified,
			stageURL: "https://archive/history",
			status:   "in_use",
			want:     "requires an S3-compatible Stage",
		},
		{
			name:     "stage is not certified",
			scope:    `{"archive_stages":[]}`,
			stageURL: stageURL,
			status:   "in_use",
			want:     "not deployment-certified",
		},
		{
			name: "provider storage contract missing",
			scope: `{"archive_stages":[{"account_id":17,"stage_id":12,` +
				`"canonical_url":"s3://archive/history","provider":"amazon",` +
				`"endpoint":"https://s3.me-south-1.amazonaws.com","region":"me-south-1",` +
				`"credential_handle":"role-arn:archive"}]}`,
			stageURL: stageURL,
			status:   "in_use",
			want:     "does not satisfy the deployment storage contract",
		},
		{
			name: "certification drift",
			scope: `{"archive_stages":[{"account_id":17,"stage_id":12,` +
				`"canonical_url":"s3://other/history","provider":"amazon",` +
				`"endpoint":"https://s3.me-south-1.amazonaws.com","region":"me-south-1",` +
				`"credential_handle":"role-arn:archive","versioning_disabled":true,` +
				`"abort_incomplete_multipart":true}]}`,
			stageURL: stageURL,
			status:   "in_use",
			want:     "drifted from deployment certification",
		},
		{
			name:      "binding digest mismatch",
			scope:     certified,
			stageURL:  stageURL,
			status:    "in_use",
			digestHex: strings.Repeat("ff", sha256.Size),
			want:      "Stage identity no longer matches",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			stageResult := executor.Result{Mp: mp}
			if !test.stageEmpty && test.stageErr == nil {
				stageResult = lifecycleStageResult(
					t,
					mp,
					test.stageURL,
					credentials,
					test.status,
				)
			}
			fake := &scriptedLifecycleSQLExecutor{
				t: t,
				steps: []lifecycleSQLStep{
					{
						contains:  "from mo_catalog.mo_feature_registry",
						accountID: 0,
						result:    lifecycleReleaseResult(t, mp, true, test.scope),
					},
					{
						contains:  "from mo_catalog.mo_stages",
						accountID: accountID,
						result:    stageResult,
						err:       test.stageErr,
					},
				},
			}
			gotDigest := test.digestHex
			if gotDigest == "" {
				gotDigest = digestHex
			}
			_, err := (SQLReleaseConfig{Executor: fake}).ResolveArchiveTarget(
				context.Background(),
				accountID,
				stageID,
				gotDigest,
			)
			require.ErrorContains(t, err, test.want)
			require.Equal(t, 2, fake.offset)
		})
	}
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
