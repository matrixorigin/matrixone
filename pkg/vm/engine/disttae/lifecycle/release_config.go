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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const lifecycleFeatureCode = "LIFECYCLE"

// ArchiveStageIdentity is the secret-free Stage identity frozen by SET
// LIFECYCLE. Keep its digest format byte-compatible with the frontend.
type ArchiveStageIdentity struct {
	StageID            uint64
	CanonicalURL       string
	Provider           string
	CanonicalEndpoint  string
	Region             string
	BucketOrContainer  string
	ImmutablePrefix    string
	StorageClass       string
	EncryptionIdentity string
	CredentialHandle   string
}

type archiveStageCertification struct {
	AccountID                uint32 `json:"account_id"`
	StageID                  uint64 `json:"stage_id"`
	CanonicalURL             string `json:"canonical_url"`
	Provider                 string `json:"provider"`
	Endpoint                 string `json:"endpoint"`
	Region                   string `json:"region"`
	CredentialHandle         string `json:"credential_handle"`
	StorageClass             string `json:"storage_class,omitempty"`
	EncryptionIdentity       string `json:"encryption_identity,omitempty"`
	VersioningDisabled       bool   `json:"versioning_disabled"`
	AbortIncompleteMultipart bool   `json:"abort_incomplete_multipart"`
}

type lifecycleReleaseConfiguration struct {
	ArchiveStages []archiveStageCertification `json:"archive_stages"`
}

// SQLReleaseConfig reads the existing deployment feature registry and the
// tenant Stage. It deliberately does not persist or resolve inline secrets.
type SQLReleaseConfig struct {
	Executor executor.SQLExecutor
}

func (config SQLReleaseConfig) Enabled(ctx context.Context) (bool, error) {
	enabled, _, err := config.load(ctx)
	return enabled, err
}

func (config SQLReleaseConfig) ResolveArchiveTarget(
	ctx context.Context,
	accountID uint32,
	stageID uint64,
	expectedDigestHex string,
) (FrozenArchiveTarget, error) {
	if config.Executor == nil || accountID == 0 || stageID == 0 {
		return FrozenArchiveTarget{}, moerr.NewInvalidInput(
			ctx,
			"Lifecycle release configuration is incomplete",
		)
	}
	expectedDigest, err := hex.DecodeString(expectedDigestHex)
	if err != nil || len(expectedDigest) != sha256.Size {
		return FrozenArchiveTarget{}, moerr.NewInvalidInput(
			ctx,
			"Lifecycle Binding Stage identity digest is invalid",
		)
	}
	enabled, release, err := config.load(ctx)
	if err != nil {
		return FrozenArchiveTarget{}, err
	}
	if !enabled {
		return FrozenArchiveTarget{}, moerr.NewNotSupported(
			ctx,
			"Lifecycle release is disabled",
		)
	}
	stageURL, credentials, status, err := config.loadStage(ctx, accountID, stageID)
	if err != nil {
		return FrozenArchiveTarget{}, err
	}
	if !strings.EqualFold(status, "in_use") {
		return FrozenArchiveTarget{}, moerr.NewInvalidInput(
			ctx,
			"Lifecycle Archive Stage is no longer in use",
		)
	}
	parsedURL, err := url.Parse(stageURL)
	if err != nil || !strings.EqualFold(parsedURL.Scheme, stage.S3_PROTOCOL) {
		return FrozenArchiveTarget{}, moerr.NewNotSupported(
			ctx,
			"Lifecycle requires an S3-compatible Stage",
		)
	}
	bucket, prefix, _, err := stage.ParseS3Url(parsedURL)
	if err != nil {
		return FrozenArchiveTarget{}, err
	}
	credentialValues, err := stage.CredentialsToMap(credentials)
	if err != nil {
		return FrozenArchiveTarget{}, err
	}
	provider := strings.ToLower(strings.TrimSpace(
		credentialValues[stage.PARAMKEY_PROVIDER],
	))
	endpoint := strings.TrimRight(strings.TrimSpace(
		credentialValues[stage.PARAMKEY_ENDPOINT],
	), "/")
	region := strings.TrimSpace(credentialValues[stage.PARAMKEY_AWS_REGION])

	for _, certification := range release.ArchiveStages {
		if certification.AccountID != accountID ||
			certification.StageID != stageID {
			continue
		}
		if !certification.VersioningDisabled ||
			!certification.AbortIncompleteMultipart {
			return FrozenArchiveTarget{}, moerr.NewNotSupported(
				ctx,
				"Lifecycle Archive Stage does not satisfy the deployment storage contract",
			)
		}
		if certification.CredentialHandle == "" ||
			certification.CanonicalURL != stageURL ||
			!strings.EqualFold(certification.Provider, provider) ||
			strings.TrimRight(certification.Endpoint, "/") != endpoint ||
			certification.Region != region {
			return FrozenArchiveTarget{}, moerr.NewInvalidInput(
				ctx,
				"Lifecycle Archive Stage drifted from deployment certification",
			)
		}
		identity := ArchiveStageIdentity{
			StageID:            stageID,
			CanonicalURL:       stageURL,
			Provider:           provider,
			CanonicalEndpoint:  endpoint,
			Region:             region,
			BucketOrContainer:  bucket,
			ImmutablePrefix:    strings.Trim(prefix, "/"),
			StorageClass:       certification.StorageClass,
			EncryptionIdentity: certification.EncryptionIdentity,
			CredentialHandle:   certification.CredentialHandle,
		}
		digest := ArchiveStageIdentityDigest(identity)
		if !bytes.Equal(digest[:], expectedDigest) {
			return FrozenArchiveTarget{}, moerr.NewInvalidInput(
				ctx,
				"Lifecycle Binding Stage identity no longer matches",
			)
		}
		target := FrozenArchiveTarget{
			FormatVersion:      frozenArchiveTargetVersion,
			StageID:            identity.StageID,
			Provider:           identity.Provider,
			CanonicalEndpoint:  identity.CanonicalEndpoint,
			Region:             identity.Region,
			BucketOrContainer:  identity.BucketOrContainer,
			ImmutablePrefix:    identity.ImmutablePrefix,
			StorageClass:       identity.StorageClass,
			EncryptionIdentity: identity.EncryptionIdentity,
			CredentialHandle:   identity.CredentialHandle,
		}
		if err := target.Validate(); err != nil {
			return FrozenArchiveTarget{}, err
		}
		return target, nil
	}
	return FrozenArchiveTarget{}, moerr.NewNotSupported(
		ctx,
		"Lifecycle Archive Stage is not deployment-certified",
	)
}

func (config SQLReleaseConfig) load(
	ctx context.Context,
) (bool, lifecycleReleaseConfiguration, error) {
	if config.Executor == nil {
		return false, lifecycleReleaseConfiguration{}, moerr.NewInvalidInput(
			ctx,
			"Lifecycle SQL executor is nil",
		)
	}
	result, err := config.Executor.Exec(
		ctx,
		`select enabled,scope_spec from mo_catalog.mo_feature_registry where feature_code = 'LIFECYCLE'`,
		executor.Options{}.WithAccountID(catalog.System_Account),
	)
	if err != nil {
		return false, lifecycleReleaseConfiguration{}, err
	}
	defer result.Close()
	var enabled bool
	var scope string
	rowsRead := 0
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != 2 || rowsRead+rows != 1 {
			decodeErr = moerr.NewInternalErrorNoCtxf("Lifecycle feature registry row is invalid")
			return false
		}
		enabled = vector.GetFixedAtNoTypeCheck[bool](columns[0], 0)
		scope = types.DecodeJson(columns[1].GetBytesAt(0)).String()
		rowsRead += rows
		return true
	})
	if decodeErr != nil {
		return false, lifecycleReleaseConfiguration{}, decodeErr
	}
	if rowsRead == 0 {
		return false, lifecycleReleaseConfiguration{}, nil
	}
	var release lifecycleReleaseConfiguration
	decoder := json.NewDecoder(strings.NewReader(scope))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&release); err != nil {
		return false, lifecycleReleaseConfiguration{}, moerr.NewInvalidInputf(
			ctx,
			"invalid Lifecycle release scope: %v",
			err,
		)
	}
	return enabled, release, nil
}

func (config SQLReleaseConfig) loadStage(
	ctx context.Context,
	accountID uint32,
	stageID uint64,
) (string, string, string, error) {
	result, err := config.Executor.Exec(
		ctx,
		fmt.Sprintf(
			"select url,stage_credentials,stage_status from mo_catalog.mo_stages where stage_id = %d",
			stageID,
		),
		executor.Options{}.WithAccountID(accountID),
	)
	if err != nil {
		return "", "", "", err
	}
	defer result.Close()
	values := [3]string{}
	rowsRead := 0
	var decodeErr error
	result.ReadRows(func(rows int, columns []*vector.Vector) bool {
		if len(columns) != len(values) || rowsRead+rows != 1 {
			decodeErr = moerr.NewInternalErrorNoCtxf("Lifecycle Stage row is invalid")
			return false
		}
		for column := range values {
			values[column] = columns[column].GetStringAt(0)
		}
		rowsRead += rows
		return true
	})
	if decodeErr != nil {
		return "", "", "", decodeErr
	}
	if rowsRead == 0 {
		return "", "", "", moerr.NewInvalidInput(
			ctx,
			"Lifecycle Archive Stage no longer exists",
		)
	}
	return values[0], values[1], values[2], nil
}

// ArchiveStageIdentityDigest is the public worker-side implementation of the
// Binding digest. Changing it requires an explicit format version.
func ArchiveStageIdentityDigest(identity ArchiveStageIdentity) [sha256.Size]byte {
	var value bytes.Buffer
	writeArchiveIdentityString(&value, "mo-lifecycle-stage-identity-v1")
	writeArchiveIdentityUint64(&value, identity.StageID)
	writeArchiveIdentityString(&value, identity.CanonicalURL)
	writeArchiveIdentityString(&value, identity.Provider)
	writeArchiveIdentityString(&value, identity.CanonicalEndpoint)
	writeArchiveIdentityString(&value, identity.Region)
	writeArchiveIdentityString(&value, identity.BucketOrContainer)
	writeArchiveIdentityString(&value, identity.ImmutablePrefix)
	writeArchiveIdentityString(&value, identity.StorageClass)
	writeArchiveIdentityString(&value, identity.EncryptionIdentity)
	writeArchiveIdentityString(&value, identity.CredentialHandle)
	return sha256.Sum256(value.Bytes())
}

func writeArchiveIdentityString(value *bytes.Buffer, field string) {
	writeArchiveIdentityUint32(value, uint32(len(field)))
	value.WriteString(field)
}

func writeArchiveIdentityUint32(value *bytes.Buffer, field uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], field)
	value.Write(encoded[:])
}

func writeArchiveIdentityUint64(value *bytes.Buffer, field uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], field)
	value.Write(encoded[:])
}
