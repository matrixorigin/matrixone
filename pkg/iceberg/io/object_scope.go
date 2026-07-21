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

package icebergio

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const ObjectScopeSignatureAlgorithm = "hmac-sha256-v1"

type ObjectScope struct {
	AccountID           uint32               `json:"account_id"`
	CatalogID           uint64               `json:"catalog_id"`
	TableUUID           string               `json:"table_uuid,omitempty"`
	StorageLocation     string               `json:"storage_location,omitempty"`
	CredentialID        string               `json:"credential_id,omitempty"`
	CredentialExpiresAt time.Time            `json:"credential_expires_at,omitempty"`
	Endpoint            string               `json:"endpoint"`
	Region              string               `json:"region"`
	Bucket              string               `json:"bucket"`
	Principal           string               `json:"principal"`
	Signature           ObjectScopeSignature `json:"signature"`
}

type ObjectScopeSignature struct {
	KeyID     string    `json:"key_id"`
	Algorithm string    `json:"algorithm"`
	ExpiresAt time.Time `json:"expires_at"`
	Digest    string    `json:"digest"`
}

type ObjectScopeSigningKey struct {
	KeyID  string
	Secret []byte
	TTL    time.Duration
}

func SignObjectScope(ctx context.Context, scope ObjectScope, key ObjectScopeSigningKey, now time.Time) (ObjectScope, error) {
	if err := validateObjectScopeSigningKey(ctx, key); err != nil {
		return ObjectScope{}, err
	}
	canonical, err := canonicalizeObjectScope(ctx, scope)
	if err != nil {
		return ObjectScope{}, err
	}
	expiresAt := now.Add(key.TTL)
	canonical.Signature = ObjectScopeSignature{
		KeyID:     key.KeyID,
		Algorithm: ObjectScopeSignatureAlgorithm,
		ExpiresAt: expiresAt.UTC(),
	}
	canonical.Signature.Digest = computeObjectScopeDigest(canonical, key.Secret)
	return canonical, nil
}

func VerifyObjectScopeSignature(ctx context.Context, scope ObjectScope, key ObjectScopeSigningKey, now time.Time) error {
	if err := validateObjectScopeSigningKey(ctx, key); err != nil {
		return err
	}
	canonical, err := canonicalizeObjectScope(ctx, scope)
	if err != nil {
		return err
	}
	if canonical.Signature.Algorithm != ObjectScopeSignatureAlgorithm || canonical.Signature.KeyID != key.KeyID {
		return api.ToMOErr(ctx, api.NewError(api.ErrRemoteSigningDenied, "Iceberg ObjectScope signature metadata does not match verifier", nil))
	}
	if canonical.Signature.ExpiresAt.IsZero() || !now.Before(canonical.Signature.ExpiresAt) {
		return api.ToMOErr(ctx, api.NewError(api.ErrRemoteSigningExpired, "Iceberg ObjectScope signature is expired", nil))
	}
	expected := computeObjectScopeDigest(canonical, key.Secret)
	if !hmac.Equal([]byte(expected), []byte(canonical.Signature.Digest)) {
		return api.ToMOErr(ctx, api.NewError(api.ErrRemoteSigningDenied, "Iceberg ObjectScope signature digest mismatch", nil))
	}
	return nil
}

func EncodeObjectScope(ctx context.Context, scope ObjectScope) ([]byte, error) {
	canonical, err := canonicalizeObjectScope(ctx, scope)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(canonical)
	if err != nil {
		return nil, api.WrapError(api.ErrInternal, "Iceberg ObjectScope serialization failed", nil, err)
	}
	return data, nil
}

func DecodeObjectScope(ctx context.Context, data []byte) (ObjectScope, error) {
	var scope ObjectScope
	if err := json.Unmarshal(data, &scope); err != nil {
		return ObjectScope{}, api.WrapError(api.ErrRemoteSigningDenied, "Iceberg ObjectScope serialization is invalid", nil, err)
	}
	return canonicalizeObjectScope(ctx, scope)
}

func VerifyEncodedObjectScope(ctx context.Context, data []byte, key ObjectScopeSigningKey, now time.Time) (ObjectScope, error) {
	scope, err := DecodeObjectScope(ctx, data)
	if err != nil {
		return ObjectScope{}, err
	}
	if err := VerifyObjectScopeSignature(ctx, scope, key, now); err != nil {
		return ObjectScope{}, err
	}
	return scope, nil
}

func ObjectScopeDigestPayload(scope ObjectScope) string {
	canonical, err := canonicalizeObjectScope(context.Background(), scope)
	if err != nil {
		return ""
	}
	return objectScopeDigestPayload(canonical)
}

func validateObjectScopeSigningKey(ctx context.Context, key ObjectScopeSigningKey) error {
	if strings.TrimSpace(key.KeyID) == "" || len(key.Secret) == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg ObjectScope signing key requires key_id and secret")
	}
	if key.TTL <= 0 {
		return moerr.NewInvalidInput(ctx, "iceberg ObjectScope signing key requires positive ttl")
	}
	return nil
}

func canonicalizeObjectScope(ctx context.Context, scope ObjectScope) (ObjectScope, error) {
	if scope.CatalogID == 0 {
		return ObjectScope{}, moerr.NewInvalidInput(ctx, "iceberg ObjectScope requires catalog_id")
	}
	endpoint, err := canonicalObjectEndpoint(ctx, scope.Endpoint)
	if err != nil {
		return ObjectScope{}, err
	}
	region := strings.ToLower(strings.TrimSpace(scope.Region))
	bucket := strings.TrimSpace(scope.Bucket)
	principal := strings.TrimSpace(scope.Principal)
	if region == "" || bucket == "" || principal == "" {
		return ObjectScope{}, moerr.NewInvalidInput(ctx, "iceberg ObjectScope requires region, bucket, and principal")
	}
	scope.Endpoint = endpoint
	scope.Region = region
	scope.Bucket = bucket
	scope.Principal = principal
	scope.TableUUID = strings.TrimSpace(scope.TableUUID)
	scope.StorageLocation = strings.TrimSpace(scope.StorageLocation)
	scope.CredentialID = strings.TrimSpace(scope.CredentialID)
	scope.CredentialExpiresAt = scope.CredentialExpiresAt.UTC()
	scope.Signature.ExpiresAt = scope.Signature.ExpiresAt.UTC()
	scope.Signature.KeyID = strings.TrimSpace(scope.Signature.KeyID)
	scope.Signature.Algorithm = strings.TrimSpace(scope.Signature.Algorithm)
	scope.Signature.Digest = strings.TrimSpace(scope.Signature.Digest)
	return scope, nil
}

func canonicalObjectEndpoint(ctx context.Context, endpoint string) (string, error) {
	value := strings.TrimSpace(endpoint)
	if value == "" {
		return "", moerr.NewInvalidInput(ctx, "iceberg ObjectScope endpoint is required")
	}
	if strings.Contains(value, "://") {
		u, err := url.Parse(value)
		if err != nil || u.Hostname() == "" {
			return "", moerr.NewInvalidInput(ctx, "iceberg ObjectScope endpoint must be a valid host or URI host")
		}
		if u.Port() != "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return "", moerr.NewInvalidInput(ctx, "iceberg ObjectScope endpoint must not include port, path, query, or fragment")
		}
		value = u.Hostname()
	}
	if strings.Contains(value, "/") {
		return "", moerr.NewInvalidInput(ctx, "iceberg ObjectScope endpoint must not include a path")
	}
	host := strings.ToLower(strings.TrimSuffix(value, "."))
	if host == "" || strings.Contains(host, ":") || net.ParseIP(host) != nil {
		return "", moerr.NewInvalidInput(ctx, "iceberg ObjectScope endpoint must be a DNS host without port")
	}
	return host, nil
}

func computeObjectScopeDigest(scope ObjectScope, secret []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(objectScopeDigestPayload(scope)))
	return hex.EncodeToString(mac.Sum(nil))
}

func objectScopeDigestPayload(scope ObjectScope) string {
	fields := []string{
		strconv.FormatUint(uint64(scope.AccountID), 10),
		strconv.FormatUint(scope.CatalogID, 10),
		scope.TableUUID,
		scope.StorageLocation,
		scope.CredentialID,
		strconv.FormatInt(scope.CredentialExpiresAt.UnixMicro(), 10),
		scope.Endpoint,
		scope.Region,
		scope.Bucket,
		scope.Principal,
		scope.Signature.KeyID,
		scope.Signature.Algorithm,
		strconv.FormatInt(scope.Signature.ExpiresAt.UnixMicro(), 10),
	}
	var payload strings.Builder
	for _, field := range fields {
		payload.WriteString(strconv.Itoa(len(field)))
		payload.WriteByte(':')
		payload.WriteString(field)
		payload.WriteByte(';')
	}
	return payload.String()
}
