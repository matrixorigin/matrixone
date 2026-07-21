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
	"strings"
	"testing"
	"time"
)

func TestObjectScopeSignatureRoundTripAndTamperDetection(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 8, 0, 0, 0, time.UTC)
	key := ObjectScopeSigningKey{
		KeyID:  "ksa-key-1",
		Secret: []byte("test-secret"),
		TTL:    time.Minute,
	}
	scope := ObjectScope{
		AccountID:           42,
		CatalogID:           7,
		TableUUID:           "tbl-uuid",
		StorageLocation:     "s3://gold/table/data.parquet",
		CredentialID:        "cred-1",
		CredentialExpiresAt: now.Add(10 * time.Minute),
		Endpoint:            "HTTPS://S3.ME-CENTRAL-1.AMAZONAWS.COM",
		Region:              "ME-CENTRAL-1",
		Bucket:              "gold",
		Principal:           "ksa-analytics",
	}
	signed, err := SignObjectScope(ctx, scope, key, now)
	if err != nil {
		t.Fatalf("sign ObjectScope: %v", err)
	}
	if signed.Endpoint != "s3.me-central-1.amazonaws.com" || signed.Region != "me-central-1" {
		t.Fatalf("ObjectScope was not canonicalized: %+v", signed)
	}
	if err := VerifyObjectScopeSignature(ctx, signed, key, now.Add(30*time.Second)); err != nil {
		t.Fatalf("verify ObjectScope: %v", err)
	}
	signed.Bucket = "silver"
	if err := VerifyObjectScopeSignature(ctx, signed, key, now.Add(30*time.Second)); err == nil || !strings.Contains(err.Error(), "ICEBERG_REMOTE_SIGNING_DENIED") {
		t.Fatalf("expected tamper denial, got %v", err)
	}
}

func TestObjectScopeAllowsSystemAccount(t *testing.T) {
	ctx := context.Background()
	key := ObjectScopeSigningKey{KeyID: "key", Secret: []byte("secret"), TTL: time.Minute}
	scope := ObjectScope{
		AccountID:       0,
		CatalogID:       7,
		StorageLocation: "s3://mo-iceberg/warehouse/table/data.parquet",
		Endpoint:        "localhost",
		Region:          "us-east-1",
		Bucket:          "mo-iceberg",
		Principal:       "local-tier-a",
	}
	if _, err := SignObjectScope(ctx, scope, key, time.Now()); err != nil {
		t.Fatalf("system account ObjectScope should be signable: %v", err)
	}
}

func TestObjectScopeSignatureExpires(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 8, 0, 0, 0, time.UTC)
	key := ObjectScopeSigningKey{KeyID: "key", Secret: []byte("secret"), TTL: time.Second}
	scope := ObjectScope{
		AccountID: 1,
		CatalogID: 2,
		Endpoint:  "s3.me-central-1.amazonaws.com",
		Region:    "me-central-1",
		Bucket:    "gold",
		Principal: "external",
	}
	signed, err := SignObjectScope(ctx, scope, key, now)
	if err != nil {
		t.Fatalf("sign ObjectScope: %v", err)
	}
	if err := VerifyObjectScopeSignature(ctx, signed, key, now.Add(2*time.Second)); err == nil || !strings.Contains(err.Error(), "ICEBERG_REMOTE_SIGNING_EXPIRED") {
		t.Fatalf("expected expired signature, got %v", err)
	}
}

func TestObjectScopeDigestPayloadUsesLengthPrefix(t *testing.T) {
	base := ObjectScope{
		AccountID: 1,
		CatalogID: 2,
		Endpoint:  "s3.me-central-1.amazonaws.com",
		Region:    "me-central-1",
		Bucket:    "gold",
		Principal: "external",
	}
	left := base
	left.TableUUID = "a"
	left.StorageLocation = "b\nc"
	right := base
	right.TableUUID = "a\nb"
	right.StorageLocation = "c"

	if ObjectScopeDigestPayload(left) == ObjectScopeDigestPayload(right) {
		t.Fatalf("expected length-prefixed ObjectScope payloads to be unambiguous")
	}
}

func TestEncodedObjectScopeRemoteVerification(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 8, 0, 0, 0, time.UTC)
	key := ObjectScopeSigningKey{KeyID: "key", Secret: []byte("secret"), TTL: time.Minute}
	scope := ObjectScope{
		AccountID:           1,
		CatalogID:           2,
		TableUUID:           "tbl",
		StorageLocation:     "s3://warehouse/t/data.parquet",
		CredentialID:        "cred",
		CredentialExpiresAt: now.Add(10 * time.Minute),
		Endpoint:            "HTTPS://S3.ME-CENTRAL-1.AMAZONAWS.COM",
		Region:              "ME-CENTRAL-1",
		Bucket:              "warehouse",
		Principal:           "external",
	}
	signed, err := SignObjectScope(ctx, scope, key, now)
	if err != nil {
		t.Fatalf("sign scope: %v", err)
	}
	data, err := EncodeObjectScope(ctx, signed)
	if err != nil {
		t.Fatalf("encode scope: %v", err)
	}
	verified, err := VerifyEncodedObjectScope(ctx, data, key, now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("verify encoded scope: %v", err)
	}
	if verified.Endpoint != "s3.me-central-1.amazonaws.com" || verified.Region != "me-central-1" {
		t.Fatalf("decoded scope not canonicalized: %+v", verified)
	}
	tampered := strings.Replace(string(data), "warehouse", "silver", 1)
	if _, err := VerifyEncodedObjectScope(ctx, []byte(tampered), key, now.Add(10*time.Second)); err == nil || !strings.Contains(err.Error(), "ICEBERG_REMOTE_SIGNING_DENIED") {
		t.Fatalf("expected tampered encoded scope to be denied, got %v", err)
	}
}

func TestObjectScopeValidationRejectsBadKeysAndEndpoints(t *testing.T) {
	ctx := context.Background()
	scope := ObjectScope{
		AccountID: 1,
		CatalogID: 2,
		Endpoint:  "s3.me-central-1.amazonaws.com",
		Region:    "me-central-1",
		Bucket:    "warehouse",
		Principal: "external",
	}
	if _, err := SignObjectScope(ctx, scope, ObjectScopeSigningKey{KeyID: "", Secret: []byte("secret"), TTL: time.Minute}, time.Now()); err == nil {
		t.Fatalf("expected missing key id to fail")
	}
	if _, err := SignObjectScope(ctx, scope, ObjectScopeSigningKey{KeyID: "key", Secret: nil, TTL: time.Minute}, time.Now()); err == nil {
		t.Fatalf("expected missing secret to fail")
	}
	if _, err := SignObjectScope(ctx, scope, ObjectScopeSigningKey{KeyID: "key", Secret: []byte("secret"), TTL: 0}, time.Now()); err == nil {
		t.Fatalf("expected non-positive ttl to fail")
	}

	key := ObjectScopeSigningKey{KeyID: "key", Secret: []byte("secret"), TTL: time.Minute}
	for _, endpoint := range []string{
		"",
		"https://s3.me-central-1.amazonaws.com:9000",
		"https://s3.me-central-1.amazonaws.com/path",
		"https://s3.me-central-1.amazonaws.com?query=1",
		"s3.me-central-1.amazonaws.com/path",
		"127.0.0.1",
	} {
		bad := scope
		bad.Endpoint = endpoint
		if _, err := SignObjectScope(ctx, bad, key, time.Now()); err == nil {
			t.Fatalf("expected endpoint %q to fail", endpoint)
		}
	}

	invalid := scope
	invalid.CatalogID = 0
	if _, err := SignObjectScope(ctx, invalid, key, time.Now()); err == nil {
		t.Fatalf("expected missing catalog id to fail")
	}
	invalid = scope
	invalid.Region = ""
	if _, err := SignObjectScope(ctx, invalid, key, time.Now()); err == nil {
		t.Fatalf("expected missing region to fail")
	}
}

func TestObjectScopeSignatureMetadataMismatch(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 8, 0, 0, 0, time.UTC)
	key := ObjectScopeSigningKey{KeyID: "key", Secret: []byte("secret"), TTL: time.Minute}
	scope := ObjectScope{
		AccountID: 1,
		CatalogID: 2,
		Endpoint:  "s3.me-central-1.amazonaws.com",
		Region:    "me-central-1",
		Bucket:    "warehouse",
		Principal: "external",
	}
	signed, err := SignObjectScope(ctx, scope, key, now)
	if err != nil {
		t.Fatalf("sign scope: %v", err)
	}
	signed.Signature.Algorithm = "other"
	if err := VerifyObjectScopeSignature(ctx, signed, key, now); err == nil || !strings.Contains(err.Error(), "ICEBERG_REMOTE_SIGNING_DENIED") {
		t.Fatalf("expected algorithm mismatch denial, got %v", err)
	}
	signed.Signature.Algorithm = ObjectScopeSignatureAlgorithm
	signed.Signature.KeyID = "other"
	if err := VerifyObjectScopeSignature(ctx, signed, key, now); err == nil || !strings.Contains(err.Error(), "ICEBERG_REMOTE_SIGNING_DENIED") {
		t.Fatalf("expected key id mismatch denial, got %v", err)
	}
}
