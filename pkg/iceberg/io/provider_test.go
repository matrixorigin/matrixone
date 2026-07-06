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

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestScopedProviderResolveAndRefresh(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	fs, err := fileservice.NewMemoryFS("iceberg-test", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	provider := ScopedProvider{
		FileService: fs,
		Now:         func() time.Time { return now },
		MinTTL:      time.Minute,
		RefreshFunc: func(ctx context.Context, scope ObjectScope) (ObjectScope, error) {
			scope.CredentialID = "cred-2"
			scope.CredentialExpiresAt = now.Add(10 * time.Minute)
			return scope, nil
		},
	}
	scope := ObjectScope{
		AccountID:           1,
		CatalogID:           2,
		StorageLocation:     "s3://warehouse/t/data.parquet",
		CredentialID:        "cred-1",
		CredentialExpiresAt: now.Add(5 * time.Minute),
		Endpoint:            "s3.me-central-1.amazonaws.com",
		Region:              "me-central-1",
		Bucket:              "warehouse",
		Principal:           "external",
	}
	resolvedFS, readPath, err := provider.Resolve(ctx, scope)
	if err != nil {
		t.Fatalf("resolve scope: %v", err)
	}
	if resolvedFS == nil || readPath != scope.StorageLocation {
		t.Fatalf("unexpected resolve result fs=%v path=%q", resolvedFS, readPath)
	}
	refreshed, err := provider.Refresh(ctx, scope)
	if err != nil {
		t.Fatalf("refresh scope: %v", err)
	}
	if refreshed.CredentialID != "cred-2" {
		t.Fatalf("refresh did not update credential: %+v", refreshed)
	}
}

func TestObjectIORegistryResolveAndRelease(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-registry", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	ref, err := RegisterObjectIOProvider(ctx, ScopedProvider{FileService: fs}, func(location string) ObjectScope {
		return ObjectScope{
			AccountID:       42,
			CatalogID:       7,
			StorageLocation: "data/orders.parquet",
			Endpoint:        "s3.me-central-1.amazonaws.com",
			Region:          "me-central-1",
			Bucket:          "warehouse",
			Principal:       "ksa-analytics",
		}
	}, time.Minute)
	if err != nil {
		t.Fatalf("register object io provider: %v", err)
	}
	defer ReleaseObjectIORef(ref)

	resolvedFS, readPath, err := ResolveObjectIORef(ctx, ref, "s3://warehouse/orders.parquet")
	if err != nil {
		t.Fatalf("resolve object io ref: %v", err)
	}
	if resolvedFS != fs || readPath != "data/orders.parquet" {
		t.Fatalf("unexpected registry resolve result fs=%v path=%q", resolvedFS, readPath)
	}

	ReleaseObjectIORef(ref)
	_, _, err = ResolveObjectIORef(ctx, ref, "s3://warehouse/orders.parquet")
	if err == nil || !strings.Contains(err.Error(), string(api.ErrObjectIO)) {
		t.Fatalf("expected released object io ref error, got %v", err)
	}
}

func TestObjectIORegistryRetainedRefSurvivesSweep(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-registry-retained", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	scopeForLocation := func(location string) ObjectScope {
		return ObjectScope{
			AccountID:       42,
			CatalogID:       7,
			StorageLocation: location,
			Endpoint:        "s3.me-central-1.amazonaws.com",
			Region:          "me-central-1",
			Bucket:          "warehouse",
			Principal:       "ksa-analytics",
		}
	}
	unretained, err := RegisterObjectIOProvider(ctx, ScopedProvider{FileService: fs}, scopeForLocation, time.Nanosecond)
	if err != nil {
		t.Fatalf("register unretained object io provider: %v", err)
	}
	retained, err := RegisterObjectIOProvider(ctx, ScopedProvider{FileService: fs}, scopeForLocation, time.Minute)
	if err != nil {
		t.Fatalf("register retained object io provider: %v", err)
	}
	if !RetainObjectIORef(retained) {
		t.Fatalf("expected retain to succeed for registered ref")
	}
	defer ReleaseObjectIORef(retained)

	SweepExpiredObjectIORefs(time.Now().Add(time.Minute))
	if _, _, err := ResolveObjectIORef(ctx, unretained, "s3://warehouse/orders.parquet"); err == nil {
		t.Fatalf("expected expired object io ref to be removed")
	}
	if resolvedFS, _, err := ResolveObjectIORef(ctx, retained, "s3://warehouse/orders.parquet"); err != nil || resolvedFS != fs {
		t.Fatalf("retained object io ref should survive sweep, fs=%v err=%v", resolvedFS, err)
	}
}

func TestObjectIORegistryRetainReleaseRefCounts(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-registry-refcount", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	scopeForLocation := func(location string) ObjectScope {
		return ObjectScope{
			CatalogID:       7,
			StorageLocation: location,
			Endpoint:        "s3.me-central-1.amazonaws.com",
			Region:          "me-central-1",
			Bucket:          "warehouse",
			Principal:       "ksa-analytics",
		}
	}
	ref, err := RegisterObjectIOProvider(ctx, ScopedProvider{FileService: fs}, scopeForLocation, time.Minute)
	if err != nil {
		t.Fatalf("register object io provider: %v", err)
	}
	if !RetainObjectIORef(ref) || !RetainObjectIORef(ref) {
		t.Fatalf("expected retain to succeed for registered ref")
	}
	ReleaseObjectIORef(ref)
	if resolvedFS, _, err := ResolveObjectIORef(ctx, ref, "s3://warehouse/orders.parquet"); err != nil || resolvedFS != fs {
		t.Fatalf("ref should remain registered after one of two releases, fs=%v err=%v", resolvedFS, err)
	}
	ReleaseObjectIORef(ref)
	if _, _, err := ResolveObjectIORef(ctx, ref, "s3://warehouse/orders.parquet"); err == nil {
		t.Fatalf("ref should be removed after final release")
	}
}

func TestScopedProviderCredentialExpiration(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	fs, err := fileservice.NewMemoryFS("iceberg-expiry", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	provider := ScopedProvider{FileService: fs, Now: func() time.Time { return now }, MinTTL: time.Minute}
	scope := ObjectScope{
		AccountID:           1,
		CatalogID:           2,
		StorageLocation:     "s3://warehouse/t/data.parquet",
		CredentialID:        "cred-1",
		CredentialExpiresAt: now.Add(30 * time.Second),
		Endpoint:            "s3.me-central-1.amazonaws.com",
		Region:              "me-central-1",
		Bucket:              "warehouse",
		Principal:           "external",
	}
	if _, _, err := provider.Resolve(ctx, scope); err == nil || !strings.Contains(err.Error(), "ICEBERG_CREDENTIAL_EXPIRED") {
		t.Fatalf("expected credential expiry error, got %v", err)
	}
	if _, err := provider.Refresh(ctx, scope); err == nil || !strings.Contains(err.Error(), "ICEBERG_CREDENTIAL_EXPIRED") {
		t.Fatalf("expected missing refresh hook to map to credential expiry, got %v", err)
	}
}

func TestScopedProviderResidencyValidator(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	fs, err := fileservice.NewMemoryFS("iceberg-residency", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	var checked ObjectScope
	provider := ScopedProvider{
		FileService: fs,
		Now:         func() time.Time { return now },
		ResidencyValidator: func(ctx context.Context, scope ObjectScope) error {
			checked = scope
			return api.ToMOErr(ctx, api.NewError(api.ErrResidencyDenied, "blocked by test residency policy", nil))
		},
	}
	scope := ObjectScope{
		AccountID:           1,
		CatalogID:           2,
		StorageLocation:     "  s3://warehouse/t/data.parquet  ",
		CredentialID:        "cred-1",
		CredentialExpiresAt: now.Add(5 * time.Minute),
		Endpoint:            "HTTPS://S3.ME-CENTRAL-1.AMAZONAWS.COM/",
		Region:              "ME-CENTRAL-1",
		Bucket:              "warehouse",
		Principal:           "external",
	}
	resolvedFS, readPath, err := provider.Resolve(ctx, scope)
	if err == nil || !strings.Contains(err.Error(), "ICEBERG_RESIDENCY_DENIED") {
		t.Fatalf("expected residency denial, got fs=%v path=%q err=%v", resolvedFS, readPath, err)
	}
	if checked.Endpoint != "s3.me-central-1.amazonaws.com" || checked.Region != "me-central-1" || checked.StorageLocation != "s3://warehouse/t/data.parquet" {
		t.Fatalf("validator should receive canonical scope, got %+v", checked)
	}
}

func TestScopedProviderRequiresResidencyValidator(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-require-residency", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	provider := ScopedProvider{FileService: fs, RequireResidencyPolicy: true}
	scope := ObjectScope{
		AccountID:       1,
		CatalogID:       2,
		StorageLocation: "s3://warehouse/t/data.parquet",
		Endpoint:        "s3.me-central-1.amazonaws.com",
		Region:          "me-central-1",
		Bucket:          "warehouse",
		Principal:       "external",
	}
	if _, _, err := provider.Resolve(ctx, scope); err == nil || !strings.Contains(err.Error(), "ICEBERG_RESIDENCY_DENIED") {
		t.Fatalf("expected missing residency validator to fail closed, got %v", err)
	}
}

func TestProviderObjectReaderReadsThroughFileService(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-reader", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	writeMemoryFile(t, ctx, fs, "warehouse/sales/data.bin", []byte("abcdef"))

	var seenLocation string
	reader := ProviderObjectReader{
		Provider: ScopedProvider{FileService: fs},
		ScopeForLocation: func(location string) ObjectScope {
			seenLocation = location
			return ObjectScope{
				AccountID:       1,
				CatalogID:       2,
				StorageLocation: location,
				Endpoint:        "s3.me-central-1.amazonaws.com",
				Region:          "me-central-1",
				Bucket:          "warehouse",
				Principal:       "external",
			}
		},
	}
	full, err := reader.Read(ctx, "warehouse/sales/data.bin", 0, -1)
	if err != nil {
		t.Fatalf("read full object: %v", err)
	}
	if string(full) != "abcdef" || seenLocation != "warehouse/sales/data.bin" {
		t.Fatalf("unexpected full read data=%q seen=%q", full, seenLocation)
	}
	part, err := reader.Read(ctx, "warehouse/sales/data.bin", 2, 3)
	if err != nil {
		t.Fatalf("read object range: %v", err)
	}
	if string(part) != "cde" {
		t.Fatalf("unexpected range read: %q", part)
	}
}

func TestProviderObjectReaderScopeBuilderCanMapExternalLocation(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-reader-map", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	writeMemoryFile(t, ctx, fs, "sales/orders.avro", []byte("manifest"))

	var seenLocation string
	reader := ProviderObjectReader{
		Provider: ScopedProvider{FileService: fs},
		ScopeForLocation: func(location string) ObjectScope {
			seenLocation = location
			return ObjectScope{
				AccountID:       1,
				CatalogID:       2,
				StorageLocation: strings.TrimPrefix(location, "s3://warehouse/"),
				Endpoint:        "s3.me-central-1.amazonaws.com",
				Region:          "me-central-1",
				Bucket:          "warehouse",
				Principal:       "external",
			}
		},
	}
	data, err := reader.Read(ctx, "s3://warehouse/sales/orders.avro", 0, -1)
	if err != nil {
		t.Fatalf("read mapped object location: %v", err)
	}
	if string(data) != "manifest" || seenLocation != "s3://warehouse/sales/orders.avro" {
		t.Fatalf("unexpected mapped read data=%q seen=%q", data, seenLocation)
	}
}

func TestProviderObjectWriterWritesThroughFileService(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-writer", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}

	var seenLocation string
	writer := ProviderObjectWriter{
		Provider: ScopedProvider{FileService: fs},
		ScopeForLocation: func(location string) ObjectScope {
			seenLocation = location
			return ObjectScope{
				AccountID:       1,
				CatalogID:       2,
				StorageLocation: strings.TrimPrefix(location, "s3://warehouse/"),
				Endpoint:        "s3.me-central-1.amazonaws.com",
				Region:          "me-central-1",
				Bucket:          "warehouse",
				Principal:       "external",
			}
		},
	}
	if err := writer.WriteObject(ctx, "s3://warehouse/sales/orders/metadata/m0.avro", []byte("manifest")); err != nil {
		t.Fatalf("write object: %v", err)
	}
	if seenLocation != "s3://warehouse/sales/orders/metadata/m0.avro" {
		t.Fatalf("unexpected scoped location: %q", seenLocation)
	}
	reader := ProviderObjectReader{
		Provider: ScopedProvider{FileService: fs},
		ScopeForLocation: func(location string) ObjectScope {
			return ObjectScope{
				AccountID:       1,
				CatalogID:       2,
				StorageLocation: strings.TrimPrefix(location, "s3://warehouse/"),
				Endpoint:        "s3.me-central-1.amazonaws.com",
				Region:          "me-central-1",
				Bucket:          "warehouse",
				Principal:       "external",
			}
		},
	}
	data, err := reader.Read(ctx, "s3://warehouse/sales/orders/metadata/m0.avro", 0, -1)
	if err != nil {
		t.Fatalf("read written object: %v", err)
	}
	if string(data) != "manifest" {
		t.Fatalf("unexpected written data: %q", data)
	}
}

func TestProviderObjectWriterValidation(t *testing.T) {
	ctx := context.Background()
	if err := (ProviderObjectWriter{}).WriteObject(ctx, "path", []byte("x")); err == nil || !strings.Contains(err.Error(), "ICEBERG_OBJECT_IO") {
		t.Fatalf("expected missing provider error, got %v", err)
	}
	fs, err := fileservice.NewMemoryFS("iceberg-writer-validation", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	writer := ProviderObjectWriter{Provider: ScopedProvider{FileService: fs}}
	if err := writer.WriteObject(ctx, "", []byte("x")); err == nil || !strings.Contains(err.Error(), "ICEBERG_OBJECT_IO") {
		t.Fatalf("expected missing location error, got %v", err)
	}
	if err := writer.WriteObject(ctx, "path", nil); err == nil || !strings.Contains(err.Error(), "ICEBERG_OBJECT_IO") {
		t.Fatalf("expected empty payload error, got %v", err)
	}
}

func TestProviderObjectReaderValidation(t *testing.T) {
	ctx := context.Background()
	if _, err := (ProviderObjectReader{}).Read(ctx, "path", 0, -1); err == nil || !strings.Contains(err.Error(), "ICEBERG_OBJECT_IO") {
		t.Fatalf("expected missing provider error, got %v", err)
	}

	fs, err := fileservice.NewMemoryFS("iceberg-reader-validation", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	reader := ProviderObjectReader{
		Provider: ScopedProvider{FileService: fs},
		ScopeForLocation: func(location string) ObjectScope {
			return ObjectScope{
				AccountID:       1,
				CatalogID:       2,
				StorageLocation: location,
				Endpoint:        "s3.me-central-1.amazonaws.com",
				Region:          "me-central-1",
				Bucket:          "warehouse",
				Principal:       "external",
			}
		},
	}
	if _, err := reader.Read(ctx, "path", -1, -1); err == nil || !strings.Contains(err.Error(), "ICEBERG_OBJECT_IO") {
		t.Fatalf("expected negative offset error, got %v", err)
	}
	if _, err := reader.Read(ctx, "path", 0, 0); err == nil || !strings.Contains(err.Error(), "ICEBERG_OBJECT_IO") {
		t.Fatalf("expected invalid length error, got %v", err)
	}

	reader.Provider = ScopedProvider{FileService: fs, RequireResidencyPolicy: true}
	if _, err := reader.Read(ctx, "path", 0, -1); err == nil || !strings.Contains(err.Error(), "ICEBERG_RESIDENCY_DENIED") {
		t.Fatalf("expected residency fail-closed error, got %v", err)
	}
}

func TestVendedCredentialProviderBuildsScopedFileService(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	fs, err := fileservice.NewMemoryFS("iceberg-vended", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	credentials := []api.StorageCredential{
		{
			Prefix:    "s3://warehouse",
			Config:    map[string]string{"s3.access-key-id": "base"},
			ExpiresAt: now.Add(10 * time.Minute),
		},
		{
			Prefix:    "s3://warehouse/sales",
			Config:    map[string]string{"s3.access-key-id": "sales"},
			ExpiresAt: now.Add(10 * time.Minute),
		},
	}
	var checked ObjectScope
	provider := VendedCredentialProvider{
		Credentials: credentials,
		Now:         func() time.Time { return now },
		MinTTL:      time.Minute,
		ResidencyValidator: func(ctx context.Context, scope ObjectScope) error {
			checked = scope
			return nil
		},
		BuildFileService: func(ctx context.Context, scope ObjectScope, credential api.StorageCredential) (fileservice.ETLFileService, string, error) {
			if credential.Prefix != "s3://warehouse/sales" || credential.Config["s3.access-key-id"] != "sales" {
				t.Fatalf("expected longest matching credential, got %+v", credential)
			}
			credential.Config["s3.access-key-id"] = "mutated"
			return fs, strings.TrimPrefix(scope.StorageLocation, credential.Prefix+"/"), nil
		},
	}
	scope := ObjectScope{
		AccountID:       1,
		CatalogID:       2,
		StorageLocation: "s3://warehouse/sales/orders/data.parquet",
		Endpoint:        "s3.me-central-1.amazonaws.com",
		Region:          "me-central-1",
		Bucket:          "warehouse",
		Principal:       "external",
	}
	resolvedFS, readPath, err := provider.Resolve(ctx, scope)
	if err != nil {
		t.Fatalf("resolve vended credential provider: %v", err)
	}
	if resolvedFS == nil || readPath != "orders/data.parquet" {
		t.Fatalf("unexpected scoped resolve result fs=%v path=%q", resolvedFS, readPath)
	}
	if checked.Endpoint != "s3.me-central-1.amazonaws.com" || checked.Region != "me-central-1" {
		t.Fatalf("residency validator did not receive canonical scope: %+v", checked)
	}
	if credentials[1].Config["s3.access-key-id"] != "sales" {
		t.Fatalf("builder must receive a cloned credential config")
	}
}

func TestVendedCredentialProviderBuildsStatementScopedFileServices(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	credentials := []api.StorageCredential{
		{
			Prefix:    "s3://warehouse/sales",
			Config:    map[string]string{"s3.access-key-id": "sales-a"},
			ExpiresAt: now.Add(10 * time.Minute),
		},
	}
	builds := 0
	provider := VendedCredentialProvider{
		Credentials: credentials,
		Now:         func() time.Time { return now },
		BuildFileService: func(ctx context.Context, scope ObjectScope, credential api.StorageCredential) (fileservice.ETLFileService, string, error) {
			builds++
			fs, err := fileservice.NewMemoryFS("iceberg-vended-scoped-"+credential.Config["s3.access-key-id"]+"-"+scope.CredentialID, fileservice.DisabledCacheConfig, nil)
			if err != nil {
				return nil, "", err
			}
			return fs, strings.TrimPrefix(scope.StorageLocation, credential.Prefix+"/"), nil
		},
	}
	scope := ObjectScope{
		AccountID:       1,
		CatalogID:       2,
		StorageLocation: "s3://warehouse/sales/orders/data.parquet",
		Endpoint:        "s3.me-central-1.amazonaws.com",
		Region:          "me-central-1",
		Bucket:          "warehouse",
		Principal:       "external",
	}
	scope.CredentialID = "stmt-a"
	firstFS, readPath, err := provider.Resolve(ctx, scope)
	if err != nil {
		t.Fatalf("resolve first scoped FS: %v", err)
	}
	writeMemoryFile(t, ctx, firstFS, readPath, []byte("statement-a"))

	scope.CredentialID = "stmt-b"
	secondFS, secondPath, err := provider.Resolve(ctx, scope)
	if err != nil {
		t.Fatalf("resolve second scoped FS: %v", err)
	}
	if builds != 2 {
		t.Fatalf("expected a scoped FileService build per resolve, got %d", builds)
	}
	if firstFS == secondFS {
		t.Fatalf("vended credential provider reused a scoped FileService across statement credentials")
	}
	vec := fileservice.IOVector{
		FilePath: secondPath,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: -1}},
	}
	if err := secondFS.Read(ctx, &vec); err == nil {
		t.Fatalf("second scoped FileService unexpectedly saw data from the first scoped FS")
	}
}

func TestVendedCredentialProviderCredentialCoverageAndExpiry(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	fs, err := fileservice.NewMemoryFS("iceberg-vended-expiry", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	builds := 0
	provider := VendedCredentialProvider{
		Credentials: []api.StorageCredential{{
			Prefix:    "s3://warehouse/sales",
			Config:    map[string]string{"s3.access-key-id": "sales"},
			ExpiresAt: now.Add(30 * time.Second),
		}},
		Now:    func() time.Time { return now },
		MinTTL: time.Minute,
		BuildFileService: func(ctx context.Context, scope ObjectScope, credential api.StorageCredential) (fileservice.ETLFileService, string, error) {
			builds++
			return fs, scope.StorageLocation, nil
		},
	}
	scope := ObjectScope{
		AccountID:       1,
		CatalogID:       2,
		StorageLocation: "s3://warehouse/sales/orders/data.parquet",
		Endpoint:        "s3.me-central-1.amazonaws.com",
		Region:          "me-central-1",
		Bucket:          "warehouse",
		Principal:       "external",
	}
	if _, _, err := provider.Resolve(ctx, scope); err == nil || !strings.Contains(err.Error(), "ICEBERG_CREDENTIAL_EXPIRED") {
		t.Fatalf("expected expiring vended credential to fail fast, got %v", err)
	}
	if builds != 0 {
		t.Fatalf("expired vended credential must fail before building FileService, got %d builds", builds)
	}
	scope.StorageLocation = "s3://warehouse/finance/orders/data.parquet"
	provider.Credentials[0].ExpiresAt = now.Add(10 * time.Minute)
	if _, _, err := provider.Resolve(ctx, scope); err == nil || !strings.Contains(err.Error(), "ICEBERG_CONFIG_INVALID") {
		t.Fatalf("expected uncovered storage location to fail, got %v", err)
	}
}

func TestS3ObjectScopeForConfigNormalizesEndpointForResidency(t *testing.T) {
	scopeForLocation := S3ObjectScopeForConfig(ObjectScope{
		AccountID: 1,
		CatalogID: 2,
		Principal: "external",
	}, map[string]string{
		"s3.endpoint":   "http://127.0.0.1:9000",
		"client.region": "us-east-1",
	})
	scope := scopeForLocation("s3://mo-iceberg/warehouse/data.parquet")
	if scope.Endpoint != "localhost" || scope.Region != "us-east-1" || scope.Bucket != "mo-iceberg" {
		t.Fatalf("unexpected normalized object scope: %+v", scope)
	}
}

func TestRemoteSigningProviderBuildsScopedSignedFileService(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	fs, err := fileservice.NewMemoryFS("iceberg-remote-signing", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	writeMemoryFile(t, ctx, fs, "sales/orders/data.parquet", []byte("remote-signed"))

	signed := SignedRequest{
		URL:       "https://bucket.s3.me-central-1.amazonaws.com/sales/orders/data.parquet?X-Amz-Signature=secret",
		Headers:   map[string]string{"Authorization": "sig-secret"},
		ExpiresAt: now.Add(10 * time.Minute),
	}
	signer := &fakeRemoteSigner{signed: signed}
	var checked ObjectScope
	provider := RemoteSigningProvider{
		Signer: signer,
		Now:    func() time.Time { return now },
		MinTTL: time.Minute,
		ResidencyValidator: func(ctx context.Context, scope ObjectScope) error {
			checked = scope
			return nil
		},
		BuildFileService: func(ctx context.Context, scope ObjectScope, signed SignedRequest) (fileservice.ETLFileService, string, error) {
			if signed.Headers["Authorization"] != "sig-secret" {
				t.Fatalf("expected signed request headers, got %+v", signed.Headers)
			}
			signed.Headers["Authorization"] = "mutated"
			return fs, strings.TrimPrefix(scope.StorageLocation, "s3://warehouse/"), nil
		},
	}
	reader := ProviderObjectReader{
		Provider: provider,
		ScopeForLocation: func(location string) ObjectScope {
			return ObjectScope{
				AccountID:       1,
				CatalogID:       2,
				StorageLocation: location,
				Endpoint:        "HTTPS://S3.ME-CENTRAL-1.AMAZONAWS.COM/",
				Region:          "ME-CENTRAL-1",
				Bucket:          "warehouse",
				Principal:       "external",
			}
		},
	}
	data, err := reader.Read(ctx, "s3://warehouse/sales/orders/data.parquet", 0, -1)
	if err != nil {
		t.Fatalf("read through remote signing provider: %v", err)
	}
	if string(data) != "remote-signed" {
		t.Fatalf("unexpected remote signed read: %q", data)
	}
	if signer.method != "GET" || signer.location != "s3://warehouse/sales/orders/data.parquet" {
		t.Fatalf("unexpected signer call method=%q location=%q", signer.method, signer.location)
	}
	if checked.Endpoint != "s3.me-central-1.amazonaws.com" || checked.Region != "me-central-1" {
		t.Fatalf("residency validator did not receive canonical scope: %+v", checked)
	}
	if signed.Headers["Authorization"] != "sig-secret" {
		t.Fatalf("builder must receive a cloned signed request")
	}
}

func TestRemoteSigningProviderSignedRequestTTLAndRedaction(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	signer := &fakeRemoteSigner{signed: SignedRequest{
		URL:       "https://bucket.s3.me-central-1.amazonaws.com/data.parquet?X-Amz-Signature=secret",
		ExpiresAt: now.Add(30 * time.Second),
	}}
	builds := 0
	provider := RemoteSigningProvider{
		Signer: signer,
		Now:    func() time.Time { return now },
		MinTTL: time.Minute,
		BuildFileService: func(ctx context.Context, scope ObjectScope, signed SignedRequest) (fileservice.ETLFileService, string, error) {
			builds++
			return nil, "", nil
		},
	}
	_, _, err := provider.Resolve(ctx, ObjectScope{
		AccountID:       1,
		CatalogID:       2,
		StorageLocation: "s3://warehouse/data.parquet",
		Endpoint:        "s3.me-central-1.amazonaws.com",
		Region:          "me-central-1",
		Bucket:          "warehouse",
		Principal:       "external",
	})
	if err == nil || !strings.Contains(err.Error(), "ICEBERG_REMOTE_SIGNING_EXPIRED") {
		t.Fatalf("expected signed request expiry, got %v", err)
	}
	if builds != 0 {
		t.Fatalf("expired signed request must fail before building FileService, got %d builds", builds)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "X-Amz") || strings.Contains(err.Error(), "bucket") {
		t.Fatalf("signed URL details leaked in error: %v", err)
	}
}

func TestRemoteSigningProviderDenialAndResidencyFailClosed(t *testing.T) {
	ctx := context.Background()
	scope := ObjectScope{
		AccountID:       1,
		CatalogID:       2,
		StorageLocation: "s3://warehouse/data.parquet",
		Endpoint:        "s3.me-central-1.amazonaws.com",
		Region:          "me-central-1",
		Bucket:          "warehouse",
		Principal:       "external",
	}
	provider := RemoteSigningProvider{
		Signer: &fakeRemoteSigner{err: api.NewError(api.ErrRemoteSigningDenied, "denied by signer", nil)},
		BuildFileService: func(ctx context.Context, scope ObjectScope, signed SignedRequest) (fileservice.ETLFileService, string, error) {
			return nil, "", nil
		},
	}
	if _, _, err := provider.Resolve(ctx, scope); err == nil || !strings.Contains(err.Error(), "ICEBERG_REMOTE_SIGNING_DENIED") {
		t.Fatalf("expected remote signing denial, got %v", err)
	}

	provider = RemoteSigningProvider{
		Signer: &fakeRemoteSigner{signed: SignedRequest{URL: "https://signed.example.com/data", ExpiresAt: time.Now().Add(time.Hour)}},
		BuildFileService: func(ctx context.Context, scope ObjectScope, signed SignedRequest) (fileservice.ETLFileService, string, error) {
			return nil, "", nil
		},
		RequireResidencyPolicy: true,
	}
	if _, _, err := provider.Resolve(ctx, scope); err == nil || !strings.Contains(err.Error(), "ICEBERG_RESIDENCY_DENIED") {
		t.Fatalf("expected missing residency validator to fail closed, got %v", err)
	}
}

func TestRedactObjectPathDropsSignedURLQuery(t *testing.T) {
	raw := "https://bucket.s3.me-central-1.amazonaws.com/path/data.parquet?X-Amz-Signature=secret#frag"
	redacted := RedactObjectPath(raw)
	if strings.Contains(redacted, "secret") || strings.Contains(redacted, "X-Amz") || strings.Contains(redacted, "bucket") {
		t.Fatalf("redacted path leaked signed URL details: %q", redacted)
	}
	if redacted != RedactObjectPath("https://bucket.s3.me-central-1.amazonaws.com/path/data.parquet") {
		t.Fatalf("signed URL query/fragment should not affect redaction hash")
	}
}

type fakeRemoteSigner struct {
	signed   SignedRequest
	err      error
	method   string
	location string
}

func (s *fakeRemoteSigner) Sign(ctx context.Context, method, location string) (SignedRequest, error) {
	s.method = method
	s.location = location
	if s.err != nil {
		return SignedRequest{}, s.err
	}
	return s.signed, nil
}

func writeMemoryFile(t *testing.T, ctx context.Context, fs fileservice.ETLFileService, path string, data []byte) {
	t.Helper()
	if err := fs.Write(ctx, fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   append([]byte(nil), data...),
		}},
	}); err != nil {
		t.Fatalf("write memory file %q: %v", path, err)
	}
}
