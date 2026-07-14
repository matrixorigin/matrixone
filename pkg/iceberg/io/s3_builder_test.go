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

func TestBuildS3ObjectStorageArgumentsFromVendedCredential(t *testing.T) {
	ctx := context.Background()
	scope := ObjectScope{
		AccountID:       1,
		CatalogID:       2,
		StorageLocation: "s3://warehouse/sales/orders/data.parquet",
		Endpoint:        "s3.me-central-1.amazonaws.com",
		Region:          "me-central-1",
		Bucket:          "warehouse",
		Principal:       "external",
	}
	credential := api.StorageCredential{
		Prefix:    "s3://warehouse/sales",
		ExpiresAt: time.Now().Add(time.Hour),
		Config: map[string]string{
			"s3.access-key-id":     "AKIA",
			"s3.secret-access-key": "secret",
			"s3.session-token":     "session",
			"s3.path-style-access": "true",
		},
	}

	args, readPath, err := BuildS3ObjectStorageArguments(ctx, scope, credential)
	if err != nil {
		t.Fatalf("build args: %v", err)
	}
	if args.Bucket != "warehouse" || args.Endpoint != "s3.me-central-1.amazonaws.com" || args.Region != "me-central-1" {
		t.Fatalf("unexpected s3 location args: %+v", args)
	}
	if args.KeyID != "AKIA" || args.KeySecret != "secret" || args.SessionToken != "session" {
		t.Fatalf("unexpected credential args: %+v", args)
	}
	if !args.IsMinio || !args.NoDefaultCredentials || !args.NoBucketValidation {
		t.Fatalf("expected scoped safe defaults, got %+v", args)
	}
	if args.KeyPrefix != "sales" || readPath != "orders/data.parquet" {
		t.Fatalf("unexpected prefix/read path keyPrefix=%q readPath=%q", args.KeyPrefix, readPath)
	}
}

func TestBuildS3ObjectStorageArgumentsUsesCredentialEndpointAndRegion(t *testing.T) {
	ctx := context.Background()
	scope := ObjectScope{
		AccountID:       1,
		CatalogID:       2,
		StorageLocation: "s3://warehouse/orders/data.parquet",
		Endpoint:        "s3.me-central-1.amazonaws.com",
		Region:          "me-central-1",
		Bucket:          "warehouse",
		Principal:       "external",
	}
	credential := api.StorageCredential{Config: map[string]string{
		"s3.endpoint":          "minio.local:9000",
		"client.region":        "us-east-1",
		"s3.access-key-id":     "AKIA",
		"s3.secret-access-key": "secret",
	}}

	args, readPath, err := BuildS3ObjectStorageArguments(ctx, scope, credential)
	if err != nil {
		t.Fatalf("build args: %v", err)
	}
	if args.Endpoint != "minio.local:9000" || args.Region != "us-east-1" || readPath != "orders/data.parquet" {
		t.Fatalf("credential endpoint/region not preferred, args=%+v readPath=%q", args, readPath)
	}
}

func TestBuildS3ObjectStorageArgumentsValidation(t *testing.T) {
	ctx := context.Background()
	scope := ObjectScope{
		AccountID:       1,
		CatalogID:       2,
		StorageLocation: "s3://warehouse/orders/data.parquet",
		Endpoint:        "s3.me-central-1.amazonaws.com",
		Region:          "me-central-1",
		Bucket:          "other",
		Principal:       "external",
	}
	credential := api.StorageCredential{Config: map[string]string{
		"s3.access-key-id":     "AKIA",
		"s3.secret-access-key": "secret",
	}}
	if _, _, err := BuildS3ObjectStorageArguments(ctx, scope, credential); err == nil || !strings.Contains(err.Error(), "bucket") {
		t.Fatalf("expected bucket mismatch error, got %v", err)
	}

	scope.Bucket = "warehouse"
	delete(credential.Config, "s3.secret-access-key")
	if _, _, err := BuildS3ObjectStorageArguments(ctx, scope, credential); err == nil || !strings.Contains(err.Error(), string(api.ErrCredentialExpired)) {
		t.Fatalf("expected missing credential error, got %v", err)
	}
}

func TestS3VendedFileServiceBuilderBuildsScopedReader(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-s3-builder", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	writeMemoryFile(t, ctx, fs, "orders/data.parquet", []byte("abc"))

	var captured fileservice.ObjectStorageArguments
	builder := S3VendedFileServiceBuilder{
		NewFileService: func(ctx context.Context, args fileservice.ObjectStorageArguments) (fileservice.ETLFileService, error) {
			captured = args
			return fs, nil
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
	credential := api.StorageCredential{
		Prefix: "s3://warehouse/sales",
		Config: map[string]string{
			"s3.access-key-id":     "AKIA",
			"s3.secret-access-key": "secret",
		},
	}
	resolvedFS, readPath, err := builder.Build(ctx, scope, credential)
	if err != nil {
		t.Fatalf("build scoped reader: %v", err)
	}
	if resolvedFS == nil || readPath != "orders/data.parquet" || captured.KeyPrefix != "sales" || !captured.NoDefaultCredentials || !captured.NoBucketValidation {
		t.Fatalf("unexpected build result fs=%v readPath=%q args=%+v", resolvedFS, readPath, captured)
	}
}

func TestS3ObjectScopeForLocationUsesMatchingCredential(t *testing.T) {
	expires := time.Date(2026, 6, 18, 10, 0, 0, 0, time.UTC)
	base := ObjectScope{
		AccountID: 1,
		CatalogID: 2,
		Endpoint:  "fallback.example.com",
		Region:    "us-east-1",
		Principal: "external",
	}
	scopeForLocation := S3ObjectScopeForLocation(base, []api.StorageCredential{
		{
			Prefix: "s3://warehouse",
			Config: map[string]string{
				"s3.endpoint":          "base.example.com",
				"s3.region":            "eu-west-1",
				"s3.access-key-id":     "base",
				"s3.secret-access-key": "base-secret",
			},
		},
		{
			Prefix:    "s3://warehouse/sales",
			ExpiresAt: expires,
			Config: map[string]string{
				"s3.endpoint":          "s3.me-central-1.amazonaws.com",
				"client.region":        "me-central-1",
				"s3.access-key-id":     "sales",
				"s3.secret-access-key": "sales-secret",
			},
		},
	})

	scope := scopeForLocation("s3://warehouse/sales/orders/data.parquet")
	if scope.Bucket != "warehouse" || scope.Endpoint != "s3.me-central-1.amazonaws.com" || scope.Region != "me-central-1" {
		t.Fatalf("unexpected object scope: %+v", scope)
	}
	if scope.CredentialID == "" || scope.CredentialExpiresAt != expires {
		t.Fatalf("credential identity/expiry not propagated: %+v", scope)
	}
	if strings.Contains(scope.CredentialID, "sales-secret") {
		t.Fatalf("credential id leaked secret: %q", scope.CredentialID)
	}
}

func TestBuildS3RemoteSigningRequestURIEscapesPathSegments(t *testing.T) {
	uri, region, err := BuildS3RemoteSigningRequestURI(context.Background(),
		"s3://warehouse/tpch/orders/data/bucket=3/part 01.parquet",
		map[string]string{
			"s3.endpoint":          "http://127.0.0.1:9000",
			"client.region":        "us-east-1",
			"s3.path-style-access": "true",
		},
	)
	if err != nil {
		t.Fatalf("build remote signing URI: %v", err)
	}
	if region != "us-east-1" {
		t.Fatalf("unexpected region: %q", region)
	}
	if !strings.Contains(uri, "/bucket%3D3/") || !strings.Contains(uri, "part%2001.parquet") {
		t.Fatalf("remote signing URI must use escaped path segments, got %q", uri)
	}
	if strings.Contains(uri, "bucket=3") || strings.Contains(uri, "part 01") {
		t.Fatalf("remote signing URI leaked unescaped path segment, got %q", uri)
	}
}

func TestBuildS3RemoteSigningRequestURIVirtualHostedAndValidation(t *testing.T) {
	uri, region, err := BuildS3RemoteSigningRequestURI(context.Background(),
		"s3://warehouse/sales/orders/data.parquet",
		map[string]string{
			"s3.endpoint":   "https://s3.me-central-1.amazonaws.com/base",
			"s3.region":     "me-central-1",
			"s3.is-minio":   "false",
			"unused_config": "ignored",
		},
	)
	if err != nil {
		t.Fatalf("build virtual-hosted remote signing uri: %v", err)
	}
	if region != "me-central-1" || !strings.HasPrefix(uri, "https://warehouse.s3.me-central-1.amazonaws.com/base/sales/orders/data.parquet") {
		t.Fatalf("unexpected virtual-hosted uri=%q region=%q", uri, region)
	}

	for _, tc := range []struct {
		location string
		config   map[string]string
	}{
		{"", map[string]string{"s3.endpoint": "s3.example.com", "s3.region": "us-east-1"}},
		{"s3://warehouse", map[string]string{"s3.endpoint": "s3.example.com", "s3.region": "us-east-1"}},
		{"s3://warehouse/data.parquet", map[string]string{"s3.region": "us-east-1"}},
		{"s3://warehouse/data.parquet", map[string]string{"s3.endpoint": "s3.example.com"}},
		{"s3://warehouse/data.parquet", map[string]string{"s3.endpoint": "://bad", "s3.region": "us-east-1"}},
	} {
		if _, _, err := BuildS3RemoteSigningRequestURI(context.Background(), tc.location, tc.config); err == nil {
			t.Fatalf("expected remote signing validation error for location=%q config=%+v", tc.location, tc.config)
		}
	}
}

func TestRemoteSigningPathStyleDetection(t *testing.T) {
	cases := []struct {
		host string
		cfg  map[string]string
		want bool
	}{
		{"localhost", nil, true},
		{"127.0.0.1:9000", nil, true},
		{"[::1]:9000", nil, true},
		{"s3.example.com", map[string]string{"s3.path-style-access": "true"}, true},
		{"s3.example.com", map[string]string{"s3.is-minio": "yes"}, true},
		{"s3.example.com", nil, false},
	}
	for _, tc := range cases {
		if got := remoteSigningUsePathStyle(tc.host, tc.cfg); got != tc.want {
			t.Fatalf("remoteSigningUsePathStyle(%q,%+v)=%v want %v", tc.host, tc.cfg, got, tc.want)
		}
	}
}

func TestCredentialIdentitiesDoNotCollideOnDelimiters(t *testing.T) {
	expires := time.Unix(1_700_000_000, 0).UTC()
	left := api.StorageCredential{Prefix: "s3://warehouse/", ExpiresAt: expires, Config: map[string]string{
		"a": "x|b=y",
	}}
	right := api.StorageCredential{Prefix: "s3://warehouse/", ExpiresAt: expires, Config: map[string]string{
		"a": "x",
		"b": "y",
	}}
	if storageCredentialIdentity(left) == storageCredentialIdentity(right) {
		t.Fatal("storage credential identities collided on config delimiters")
	}
	if remoteSigningConfigIdentity(left.Config) == remoteSigningConfigIdentity(right.Config) {
		t.Fatal("remote signing config identities collided on config delimiters")
	}
}
