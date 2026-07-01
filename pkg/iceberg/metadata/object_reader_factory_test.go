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

package metadata

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestVendedObjectReaderFactoryLoadsCredentialsAndBuildsScopedReader(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-vended-reader-factory", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	writeMetadataTestFile(t, ctx, fs, "orders/metadata.json", []byte("metadata"))

	expires := futureCredentialExpiry()
	client := &catalog.MockClient{
		LoadCredentialsFunc: func(ctx context.Context, req api.LoadCredentialsRequest) (*api.LoadCredentialsResponse, error) {
			if strings.Join(req.Namespace, ".") != "sales" || req.Table != "orders" || req.ExternalPrincipal != "ksa-analytics" {
				t.Fatalf("unexpected load credentials request: %+v", req)
			}
			return &api.LoadCredentialsResponse{StorageCredentials: []api.StorageCredential{{
				Prefix:    "s3://warehouse/sales",
				ExpiresAt: expires,
				Config: map[string]string{
					"s3.endpoint":          "s3.me-central-1.amazonaws.com",
					"client.region":        "me-central-1",
					"s3.access-key-id":     "AKIA",
					"s3.secret-access-key": "secret",
				},
			}}}, nil
		},
	}
	var checked icebergio.ObjectScope
	factory := VendedObjectReaderFactory{
		BuildFileService: func(ctx context.Context, scope icebergio.ObjectScope, credential api.StorageCredential) (fileservice.ETLFileService, string, error) {
			if credential.Prefix != "s3://warehouse/sales" || credential.Config["s3.access-key-id"] != "AKIA" {
				t.Fatalf("unexpected credential: %+v", credential)
			}
			return fs, strings.TrimPrefix(scope.StorageLocation, credential.Prefix+"/"), nil
		},
		ResidencyValidator: func(ctx context.Context, scope icebergio.ObjectScope) error {
			checked = scope
			return nil
		},
		RequireResidencyPolicy: true,
	}
	req := api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{
			Catalog:           model.Catalog{AccountID: 42, CatalogID: 7, Name: "prod"},
			ExternalPrincipal: "ksa-analytics",
		},
		Namespace: api.Namespace{"sales"},
		Table:     "orders",
	}

	reader, readerCtx, err := factory.NewObjectReader(ctx, client, req)
	if err != nil {
		t.Fatalf("new object reader: %v", err)
	}
	data, err := reader.Read(ctx, "s3://warehouse/sales/orders/metadata.json", 0, -1)
	if err != nil {
		t.Fatalf("read through vended object reader: %v", err)
	}
	if string(data) != "metadata" {
		t.Fatalf("unexpected data: %q", data)
	}
	if readerCtx.CredentialHash == "" || readerCtx.CredentialScope != readerCtx.CredentialHash {
		t.Fatalf("credential context not populated: %+v", readerCtx)
	}
	if checked.AccountID != 42 || checked.CatalogID != 7 || checked.Endpoint != "s3.me-central-1.amazonaws.com" ||
		checked.Region != "me-central-1" || checked.Bucket != "warehouse" || checked.Principal != "ksa-analytics" {
		t.Fatalf("residency validator saw wrong scope: %+v", checked)
	}
}

func TestVendedObjectReaderFactoryUsesRequestScopedResidencyValidator(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-vended-request-residency", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	writeMetadataTestFile(t, ctx, fs, "orders/metadata.json", []byte("metadata"))

	client := &catalog.MockClient{
		LoadCredentialsFunc: func(ctx context.Context, req api.LoadCredentialsRequest) (*api.LoadCredentialsResponse, error) {
			return &api.LoadCredentialsResponse{StorageCredentials: []api.StorageCredential{{
				Prefix:    "s3://warehouse/sales",
				ExpiresAt: futureCredentialExpiry(),
				Config: map[string]string{
					"s3.endpoint":          "s3.me-central-1.amazonaws.com",
					"client.region":        "me-central-1",
					"s3.access-key-id":     "AKIA",
					"s3.secret-access-key": "secret",
				},
			}}}, nil
		},
	}
	var checked api.ObjectResidencyRequest
	factory := VendedObjectReaderFactory{
		BuildFileService: func(ctx context.Context, scope icebergio.ObjectScope, credential api.StorageCredential) (fileservice.ETLFileService, string, error) {
			return fs, strings.TrimPrefix(scope.StorageLocation, credential.Prefix+"/"), nil
		},
		RequireResidencyPolicy: true,
	}
	req := api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{
			Catalog:           model.Catalog{AccountID: 42, CatalogID: 7, Name: "prod"},
			ExternalPrincipal: "ksa-analytics",
		},
		Namespace: api.Namespace{"sales"},
		Table:     "orders",
		ObjectResidencyValidator: func(ctx context.Context, req api.ObjectResidencyRequest) error {
			checked = req
			return nil
		},
	}

	reader, _, err := factory.NewObjectReader(ctx, client, req)
	if err != nil {
		t.Fatalf("new object reader: %v", err)
	}
	if _, err := reader.Read(ctx, "s3://warehouse/sales/orders/metadata.json", 0, -1); err != nil {
		t.Fatalf("read through request residency validator: %v", err)
	}
	if checked.AccountID != 42 || checked.CatalogID != 7 || checked.Endpoint != "s3.me-central-1.amazonaws.com" ||
		checked.Region != "me-central-1" || checked.Bucket != "warehouse" || checked.Principal != "ksa-analytics" {
		t.Fatalf("request residency validator saw wrong scope: %+v", checked)
	}
}

func TestVendedObjectReaderFactoryFallsBackToRemoteSigning(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-remote-signing-reader-factory", fileservice.DisabledCacheConfig, nil)
	if err != nil {
		t.Fatalf("new memory fs: %v", err)
	}
	writeMetadataTestFile(t, ctx, fs, "orders/metadata.json", []byte("metadata"))

	signer := &fakeFactoryRemoteSigner{signed: icebergio.SignedRequest{
		URL:       "https://signed.example.com/orders/metadata.json",
		Headers:   map[string]string{"Authorization": "signed"},
		ExpiresAt: time.Now().Add(10 * time.Minute),
	}}
	client := &remoteSigningCatalogClient{
		MockClient: &catalog.MockClient{
			LoadCredentialsFunc: func(ctx context.Context, req api.LoadCredentialsRequest) (*api.LoadCredentialsResponse, error) {
				return &api.LoadCredentialsResponse{}, nil
			},
			LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
				if req.Snapshots != "all" || strings.Join(req.Namespace, ".") != "sales" || req.Table != "orders" {
					t.Fatalf("unexpected load table request: %+v", req)
				}
				return &api.LoadTableResponse{
					Config: map[string]string{
						"s3.endpoint":               "https://s3.me-central-1.amazonaws.com",
						"client.region":             "me-central-1",
						"s3.remote-signing-enabled": "true",
					},
					Capabilities: api.CatalogCapabilities{RemoteSigning: true},
				}, nil
			},
		},
		signer: signer,
	}
	var checked icebergio.ObjectScope
	factory := VendedObjectReaderFactory{
		BuildFileService: func(ctx context.Context, scope icebergio.ObjectScope, credential api.StorageCredential) (fileservice.ETLFileService, string, error) {
			t.Fatalf("vended file service builder should not be used")
			return nil, "", nil
		},
		BuildSignedFileService: func(ctx context.Context, scope icebergio.ObjectScope, signed icebergio.SignedRequest) (fileservice.ETLFileService, string, error) {
			if signed.Headers["Authorization"] != "signed" {
				t.Fatalf("unexpected signed request: %+v", signed)
			}
			return fs, "orders/metadata.json", nil
		},
		ResidencyValidator: func(ctx context.Context, scope icebergio.ObjectScope) error {
			checked = scope
			return nil
		},
		RequireResidencyPolicy: true,
	}
	req := api.ScanPlanRequest{
		CatalogRequest: api.CatalogRequest{
			Catalog:           model.Catalog{AccountID: 42, CatalogID: 7, Name: "prod"},
			ExternalPrincipal: "ksa-analytics",
		},
		Namespace: api.Namespace{"sales"},
		Table:     "orders",
	}
	reader, readerCtx, err := factory.NewObjectReader(ctx, client, req)
	if err != nil {
		t.Fatalf("new remote signing object reader: %v", err)
	}
	data, err := reader.Read(ctx, "s3://warehouse/sales/orders/metadata.json", 0, -1)
	if err != nil {
		t.Fatalf("read through remote signing object reader: %v", err)
	}
	if string(data) != "metadata" {
		t.Fatalf("unexpected data: %q", data)
	}
	if signer.method != "GET" || signer.location != "s3://warehouse/sales/orders/metadata.json" {
		t.Fatalf("unexpected signer call: method=%q location=%q", signer.method, signer.location)
	}
	if checked.Endpoint != "s3.me-central-1.amazonaws.com" || checked.Region != "me-central-1" || checked.Bucket != "warehouse" {
		t.Fatalf("residency validator saw wrong remote scope: %+v", checked)
	}
	if readerCtx.ObjectIORef == "" || readerCtx.CredentialHash == "" {
		t.Fatalf("reader context not populated: %+v", readerCtx)
	}
}

func TestVendedObjectReaderFactoryRequiresCredentials(t *testing.T) {
	_, _, err := (VendedObjectReaderFactory{}).NewObjectReader(context.Background(), &catalog.MockClient{}, api.ScanPlanRequest{})
	if err == nil || !strings.Contains(err.Error(), string(api.ErrConfigInvalid)) {
		t.Fatalf("expected missing builder error, got %v", err)
	}

	factory := VendedObjectReaderFactory{
		BuildFileService: func(ctx context.Context, scope icebergio.ObjectScope, credential api.StorageCredential) (fileservice.ETLFileService, string, error) {
			return nil, "", nil
		},
	}
	_, _, err = factory.NewObjectReader(context.Background(), &catalog.MockClient{}, api.ScanPlanRequest{})
	if err == nil || !strings.Contains(err.Error(), string(api.ErrCredentialExpired)) {
		t.Fatalf("expected missing credentials error, got %v", err)
	}
}

type remoteSigningCatalogClient struct {
	*catalog.MockClient
	signer icebergio.RemoteSigner
}

func (c *remoteSigningCatalogClient) NewRemoteSigner(req api.CatalogRequest, config map[string]string) icebergio.RemoteSigner {
	return c.signer
}

type fakeFactoryRemoteSigner struct {
	signed   icebergio.SignedRequest
	method   string
	location string
}

func (s *fakeFactoryRemoteSigner) Sign(ctx context.Context, method, location string) (icebergio.SignedRequest, error) {
	s.method = method
	s.location = location
	return s.signed, nil
}

func futureCredentialExpiry() time.Time {
	return time.Now().UTC().Add(24 * time.Hour)
}

func writeMetadataTestFile(t *testing.T, ctx context.Context, fs fileservice.ETLFileService, path string, data []byte) {
	t.Helper()
	vec := fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   append([]byte(nil), data...),
		}},
	}
	if err := fs.Write(ctx, vec); err != nil {
		t.Fatalf("write metadata test file: %v", err)
	}
}
