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

package catalog

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestFactorySelectsNativeRESTByDefault(t *testing.T) {
	native := &MockClient{Name: AdapterNativeREST}
	factory := NewFactory(WithNativeRESTClient(native))
	client, err := factory.NewClient(context.Background(), model.Catalog{Type: "rest", Name: "c"})
	if err != nil {
		t.Fatalf("new native client: %v", err)
	}
	facade, ok := client.(api.CatalogFacade)
	if !ok || facade.AdapterName() != AdapterNativeREST {
		t.Fatalf("expected native facade, got %T", client)
	}
}

func TestFactoryNativeRESTOptionsApplyToDefaultClient(t *testing.T) {
	var tokenCalls atomic.Int32
	factory := NewFactory(WithNativeRESTOptions(
		WithAllowPlainHTTP(true),
		WithTokenProvider(countingTokenProvider{calls: &tokenCalls}),
		WithResidencyValidator(func(ctx context.Context, req api.CatalogRequest) error {
			return api.NewError(api.ErrResidencyDenied, "catalog endpoint is not allowed", map[string]string{
				"catalog_uri": req.Catalog.URI,
			})
		}),
	))
	catalog := testCatalog("http://catalog.example.com")
	catalog.TokenSecretRef = "secret://catalog"
	client, err := factory.NewClient(context.Background(), catalog)
	if err != nil {
		t.Fatalf("new native client: %v", err)
	}
	_, err = client.GetConfig(context.Background(), api.GetConfigRequest{
		CatalogRequest: api.CatalogRequest{Catalog: catalog},
	})
	if err == nil || !strings.Contains(err.Error(), string(api.ErrResidencyDenied)) {
		t.Fatalf("expected residency denial from factory native client, got %v", err)
	}
	if tokenCalls.Load() != 0 {
		t.Fatalf("factory native residency denial should happen before token resolution, token calls=%d", tokenCalls.Load())
	}
}

func TestFactorySelectsRegisteredIcebergGoAdapter(t *testing.T) {
	expected := &MockClient{Name: AdapterIcebergGo}
	factory := NewFactory(WithAdapter(AdapterIcebergGo, ClientFactoryFunc(func(context.Context, model.Catalog) (api.CatalogClient, error) {
		return expected, nil
	})))
	client, err := factory.NewClient(context.Background(), model.Catalog{Type: AdapterIcebergGo, Name: "c"})
	if err != nil {
		t.Fatalf("new adapter client: %v", err)
	}
	if client != expected {
		t.Fatalf("expected registered adapter")
	}
}

func TestFactoryRejectsUnknownAdapter(t *testing.T) {
	factory := NewFactory()
	_, err := factory.NewClient(context.Background(), model.Catalog{Type: "glue", Name: "c"})
	if err == nil || !strings.Contains(err.Error(), string(api.ErrUnsupportedFeature)) {
		t.Fatalf("expected unsupported adapter error, got %v", err)
	}
}

type ClientFactoryFunc func(context.Context, model.Catalog) (api.CatalogClient, error)

func (f ClientFactoryFunc) NewClient(ctx context.Context, catalog model.Catalog) (api.CatalogClient, error) {
	return f(ctx, catalog)
}
