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

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

const AdapterIcebergGo = "iceberg-go"

type Factory struct {
	nativeREST        api.CatalogClient
	nativeRESTOptions []RESTClientOption
	adapters          map[string]ClientFactory
}

type FactoryOption func(*Factory)

func NewFactory(opts ...FactoryOption) *Factory {
	f := &Factory{
		adapters: make(map[string]ClientFactory),
	}
	for _, opt := range opts {
		opt(f)
	}
	if f.nativeREST == nil {
		f.nativeREST = NewRESTClient(f.nativeRESTOptions...)
	}
	if f.adapters == nil {
		f.adapters = make(map[string]ClientFactory)
	}
	return f
}

func WithNativeRESTClient(client api.CatalogClient) FactoryOption {
	return func(f *Factory) {
		f.nativeREST = client
	}
}

func WithNativeRESTOptions(opts ...RESTClientOption) FactoryOption {
	return func(f *Factory) {
		f.nativeRESTOptions = append(f.nativeRESTOptions, opts...)
	}
}

func WithAdapter(name string, factory ClientFactory) FactoryOption {
	return func(f *Factory) {
		if f.adapters == nil {
			f.adapters = make(map[string]ClientFactory)
		}
		f.adapters[strings.ToLower(strings.TrimSpace(name))] = factory
	}
}

func (f *Factory) NewClient(ctx context.Context, catalog model.Catalog) (api.CatalogClient, error) {
	adapter := strings.ToLower(strings.TrimSpace(catalog.Type))
	switch adapter {
	case "", "rest", AdapterNativeREST:
		return f.nativeREST, nil
	}
	if factory, ok := f.adapters[adapter]; ok {
		return factory.NewClient(ctx, catalog)
	}
	return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg catalog adapter is not registered", map[string]string{
		"catalog": catalog.Name,
		"type":    catalog.Type,
	})
}

type UnsupportedAdapterFactory struct {
	Name string
}

func (f UnsupportedAdapterFactory) NewClient(ctx context.Context, catalog model.Catalog) (api.CatalogClient, error) {
	name := strings.TrimSpace(f.Name)
	if name == "" {
		name = strings.TrimSpace(catalog.Type)
	}
	return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg catalog adapter is not implemented", map[string]string{
		"catalog": catalog.Name,
		"type":    name,
	})
}
