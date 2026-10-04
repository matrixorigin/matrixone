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

package iceberggo

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	"github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

const AdapterName = "iceberg-go"

type RowDeltaCapability struct {
	AdapterName      string
	Supported        bool
	RequiresBuildTag bool
	Reason           string
}

func NewCatalogClientFactory() catalog.ClientFactory {
	return catalog.UnsupportedAdapterFactory{Name: AdapterName}
}

func NewManifestCommitAdapter() write.ManifestCommitAdapter {
	return write.UnsupportedManifestCommitAdapter{Name: AdapterName}
}

func ProbeRowDeltaCapability() RowDeltaCapability {
	return RowDeltaCapability{
		AdapterName:      AdapterName,
		Supported:        false,
		RequiresBuildTag: true,
		Reason:           "apache/iceberg-go is isolated behind the adapter module; RowDelta production support is not enabled until the facade can build MO-compatible delete/data manifests",
	}
}

func NewRowDeltaAdapter() dml.RowDeltaAdapter {
	return unsupportedRowDeltaAdapter{}
}

type unsupportedRowDeltaAdapter struct{}

func (unsupportedRowDeltaAdapter) Name() string {
	return AdapterName
}

func (unsupportedRowDeltaAdapter) SupportsRowDelta() bool {
	return ProbeRowDeltaCapability().Supported
}

func (unsupportedRowDeltaAdapter) BuildDelete(ctx context.Context, req dml.DeleteRequest) (*dml.ActionStream, bool, error) {
	return nil, false, api.NewError(api.ErrUnsupportedFeature, "Iceberg RowDelta adapter is not implemented", map[string]string{
		"adapter": AdapterName,
	})
}

func (unsupportedRowDeltaAdapter) BuildUpdate(ctx context.Context, req dml.UpdateRequest) (*dml.ActionStream, bool, error) {
	return nil, false, api.NewError(api.ErrUnsupportedFeature, "Iceberg RowDelta adapter is not implemented", map[string]string{
		"adapter": AdapterName,
	})
}

func (unsupportedRowDeltaAdapter) BuildMerge(ctx context.Context, req dml.MergeRequest) (*dml.ActionStream, bool, error) {
	return nil, false, api.NewError(api.ErrUnsupportedFeature, "Iceberg RowDelta adapter is not implemented", map[string]string{
		"adapter": AdapterName,
	})
}

var _ dml.RowDeltaAdapter = unsupportedRowDeltaAdapter{}
