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

package maintenance

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

type CatalogMetadataLoader struct {
	Client  api.CatalogClient
	Catalog api.CatalogRequest
}

func (l CatalogMetadataLoader) LoadMaintenanceTableMetadata(ctx context.Context, req Request) (*api.TableMetadata, error) {
	if l.Client == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance catalog metadata loader requires a catalog client", nil)
	}
	if strings.TrimSpace(req.Namespace) == "" || strings.TrimSpace(req.Table) == "" {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance catalog metadata loader requires namespace and table", nil)
	}
	resp, err := l.Client.LoadTable(ctx, api.LoadTableRequest{
		CatalogRequest: l.Catalog,
		Namespace:      namespaceFromString(req.Namespace),
		Table:          req.Table,
		// Maintenance and orphan validation need the complete snapshot/ref graph.
		// The REST catalog parameter accepts "all" or "refs", not a ref name.
		Snapshots: "all",
	})
	if err != nil {
		return nil, err
	}
	if resp == nil || len(resp.MetadataJSON) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg maintenance catalog load did not return metadata JSON", map[string]string{"table": req.Table})
	}
	return metadata.ParseTableMetadata(resp.MetadataJSON, resp.MetadataLocation)
}

var _ MaintenanceTableMetadataLoader = CatalogMetadataLoader{}
