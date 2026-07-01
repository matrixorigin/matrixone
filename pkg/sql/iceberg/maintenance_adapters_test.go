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

package iceberg

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestMaintenanceCatalogResolverLoadsCapabilitiesFromDAO(t *testing.T) {
	resolution, err := (MaintenanceCatalogResolver{
		DAO: fakeCatalogByNameGetter{
			catalog: model.Catalog{
				AccountID:        7,
				CatalogID:        42,
				Name:             "ksa_gold",
				CapabilitiesJSON: `{"branch-tag":true,"metrics_report":true}`,
			},
		},
	}).ResolveMaintenanceCatalog(context.Background(), 7, "ksa_gold")
	if err != nil {
		t.Fatalf("resolve catalog: %v", err)
	}
	if resolution.CatalogID != 42 || !resolution.Capabilities.BranchTag || !resolution.Capabilities.MetricsReport {
		t.Fatalf("unexpected resolution: %+v", resolution)
	}
}

func TestMaintenanceCatalogResolverRejectsInvalidCapabilities(t *testing.T) {
	_, err := (MaintenanceCatalogResolver{
		DAO: fakeCatalogByNameGetter{
			catalog: model.Catalog{
				AccountID:        7,
				CatalogID:        42,
				Name:             "ksa_gold",
				CapabilitiesJSON: `{"branch_tag":"maybe"}`,
			},
		},
	}).ResolveMaintenanceCatalog(context.Background(), 7, "ksa_gold")
	if err == nil {
		t.Fatalf("expected invalid capabilities error")
	}
}

type fakeCatalogByNameGetter struct {
	catalog model.Catalog
	err     error
}

func (g fakeCatalogByNameGetter) GetCatalogByName(ctx context.Context, accountID uint32, name string) (model.Catalog, error) {
	if g.err != nil {
		return model.Catalog{}, g.err
	}
	return g.catalog, nil
}
