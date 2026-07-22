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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

type CatalogByNameGetter interface {
	GetCatalogByName(ctx context.Context, accountID uint32, name string) (model.Catalog, error)
}

type MaintenanceCatalogResolver struct {
	DAO CatalogByNameGetter
}

func (r MaintenanceCatalogResolver) ResolveMaintenanceCatalog(ctx context.Context, accountID uint32, catalogName string) (maintenance.ProcedureCatalogResolution, error) {
	if r.DAO == nil {
		return maintenance.ProcedureCatalogResolution{}, moerr.NewInvalidInput(ctx, "iceberg maintenance catalog resolver requires a DAO")
	}
	catalog, err := r.DAO.GetCatalogByName(ctx, accountID, catalogName)
	if err != nil {
		return maintenance.ProcedureCatalogResolution{}, err
	}
	return maintenance.ProcedureCatalogResolutionFromModel(catalog)
}
