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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

type PrincipalMapLister interface {
	ListPrincipalMaps(ctx context.Context, accountID uint32, catalogID uint64) ([]model.PrincipalMap, error)
}

type RuntimeCatalogAccess struct {
	Decision          CatalogAccessDecision
	ResidencyPolicies []model.ResidencyPolicy
}

func checkRuntimeCatalogAccess(
	ctx context.Context,
	store any,
	accountID uint32,
	catalog model.Catalog,
	roleID uint64,
	userID uint64,
) (RuntimeCatalogAccess, error) {
	if catalog.CatalogID == 0 {
		return RuntimeCatalogAccess{}, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg runtime catalog access requires catalog id", nil))
	}
	principalLister, ok := store.(PrincipalMapLister)
	if !ok || principalLister == nil {
		return RuntimeCatalogAccess{}, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg runtime catalog access requires principal map store", map[string]string{
			"catalog_id": strconv.FormatUint(catalog.CatalogID, 10),
		}))
	}
	residencyLister, ok := store.(ResidencyPolicyLister)
	if !ok || residencyLister == nil {
		return RuntimeCatalogAccess{}, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg runtime catalog access requires residency policy store", map[string]string{
			"catalog_id": strconv.FormatUint(catalog.CatalogID, 10),
		}))
	}
	principalMaps, err := principalLister.ListPrincipalMaps(ctx, accountID, catalog.CatalogID)
	if err != nil {
		return RuntimeCatalogAccess{}, err
	}
	residencyPolicies, err := residencyLister.ListResidencyPolicies(ctx, accountID, catalog.CatalogID)
	if err != nil {
		return RuntimeCatalogAccess{}, err
	}
	decision, err := CheckCatalogAccess(ctx, principalMaps, residencyPolicies, CatalogAccessRequest{
		AccountID:  accountID,
		CatalogID:  catalog.CatalogID,
		RoleID:     roleID,
		UserID:     userID,
		CatalogURI: catalog.URI,
	})
	if err != nil {
		return RuntimeCatalogAccess{}, err
	}
	return RuntimeCatalogAccess{
		Decision:          decision,
		ResidencyPolicies: residencyPolicies,
	}, nil
}
