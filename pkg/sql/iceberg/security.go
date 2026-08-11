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
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

type CatalogAccessRequest struct {
	AccountID  uint32
	CatalogID  uint64
	RoleID     uint64
	UserID     uint64
	CatalogURI string
}

type CatalogAccessDecision struct {
	ExternalPrincipal string
	PrincipalMap      model.PrincipalMap
}

func CheckCatalogAccess(
	ctx context.Context,
	principalMaps []model.PrincipalMap,
	residencyPolicies []model.ResidencyPolicy,
	req CatalogAccessRequest,
) (CatalogAccessDecision, error) {
	if req.CatalogID == 0 {
		return CatalogAccessDecision{}, moerr.NewInvalidInput(ctx, "iceberg catalog access check requires catalog_id")
	}
	mapping, ok, err := SelectPrincipalMap(ctx, principalMaps, req.RoleID, req.UserID)
	if err != nil {
		return CatalogAccessDecision{}, err
	}
	if !ok {
		return CatalogAccessDecision{}, PrincipalNotMappedError(ctx, req.AccountID, req.CatalogID, req.RoleID, req.UserID)
	}
	if err := CheckCatalogResidency(ctx, residencyPolicies, CatalogResidencyRequest{
		AccountID:  req.AccountID,
		CatalogID:  req.CatalogID,
		CatalogURI: req.CatalogURI,
	}); err != nil {
		return CatalogAccessDecision{}, err
	}
	return CatalogAccessDecision{
		ExternalPrincipal: mapping.ExternalPrincipal,
		PrincipalMap:      mapping,
	}, nil
}
