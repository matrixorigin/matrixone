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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

const (
	PrincipalSpecificityNone     = 0
	PrincipalSpecificityRoleOnly = 1
	PrincipalSpecificityUserOnly = 2
	PrincipalSpecificityExact    = 3
)

func ValidatePrincipalMap(ctx context.Context, mapping model.PrincipalMap) error {
	if mapping.CatalogID == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg principal map requires catalog_id")
	}
	if mapping.AccountID != 0 && mapping.MORoleID == model.PrincipalUnspecifiedID && mapping.MOUserID == model.PrincipalUnspecifiedID {
		return moerr.NewInvalidInput(ctx, "iceberg principal map requires mo_role_id or mo_user_id")
	}
	if trimNonEmpty(mapping.ExternalPrincipal) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg principal map requires external_principal")
	}
	return nil
}

func ValidatePrincipalMapPrivilege(ctx context.Context, actorAccountID uint32, targetAccountID uint32, isClusterAdmin bool) error {
	if targetAccountID == 0 && !isClusterAdmin {
		return moerr.NewInvalidInput(ctx, "iceberg principal map privilege check requires target account")
	}
	if isClusterAdmin || actorAccountID == targetAccountID {
		return nil
	}
	return moerr.NewInvalidInput(ctx, "iceberg principal map can only be managed by the target account or cluster admin")
}

func PrincipalNotMappedError(ctx context.Context, accountID uint32, catalogID uint64, roleID, userID uint64) error {
	return api.ToMOErr(ctx, api.NewError(api.ErrPrincipalNotMapped, "MO principal is not mapped to an Iceberg external principal", map[string]string{
		"account_id": fmt.Sprintf("%d", accountID),
		"catalog_id": fmt.Sprintf("%d", catalogID),
		"role_id":    fmt.Sprintf("%d", roleID),
		"user_id":    fmt.Sprintf("%d", userID),
	}))
}

func PrincipalSpecificity(mapping model.PrincipalMap, roleID, userID uint64) int {
	switch {
	case mapping.MORoleID == roleID && mapping.MOUserID == userID:
		return PrincipalSpecificityExact
	case mapping.MOUserID == userID && mapping.MORoleID == model.PrincipalUnspecifiedID:
		return PrincipalSpecificityUserOnly
	case mapping.MORoleID == roleID && mapping.MOUserID == model.PrincipalUnspecifiedID:
		return PrincipalSpecificityRoleOnly
	default:
		return PrincipalSpecificityNone
	}
}

func SelectPrincipalMap(ctx context.Context, candidates []model.PrincipalMap, roleID, userID uint64) (model.PrincipalMap, bool, error) {
	var selected model.PrincipalMap
	best := PrincipalSpecificityNone
	for _, candidate := range candidates {
		score := PrincipalSpecificity(candidate, roleID, userID)
		if score == PrincipalSpecificityNone {
			continue
		}
		if score > best {
			selected = candidate
			best = score
			continue
		}
		if score == best && selected.ExternalPrincipal != candidate.ExternalPrincipal {
			return model.PrincipalMap{}, false, moerr.NewInvalidInput(ctx, "conflicting iceberg principal mappings at same specificity")
		}
	}
	return selected, best != PrincipalSpecificityNone, nil
}

func DetectPrincipalConflicts(ctx context.Context, mappings []model.PrincipalMap) error {
	seen := make(map[string]string, len(mappings))
	for _, mapping := range mappings {
		if err := ValidatePrincipalMap(ctx, mapping); err != nil {
			return err
		}
		key := fmt.Sprintf("%d/%d/%d/%d", mapping.AccountID, mapping.CatalogID, mapping.MORoleID, mapping.MOUserID)
		if existing, ok := seen[key]; ok && existing != mapping.ExternalPrincipal {
			return moerr.NewInvalidInput(ctx, "conflicting iceberg principal mapping for account/catalog/role/user")
		}
		seen[key] = mapping.ExternalPrincipal
	}
	return nil
}
