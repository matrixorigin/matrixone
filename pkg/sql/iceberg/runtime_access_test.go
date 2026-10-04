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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/stretchr/testify/require"
)

func TestCheckRuntimeCatalogAccessAllowsMappedPrincipal(t *testing.T) {
	ctx := context.Background()
	store := &fakeRuntimeAccessStore{
		principalMaps: []model.PrincipalMap{{
			AccountID:         42,
			CatalogID:         7,
			MORoleID:          10,
			MOUserID:          20,
			ExternalPrincipal: "analytics-principal",
		}},
		residencyPolicies: []model.ResidencyPolicy{{
			ScopeType:         model.ResidencyScopeCluster,
			AccountID:         0,
			CatalogID:         7,
			AllowedCatalogURI: "https://catalog.example.com/rest",
			AllowedEndpoint:   "catalog.example.com",
			AllowedRegion:     model.ResidencyWildcard,
			AllowedBucket:     model.ResidencyWildcard,
			PolicyState:       model.ResidencyPolicyEnabled,
		}},
	}

	access, err := checkRuntimeCatalogAccess(ctx, store, 42, model.Catalog{
		CatalogID: 7,
		URI:       "https://catalog.example.com/rest",
	}, 10, 20)
	require.NoError(t, err)
	require.Equal(t, "analytics-principal", access.Decision.ExternalPrincipal)
	require.Len(t, access.ResidencyPolicies, 1)
	require.Equal(t, uint32(42), store.principalAccountID)
	require.Equal(t, uint64(7), store.principalCatalogID)
	require.Equal(t, uint32(42), store.residencyAccountID)
	require.Equal(t, uint64(7), store.residencyCatalogID)
}

func TestCheckRuntimeCatalogAccessRejectsInvalidInputs(t *testing.T) {
	ctx := context.Background()

	_, err := checkRuntimeCatalogAccess(ctx, &fakeRuntimeAccessStore{}, 42, model.Catalog{}, 10, 20)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires catalog id")

	_, err = checkRuntimeCatalogAccess(ctx, fakeRuntimeResidencyOnlyStore{}, 42, model.Catalog{CatalogID: 7}, 10, 20)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires principal map store")

	_, err = checkRuntimeCatalogAccess(ctx, fakeRuntimePrincipalOnlyStore{}, 42, model.Catalog{CatalogID: 7}, 10, 20)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires residency policy store")
}

func TestCheckRuntimeCatalogAccessPropagatesStoreErrors(t *testing.T) {
	ctx := context.Background()
	principalErr := fakeRuntimeAccessError("principal list failed")
	residencyErr := fakeRuntimeAccessError("residency list failed")

	_, err := checkRuntimeCatalogAccess(ctx, &fakeRuntimeAccessStore{principalErr: principalErr}, 42, model.Catalog{
		CatalogID: 7,
		URI:       "https://catalog.example.com/rest",
	}, 10, 20)
	require.ErrorIs(t, err, principalErr)

	_, err = checkRuntimeCatalogAccess(ctx, &fakeRuntimeAccessStore{residencyErr: residencyErr}, 42, model.Catalog{
		CatalogID: 7,
		URI:       "https://catalog.example.com/rest",
	}, 10, 20)
	require.ErrorIs(t, err, residencyErr)
}

func TestCheckRuntimeCatalogAccessRequiresPrincipalMapping(t *testing.T) {
	ctx := context.Background()
	store := &fakeRuntimeAccessStore{
		principalMaps: []model.PrincipalMap{},
		residencyPolicies: []model.ResidencyPolicy{{
			ScopeType:         model.ResidencyScopeCluster,
			AccountID:         0,
			CatalogID:         7,
			AllowedCatalogURI: "https://catalog.example.com/rest",
			AllowedEndpoint:   "catalog.example.com",
			AllowedRegion:     model.ResidencyWildcard,
			AllowedBucket:     model.ResidencyWildcard,
			PolicyState:       model.ResidencyPolicyEnabled,
		}},
	}

	_, err := checkRuntimeCatalogAccess(ctx, store, 42, model.Catalog{
		CatalogID: 7,
		URI:       "https://catalog.example.com/rest",
	}, 10, 20)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "principal") || strings.Contains(err.Error(), "ICEBERG_PRINCIPAL_NOT_MAPPED"), err.Error())
}

type fakeRuntimeAccessStore struct {
	principalMaps     []model.PrincipalMap
	residencyPolicies []model.ResidencyPolicy
	principalErr      error
	residencyErr      error

	principalAccountID uint32
	principalCatalogID uint64
	residencyAccountID uint32
	residencyCatalogID uint64
}

type fakeRuntimeAccessError string

func (e fakeRuntimeAccessError) Error() string {
	return string(e)
}

func (s *fakeRuntimeAccessStore) ListPrincipalMaps(ctx context.Context, accountID uint32, catalogID uint64) ([]model.PrincipalMap, error) {
	s.principalAccountID = accountID
	s.principalCatalogID = catalogID
	if s.principalErr != nil {
		return nil, s.principalErr
	}
	return append([]model.PrincipalMap(nil), s.principalMaps...), nil
}

func (s *fakeRuntimeAccessStore) ListResidencyPolicies(ctx context.Context, accountID uint32, catalogID uint64) ([]model.ResidencyPolicy, error) {
	s.residencyAccountID = accountID
	s.residencyCatalogID = catalogID
	if s.residencyErr != nil {
		return nil, s.residencyErr
	}
	return append([]model.ResidencyPolicy(nil), s.residencyPolicies...), nil
}

type fakeRuntimePrincipalOnlyStore struct{}

func (fakeRuntimePrincipalOnlyStore) ListPrincipalMaps(ctx context.Context, accountID uint32, catalogID uint64) ([]model.PrincipalMap, error) {
	return nil, nil
}

type fakeRuntimeResidencyOnlyStore struct{}

func (fakeRuntimeResidencyOnlyStore) ListResidencyPolicies(ctx context.Context, accountID uint32, catalogID uint64) ([]model.ResidencyPolicy, error) {
	return nil, nil
}
