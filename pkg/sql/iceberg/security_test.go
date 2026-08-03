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
)

func TestCheckCatalogAccessRequiresPrincipalAndResidency(t *testing.T) {
	ctx := context.Background()
	principals := []model.PrincipalMap{
		{AccountID: 42, CatalogID: 7, MORoleID: 10, MOUserID: 20, ExternalPrincipal: "ksa-analytics"},
	}
	policies := []model.ResidencyPolicy{
		{
			ScopeType:         model.ResidencyScopeCluster,
			CatalogID:         7,
			AllowedCatalogURI: "https://catalog.example.com/rest",
			AllowedEndpoint:   "catalog.example.com",
			AllowedRegion:     model.ResidencyWildcard,
			AllowedBucket:     model.ResidencyWildcard,
			PolicyState:       model.ResidencyPolicyEnabled,
		},
	}
	decision, err := CheckCatalogAccess(ctx, principals, policies, CatalogAccessRequest{
		AccountID:  42,
		CatalogID:  7,
		RoleID:     10,
		UserID:     20,
		CatalogURI: "https://catalog.example.com/rest",
	})
	if err != nil {
		t.Fatalf("expected catalog access to pass: %v", err)
	}
	if decision.ExternalPrincipal != "ksa-analytics" {
		t.Fatalf("unexpected external principal %q", decision.ExternalPrincipal)
	}
	_, err = CheckCatalogAccess(ctx, nil, policies, CatalogAccessRequest{
		AccountID:  42,
		CatalogID:  7,
		RoleID:     10,
		UserID:     20,
		CatalogURI: "https://catalog.example.com/rest",
	})
	if err == nil || !strings.Contains(err.Error(), "ICEBERG_PRINCIPAL_NOT_MAPPED") {
		t.Fatalf("expected principal mapping error, got %v", err)
	}
}

func TestCheckCatalogAccessAllowsSystemAccount(t *testing.T) {
	ctx := context.Background()
	principals := []model.PrincipalMap{
		{AccountID: 0, CatalogID: 7, MORoleID: 0, MOUserID: 0, ExternalPrincipal: "local-tier-a"},
	}
	policies := []model.ResidencyPolicy{
		{
			ScopeType:         model.ResidencyScopeCluster,
			CatalogID:         7,
			AllowedCatalogURI: "http://127.0.0.1:19120/iceberg",
			AllowedEndpoint:   "localhost",
			AllowedRegion:     model.ResidencyWildcard,
			AllowedBucket:     model.ResidencyWildcard,
			PolicyState:       model.ResidencyPolicyEnabled,
		},
	}
	decision, err := CheckCatalogAccess(ctx, principals, policies, CatalogAccessRequest{
		AccountID:  0,
		CatalogID:  7,
		RoleID:     0,
		UserID:     0,
		CatalogURI: "http://127.0.0.1:19120/iceberg",
	})
	if err != nil {
		t.Fatalf("expected system account catalog access to pass: %v", err)
	}
	if decision.ExternalPrincipal != "local-tier-a" {
		t.Fatalf("unexpected external principal %q", decision.ExternalPrincipal)
	}
}
