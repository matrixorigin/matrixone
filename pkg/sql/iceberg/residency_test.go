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

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestResidencyClusterAllowAndAccountIntersection(t *testing.T) {
	ctx := context.Background()
	policies := []model.ResidencyPolicy{
		{
			ScopeType:         model.ResidencyScopeCluster,
			CatalogID:         7,
			AllowedCatalogURI: "HTTPS://Catalog.Example.com/rest",
			AllowedEndpoint:   "https://s3.me-central-1.amazonaws.com",
			AllowedRegion:     "me-central-1",
			AllowedBucket:     model.ResidencyWildcard,
			PolicyState:       model.ResidencyPolicyEnabled,
		},
		{
			ScopeType:         model.ResidencyScopeAccount,
			AccountID:         42,
			CatalogID:         7,
			AllowedCatalogURI: "https://catalog.example.com/rest",
			AllowedEndpoint:   "s3.me-central-1.amazonaws.com",
			AllowedRegion:     "me-central-1",
			AllowedBucket:     "gold",
			PolicyState:       model.ResidencyPolicyEnabled,
		},
	}
	err := CheckResidency(ctx, policies, ResidencyRequest{
		AccountID:  42,
		CatalogID:  7,
		CatalogURI: "https://catalog.example.com/rest",
		Endpoint:   "s3.me-central-1.amazonaws.com",
		Region:     "me-central-1",
		Bucket:     "gold",
	})
	if err != nil {
		t.Fatalf("expected residency allow: %v", err)
	}
	err = CheckResidency(ctx, policies, ResidencyRequest{
		AccountID:  42,
		CatalogID:  7,
		CatalogURI: "https://catalog.example.com/rest",
		Endpoint:   "s3.me-central-1.amazonaws.com",
		Region:     "me-central-1",
		Bucket:     "silver",
	})
	if err == nil {
		t.Fatalf("account policy should narrow cluster allow set")
	}
}

func TestCatalogResidencyUsesEnabledPoliciesOnly(t *testing.T) {
	ctx := context.Background()
	policies := []model.ResidencyPolicy{
		{
			ScopeType:         model.ResidencyScopeCluster,
			CatalogID:         7,
			AllowedCatalogURI: "https://catalog.example.com/rest",
			AllowedEndpoint:   "catalog.example.com",
			AllowedRegion:     model.ResidencyWildcard,
			AllowedBucket:     model.ResidencyWildcard,
			PolicyState:       model.ResidencyPolicyAudit,
		},
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
	if err := CheckCatalogResidency(ctx, policies, CatalogResidencyRequest{
		AccountID:  42,
		CatalogID:  7,
		CatalogURI: "HTTPS://Catalog.Example.com/rest",
	}); err != nil {
		t.Fatalf("expected enabled cluster catalog policy to allow: %v", err)
	}
	policies[1].PolicyState = model.ResidencyPolicyDisabled
	if err := CheckCatalogResidency(ctx, policies, CatalogResidencyRequest{
		AccountID:  42,
		CatalogID:  7,
		CatalogURI: "https://catalog.example.com/rest",
	}); err == nil {
		t.Fatalf("audit/disabled policies must not grant catalog access")
	}
}

func TestCatalogResidencyAllowsSystemAccount(t *testing.T) {
	ctx := context.Background()
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
	if err := CheckCatalogResidency(ctx, policies, CatalogResidencyRequest{
		AccountID:  0,
		CatalogID:  7,
		CatalogURI: "http://127.0.0.1:19120/iceberg",
	}); err != nil {
		t.Fatalf("expected system account catalog residency to pass: %v", err)
	}
}

func TestCatalogResidencyDoesNotShareAccountLocalCatalogIDAcrossURIs(t *testing.T) {
	ctx := context.Background()
	policies := []model.ResidencyPolicy{{
		ScopeType:         model.ResidencyScopeCluster,
		CatalogID:         1,
		AllowedCatalogURI: "https://catalog-a.example.com/rest",
		AllowedEndpoint:   "catalog-a.example.com",
		AllowedRegion:     model.ResidencyWildcard,
		AllowedBucket:     model.ResidencyWildcard,
		PolicyState:       model.ResidencyPolicyEnabled,
	}}
	err := CheckCatalogResidency(ctx, policies, CatalogResidencyRequest{
		AccountID:  22,
		CatalogID:  1,
		CatalogURI: "https://catalog-b.example.com/rest",
	})
	if err == nil || !strings.Contains(err.Error(), "no cluster residency policy configured") {
		t.Fatalf("cluster policy for another catalog URI must not match account-local catalog id collision, got %v", err)
	}
}

func TestCatalogResidencyAllowsDistinctClusterPoliciesWithSameAccountLocalCatalogID(t *testing.T) {
	ctx := context.Background()
	policies := []model.ResidencyPolicy{
		{
			ScopeType:         model.ResidencyScopeCluster,
			AccountID:         0,
			CatalogID:         1,
			AllowedCatalogURI: "https://catalog-a.example.com/rest",
			AllowedEndpoint:   "catalog-a.example.com",
			AllowedRegion:     model.ResidencyWildcard,
			AllowedBucket:     model.ResidencyWildcard,
			PolicyState:       model.ResidencyPolicyEnabled,
		},
		{
			ScopeType:         model.ResidencyScopeCluster,
			AccountID:         0,
			CatalogID:         1,
			AllowedCatalogURI: "https://catalog-b.example.com/rest",
			AllowedEndpoint:   "catalog-b.example.com",
			AllowedRegion:     model.ResidencyWildcard,
			AllowedBucket:     model.ResidencyWildcard,
			PolicyState:       model.ResidencyPolicyEnabled,
		},
	}
	for _, req := range []CatalogResidencyRequest{
		{AccountID: 11, CatalogID: 1, CatalogURI: "https://catalog-a.example.com/rest"},
		{AccountID: 22, CatalogID: 1, CatalogURI: "https://catalog-b.example.com/rest"},
	} {
		if err := CheckCatalogResidency(ctx, policies, req); err != nil {
			t.Fatalf("cluster residency policy should be isolated by catalog URI for request %+v: %v", req, err)
		}
	}
}

func TestNormalizeResidencyPolicyStorageIdentityCanonicalizesMatchingFields(t *testing.T) {
	ctx := context.Background()
	enabledPolicy := model.ResidencyPolicy{
		ScopeType:         model.ResidencyScopeCluster,
		AccountID:         0,
		CatalogID:         1,
		AllowedCatalogURI: "https://catalog.example.com/rest",
		AllowedEndpoint:   "s3.me-central-1.amazonaws.com",
		AllowedRegion:     "me-central-1",
		AllowedBucket:     "gold",
		PolicyState:       model.ResidencyPolicyEnabled,
	}
	disabledVariant := enabledPolicy
	disabledVariant.AllowedCatalogURI = "HTTPS://Catalog.Example.com/rest#disabled-by-equivalent-uri"
	disabledVariant.AllowedEndpoint = "https://S3.ME-CENTRAL-1.AMAZONAWS.COM/"
	disabledVariant.AllowedRegion = "ME-CENTRAL-1"
	disabledVariant.AllowedBucket = " gold "
	disabledVariant.PolicyState = model.ResidencyPolicyDisabled

	enabledNormalized, err := NormalizeResidencyPolicyStorageIdentity(ctx, enabledPolicy)
	if err != nil {
		t.Fatalf("normalize enabled policy: %v", err)
	}
	disabledNormalized, err := NormalizeResidencyPolicyStorageIdentity(ctx, disabledVariant)
	if err != nil {
		t.Fatalf("normalize disabled variant: %v", err)
	}
	if enabledNormalized.AllowedCatalogURI != disabledNormalized.AllowedCatalogURI ||
		enabledNormalized.AllowedEndpoint != disabledNormalized.AllowedEndpoint ||
		enabledNormalized.AllowedRegion != disabledNormalized.AllowedRegion ||
		enabledNormalized.AllowedBucket != disabledNormalized.AllowedBucket {
		t.Fatalf("equivalent residency policies must share storage identity:\n enabled=%+v\n disabled=%+v",
			enabledNormalized, disabledNormalized)
	}

	err = CheckResidency(ctx, []model.ResidencyPolicy{disabledNormalized}, ResidencyRequest{
		AccountID:  42,
		CatalogID:  1,
		CatalogURI: "https://catalog.example.com/rest",
		Endpoint:   "s3.me-central-1.amazonaws.com",
		Region:     "me-central-1",
		Bucket:     "gold",
	})
	if err == nil || !strings.Contains(err.Error(), "no cluster residency policy configured") {
		t.Fatalf("disabled equivalent policy upsert should deny after replacing enabled policy, got %v", err)
	}
}

func TestObjectResidencyDoesNotShareAccountLocalCatalogIDAcrossURIs(t *testing.T) {
	ctx := context.Background()
	policies := []model.ResidencyPolicy{{
		ScopeType:         model.ResidencyScopeCluster,
		CatalogID:         1,
		AllowedCatalogURI: "https://catalog-a.example.com/rest",
		AllowedEndpoint:   "s3.me-central-1.amazonaws.com",
		AllowedRegion:     "me-central-1",
		AllowedBucket:     "gold",
		PolicyState:       model.ResidencyPolicyEnabled,
	}}
	err := CheckResidency(ctx, policies, ResidencyRequest{
		AccountID:  22,
		CatalogID:  1,
		CatalogURI: "https://catalog-b.example.com/rest",
		Endpoint:   "s3.me-central-1.amazonaws.com",
		Region:     "me-central-1",
		Bucket:     "gold",
	})
	if err == nil || !strings.Contains(err.Error(), "no cluster residency policy configured") {
		t.Fatalf("object policy for another catalog URI must not match account-local catalog id collision, got %v", err)
	}
}

func TestResidencyDefaultDenyAndEndpointNormalization(t *testing.T) {
	ctx := context.Background()
	_, err := NormalizeEndpoint(ctx, "s3.me-central-1.amazonaws.com:443")
	if err == nil {
		t.Fatalf("endpoint with port must be rejected")
	}
	err = CheckResidency(ctx, nil, ResidencyRequest{
		AccountID:  1,
		CatalogID:  1,
		CatalogURI: "https://catalog.example.com",
		Endpoint:   "s3.me-central-1.amazonaws.com",
		Region:     "me-central-1",
		Bucket:     "gold",
	})
	if err == nil {
		t.Fatalf("missing cluster allow policy must deny")
	}
	if !strings.Contains(err.Error(), "no cluster residency policy configured") {
		t.Fatalf("expected no-cluster-policy diagnostic, got %v", err)
	}
}

func TestResidencyWildcardAndInactivePoliciesForObjectAccess(t *testing.T) {
	ctx := context.Background()
	req := ResidencyRequest{
		AccountID:  42,
		CatalogID:  7,
		CatalogURI: "https://catalog.example.com/rest",
		Endpoint:   "https://s3.me-central-1.amazonaws.com",
		Region:     "me-central-1",
		Bucket:     "gold",
	}
	inactivePolicies := []model.ResidencyPolicy{
		{
			ScopeType:         model.ResidencyScopeCluster,
			CatalogID:         7,
			AllowedCatalogURI: "https://catalog.example.com/rest",
			AllowedEndpoint:   "s3.me-central-1.amazonaws.com",
			AllowedRegion:     model.ResidencyWildcard,
			AllowedBucket:     model.ResidencyWildcard,
			PolicyState:       model.ResidencyPolicyAudit,
		},
		{
			ScopeType:         model.ResidencyScopeCluster,
			CatalogID:         7,
			AllowedCatalogURI: "https://catalog.example.com/rest",
			AllowedEndpoint:   "s3.me-central-1.amazonaws.com",
			AllowedRegion:     model.ResidencyWildcard,
			AllowedBucket:     model.ResidencyWildcard,
			PolicyState:       model.ResidencyPolicyDisabled,
		},
	}
	if err := CheckResidency(ctx, inactivePolicies, req); err == nil || !strings.Contains(err.Error(), "no cluster residency policy configured") {
		t.Fatalf("audit/disabled cluster policies must not grant object access, got %v", err)
	}

	enabledWildcardPolicy := inactivePolicies[0]
	enabledWildcardPolicy.PolicyState = model.ResidencyPolicyEnabled
	if err := CheckResidency(ctx, []model.ResidencyPolicy{enabledWildcardPolicy}, req); err != nil {
		t.Fatalf("enabled wildcard region/bucket policy should allow object access: %v", err)
	}
}

func TestResidencySkipsInactivePoliciesBeforeEndpointValidation(t *testing.T) {
	ctx := context.Background()
	req := ResidencyRequest{
		AccountID:  42,
		CatalogID:  7,
		CatalogURI: "http://127.0.0.1:19120/iceberg",
		Endpoint:   "localhost",
		Region:     "us-east-1",
		Bucket:     "mo-iceberg",
	}
	invalidInactive := model.ResidencyPolicy{
		ScopeType:         model.ResidencyScopeCluster,
		CatalogID:         7,
		AllowedCatalogURI: "http://127.0.0.1:19120/iceberg",
		AllowedEndpoint:   "127.0.0.1",
		AllowedRegion:     "us-east-1",
		AllowedBucket:     "mo-iceberg",
		PolicyState:       model.ResidencyPolicyDisabled,
	}
	validEnabled := invalidInactive
	validEnabled.AllowedEndpoint = "localhost"
	validEnabled.PolicyState = model.ResidencyPolicyEnabled
	if err := CheckResidency(ctx, []model.ResidencyPolicy{invalidInactive, validEnabled}, req); err != nil {
		t.Fatalf("disabled invalid endpoint policy should be ignored: %v", err)
	}

	invalidEnabled := invalidInactive
	invalidEnabled.PolicyState = model.ResidencyPolicyEnabled
	if err := CheckResidency(ctx, []model.ResidencyPolicy{invalidEnabled, validEnabled}, req); err == nil ||
		!strings.Contains(err.Error(), "DNS host") {
		t.Fatalf("enabled invalid endpoint policy should fail validation, got %v", err)
	}
}

func TestResidencyPolicyPrivilegeValidation(t *testing.T) {
	ctx := context.Background()
	clusterPolicy := model.ResidencyPolicy{
		ScopeType:         model.ResidencyScopeCluster,
		CatalogID:         7,
		AllowedCatalogURI: "https://catalog.example.com/rest",
		AllowedEndpoint:   "s3.me-central-1.amazonaws.com",
		AllowedRegion:     model.ResidencyWildcard,
		AllowedBucket:     model.ResidencyWildcard,
		PolicyState:       model.ResidencyPolicyEnabled,
	}
	if err := ValidateResidencyPolicyPrivilege(ctx, 42, false, clusterPolicy); err == nil {
		t.Fatalf("non-admin must not manage cluster residency policy")
	}
	if err := ValidateResidencyPolicyPrivilege(ctx, 42, true, clusterPolicy); err != nil {
		t.Fatalf("cluster admin should manage cluster residency policy: %v", err)
	}

	accountPolicy := clusterPolicy
	accountPolicy.ScopeType = model.ResidencyScopeAccount
	accountPolicy.AccountID = 42
	accountPolicy.AllowedBucket = "gold"
	if err := ValidateResidencyPolicyPrivilege(ctx, 42, false, accountPolicy); err != nil {
		t.Fatalf("target account should manage own account residency policy: %v", err)
	}
	if err := ValidateResidencyPolicyPrivilege(ctx, 43, false, accountPolicy); err == nil {
		t.Fatalf("cross-account non-admin must not manage residency policy")
	}
	if err := ValidateResidencyPolicyPrivilege(ctx, 43, true, accountPolicy); err != nil {
		t.Fatalf("cluster admin should manage account residency policy: %v", err)
	}
}

func TestCheckObjectScopeResidency(t *testing.T) {
	ctx := context.Background()
	policies := []model.ResidencyPolicy{
		{
			ScopeType:         model.ResidencyScopeCluster,
			CatalogID:         7,
			AllowedCatalogURI: "https://catalog.example.com/rest",
			AllowedEndpoint:   "s3.me-central-1.amazonaws.com",
			AllowedRegion:     "me-central-1",
			AllowedBucket:     model.ResidencyWildcard,
			PolicyState:       model.ResidencyPolicyEnabled,
		},
		{
			ScopeType:         model.ResidencyScopeAccount,
			AccountID:         42,
			CatalogID:         7,
			AllowedCatalogURI: "https://catalog.example.com/rest",
			AllowedEndpoint:   "s3.me-central-1.amazonaws.com",
			AllowedRegion:     "me-central-1",
			AllowedBucket:     "gold",
			PolicyState:       model.ResidencyPolicyEnabled,
		},
	}
	scope := icebergio.ObjectScope{
		AccountID:       42,
		CatalogID:       7,
		StorageLocation: "s3://gold/t/data.parquet",
		Endpoint:        "HTTPS://S3.ME-CENTRAL-1.AMAZONAWS.COM/",
		Region:          "ME-CENTRAL-1",
		Bucket:          "gold",
		Principal:       "external",
	}
	if err := CheckObjectScopeResidency(ctx, policies, "https://catalog.example.com/rest", scope); err != nil {
		t.Fatalf("expected object scope residency allow: %v", err)
	}
	scope.Bucket = "silver"
	if err := CheckObjectScopeResidency(ctx, policies, "https://catalog.example.com/rest", scope); err == nil || !strings.Contains(err.Error(), "ICEBERG_RESIDENCY_DENIED") {
		t.Fatalf("expected account policy to deny object scope, got %v", err)
	}
}

func TestResidencyValidatorAdapters(t *testing.T) {
	ctx := context.Background()
	policies := []model.ResidencyPolicy{
		{
			ScopeType:         model.ResidencyScopeCluster,
			CatalogID:         7,
			AllowedCatalogURI: "https://catalog.example.com/rest",
			AllowedEndpoint:   "s3.me-central-1.amazonaws.com",
			AllowedRegion:     "me-central-1",
			AllowedBucket:     model.ResidencyWildcard,
			PolicyState:       model.ResidencyPolicyEnabled,
		},
		{
			ScopeType:         model.ResidencyScopeAccount,
			AccountID:         42,
			CatalogID:         7,
			AllowedCatalogURI: "https://catalog.example.com/rest",
			AllowedEndpoint:   "s3.me-central-1.amazonaws.com",
			AllowedRegion:     "me-central-1",
			AllowedBucket:     "gold",
			PolicyState:       model.ResidencyPolicyEnabled,
		},
	}

	catalogValidator := CatalogRequestResidencyValidator(policies)
	err := catalogValidator(ctx, api.CatalogRequest{Catalog: model.Catalog{
		AccountID: 42,
		CatalogID: 7,
		URI:       "HTTPS://Catalog.Example.com/rest",
	}})
	if err != nil {
		t.Fatalf("catalog request validator should allow normalized catalog URI: %v", err)
	}
	err = catalogValidator(ctx, api.CatalogRequest{Catalog: model.Catalog{
		AccountID: 42,
		CatalogID: 7,
		URI:       "https://other.example.com/rest",
	}})
	if err == nil || !strings.Contains(err.Error(), "ICEBERG_RESIDENCY_DENIED") {
		t.Fatalf("catalog request validator should deny unknown catalog URI, got %v", err)
	}

	objectValidator := ObjectScopeResidencyValidator(policies, "https://catalog.example.com/rest")
	err = objectValidator(ctx, icebergio.ObjectScope{
		AccountID:       42,
		CatalogID:       7,
		StorageLocation: "s3://gold/t/data.parquet",
		Endpoint:        "s3.me-central-1.amazonaws.com",
		Region:          "ME-CENTRAL-1",
		Bucket:          "gold",
		Principal:       "external",
	})
	if err != nil {
		t.Fatalf("object scope validator should allow account bucket: %v", err)
	}
	err = objectValidator(ctx, icebergio.ObjectScope{
		AccountID:       42,
		CatalogID:       7,
		StorageLocation: "s3://silver/t/data.parquet",
		Endpoint:        "s3.me-central-1.amazonaws.com",
		Region:          "me-central-1",
		Bucket:          "silver",
		Principal:       "external",
	})
	if err == nil || !strings.Contains(err.Error(), "ICEBERG_RESIDENCY_DENIED") {
		t.Fatalf("object scope validator should deny bucket outside account policy, got %v", err)
	}

	objectRequestValidator := ObjectResidencyRequestValidator(policies, "https://catalog.example.com/rest")
	err = objectRequestValidator(ctx, api.ObjectResidencyRequest{
		AccountID: 42,
		CatalogID: 7,
		Endpoint:  "s3.me-central-1.amazonaws.com",
		Region:    "me-central-1",
		Bucket:    "gold",
	})
	if err != nil {
		t.Fatalf("object request validator should allow account bucket: %v", err)
	}
	err = objectRequestValidator(ctx, api.ObjectResidencyRequest{
		AccountID: 42,
		CatalogID: 7,
		Endpoint:  "s3.me-central-1.amazonaws.com",
		Region:    "me-central-1",
		Bucket:    "silver",
	})
	if err == nil || !strings.Contains(err.Error(), "ICEBERG_RESIDENCY_DENIED") {
		t.Fatalf("object request validator should deny bucket outside account policy, got %v", err)
	}
}
