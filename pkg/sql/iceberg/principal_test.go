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

func TestPrincipalMapSpecificity(t *testing.T) {
	candidates := []model.PrincipalMap{
		{AccountID: 1, CatalogID: 2, MORoleID: 10, ExternalPrincipal: "role"},
		{AccountID: 1, CatalogID: 2, MOUserID: 20, ExternalPrincipal: "user"},
		{AccountID: 1, CatalogID: 2, MORoleID: 10, MOUserID: 20, ExternalPrincipal: "exact"},
	}
	selected, ok, err := SelectPrincipalMap(context.Background(), candidates, 10, 20)
	if err != nil {
		t.Fatalf("select principal: %v", err)
	}
	if !ok || selected.ExternalPrincipal != "exact" {
		t.Fatalf("expected exact mapping, got %+v ok=%v", selected, ok)
	}
}

func TestPrincipalMapRequiresRoleOrUserSentinel(t *testing.T) {
	err := ValidatePrincipalMap(context.Background(), model.PrincipalMap{
		AccountID:         1,
		CatalogID:         2,
		ExternalPrincipal: "x",
	})
	if err == nil {
		t.Fatalf("expected role/user sentinel validation error")
	}
	if err := ValidatePrincipalMap(context.Background(), model.PrincipalMap{
		AccountID:         0,
		CatalogID:         2,
		ExternalPrincipal: "system-root",
	}); err != nil {
		t.Fatalf("system account root mapping should allow role/user id 0: %v", err)
	}
}

func TestPrincipalMapPrivilegeValidation(t *testing.T) {
	if err := ValidatePrincipalMapPrivilege(context.Background(), 10, 11, false); err == nil {
		t.Fatalf("cross-account non-admin mutation should be rejected")
	}
	if err := ValidatePrincipalMapPrivilege(context.Background(), 10, 11, true); err != nil {
		t.Fatalf("cluster admin should manage principal mapping: %v", err)
	}
	if err := ValidatePrincipalMapPrivilege(context.Background(), 10, 10, false); err != nil {
		t.Fatalf("target account should manage own principal mapping: %v", err)
	}
	if err := ValidatePrincipalMapPrivilege(context.Background(), 0, 0, true); err != nil {
		t.Fatalf("cluster admin should manage system account principal mapping: %v", err)
	}
}

func TestPrincipalNotMappedErrorUsesIcebergTaxonomy(t *testing.T) {
	err := PrincipalNotMappedError(context.Background(), 1, 2, 3, 4)
	if err == nil || !strings.Contains(err.Error(), "ICEBERG_PRINCIPAL_NOT_MAPPED") {
		t.Fatalf("expected principal-not-mapped iceberg error, got %v", err)
	}
}
