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

package api

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func TestIcebergErrorRedactsSensitiveFields(t *testing.T) {
	err := NewError(ErrCredentialExpired, "bad credential", map[string]string{
		"token":       "secret-token",
		"catalog":     "prod",
		"access_key":  "secret-key",
		"access-key":  "dashed-secret-key",
		"access.key":  "dotted-secret-key",
		"safe_region": "me-central-1",
	})
	msg := err.Error()
	if strings.Contains(msg, "secret-token") ||
		strings.Contains(msg, "secret-key") ||
		strings.Contains(msg, "dashed-secret-key") ||
		strings.Contains(msg, "dotted-secret-key") {
		t.Fatalf("sensitive fields leaked: %s", msg)
	}
	if !strings.Contains(msg, "catalog=prod") || !strings.Contains(msg, "token=<redacted>") {
		t.Fatalf("expected public and redacted fields: %s", msg)
	}
}

func TestIsSensitiveFieldNormalizesCommonSeparators(t *testing.T) {
	cases := []string{
		"access_key",
		"access-key",
		"access.key",
		"aws access key",
		"x-amz-security-token",
		"api-key",
		"signature",
		"authorization",
	}
	for _, key := range cases {
		if !IsSensitiveField(key) {
			t.Fatalf("expected key %q to be sensitive", key)
		}
	}
	if IsSensitiveField("safe_region") {
		t.Fatalf("safe_region should remain public")
	}
}

func TestRedactPath(t *testing.T) {
	path := "s3://warehouse/sales/orders/metadata/v1.json"
	redacted := RedactPath(path)
	if redacted == "" || strings.Contains(redacted, "warehouse") || strings.Contains(redacted, "orders") || strings.Contains(redacted, "v1.json") {
		t.Fatalf("path redaction leaked original path: %q", redacted)
	}
	if redacted != RedactPath(path) {
		t.Fatalf("path redaction should be stable")
	}
}

func TestToMOErrMapping(t *testing.T) {
	cases := []struct {
		code ErrorCode
		want uint16
	}{
		{ErrConfigInvalid, moerr.ErrBadConfig},
		{ErrFeatureDisabled, moerr.ErrNotSupported},
		{ErrAuthUnauthorized, moerr.ErrInvalidInput},
		{ErrResidencyDenied, moerr.ErrInvalidInput},
		{ErrCatalogUnavailable, moerr.ErrInvalidState},
		{ErrPlanningLimitExceeded, moerr.ErrInvalidState},
		{ErrInternal, moerr.ErrInternal},
	}
	for _, tc := range cases {
		err := ToMOErr(context.Background(), NewError(tc.code, "boom", nil))
		mo, ok := err.(*moerr.Error)
		if !ok {
			t.Fatalf("expected moerr for %s, got %T", tc.code, err)
		}
		if mo.ErrorCode() != tc.want {
			t.Fatalf("code %s maps to %d, want %d", tc.code, mo.ErrorCode(), tc.want)
		}
	}
}

func TestCauseForCode(t *testing.T) {
	if CauseForCode(ErrResidencyDenied) != moerr.CauseIcebergResidency {
		t.Fatalf("residency code should map to residency cause")
	}
	if CauseForCode(ErrMetadataInvalid) != moerr.CauseIcebergMetadata {
		t.Fatalf("metadata code should map to metadata cause")
	}
	if CauseForCode(ErrPlanningTimeout) != moerr.CauseIcebergPlanning {
		t.Fatalf("planning timeout code should map to planning cause")
	}
}
