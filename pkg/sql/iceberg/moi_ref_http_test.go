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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestMOIRefCacheHTTPHandlerServesRefStatus(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	lister := &fakeMOIRefCacheStatusLister{
		statuses: []RefCacheStatus{{
			RefCache: model.RefCache{
				AccountID:  7,
				CatalogID:  42,
				Namespace:  "sales",
				TableName:  "orders",
				RefName:    "main",
				RefType:    "branch",
				SnapshotID: "100",
				LastSeenAt: now.Add(-time.Minute),
				Source:     "metadata",
				Version:    3,
			},
			Age: time.Minute,
		}},
	}
	handler := NewMOIRefCacheHTTPHandler(MOIRefCacheAPI{
		Lister: lister,
		Now:    func() time.Time { return now },
	})

	req := httptest.NewRequest(http.MethodGet, "/iceberg/refs?account_id=7&catalog_id=42&namespace=sales&table=orders&stale_after_seconds=300", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if lister.staleAfter != 300*time.Second {
		t.Fatalf("unexpected stale_after: %s", lister.staleAfter)
	}
	var resp MOIRefCacheStatusResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.AccountID != 7 || resp.CatalogID != 42 || resp.Namespace != "sales" || resp.TableName != "orders" {
		t.Fatalf("unexpected response identity: %+v", resp)
	}
	if len(resp.Refs) != 1 || resp.Refs[0].DisplayCaption != "branch:main" || resp.Refs[0].Status != "fresh" {
		t.Fatalf("unexpected refs: %+v", resp.Refs)
	}
}

func TestMOIRefCacheHTTPHandlerRejectsBadRequest(t *testing.T) {
	handler := NewMOIRefCacheHTTPHandler(MOIRefCacheAPI{Lister: &fakeMOIRefCacheStatusLister{}})
	req := httptest.NewRequest(http.MethodGet, "/iceberg/refs?catalog_id=42&namespace=sales&table=orders", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

type fakeMOIRefCacheStatusLister struct {
	statuses   []RefCacheStatus
	staleAfter time.Duration
}

func (l *fakeMOIRefCacheStatusLister) ListRefCacheStatus(ctx context.Context, accountID uint32, catalogID uint64, namespace, tableName string, staleAfter time.Duration, now time.Time) ([]RefCacheStatus, error) {
	l.staleAfter = staleAfter
	return append([]RefCacheStatus(nil), l.statuses...), nil
}
