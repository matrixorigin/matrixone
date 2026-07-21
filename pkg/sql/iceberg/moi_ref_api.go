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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type MOIRefCacheStatusLister interface {
	ListRefCacheStatus(ctx context.Context, accountID uint32, catalogID uint64, namespace, tableName string, staleAfter time.Duration, now time.Time) ([]RefCacheStatus, error)
}

type MOIRefCacheAPI struct {
	Lister MOIRefCacheStatusLister
	Now    func() time.Time
}

type MOIRefCacheStatusRequest struct {
	AccountID  uint32
	CatalogID  uint64
	Namespace  string
	TableName  string
	StaleAfter time.Duration
}

type MOIRefCacheStatusResponse struct {
	AccountID uint32             `json:"account_id"`
	CatalogID uint64             `json:"catalog_id"`
	Namespace string             `json:"namespace"`
	TableName string             `json:"table_name"`
	Refs      []MOIRefCacheEntry `json:"refs"`
}

type MOIRefCacheEntry struct {
	RefName        string `json:"ref_name"`
	RefType        string `json:"ref_type"`
	SnapshotID     string `json:"snapshot_id"`
	LastSeenAt     string `json:"last_seen_at"`
	AgeSeconds     int64  `json:"age_seconds"`
	Stale          bool   `json:"stale"`
	Source         string `json:"source"`
	Status         string `json:"status"`
	Version        uint64 `json:"version"`
	DisplayCaption string `json:"display_caption"`
}

func (api MOIRefCacheAPI) ListRefCacheStatus(ctx context.Context, req MOIRefCacheStatusRequest) (MOIRefCacheStatusResponse, error) {
	if api.Lister == nil {
		return MOIRefCacheStatusResponse{}, moerr.NewInvalidInput(ctx, "iceberg MOI ref cache API requires a lister")
	}
	if req.AccountID == 0 || req.CatalogID == 0 || trimNonEmpty(req.Namespace) == "" || trimNonEmpty(req.TableName) == "" {
		return MOIRefCacheStatusResponse{}, moerr.NewInvalidInput(ctx, "iceberg MOI ref cache API requires account_id, catalog_id, namespace, and table_name")
	}
	now := time.Now()
	if api.Now != nil {
		now = api.Now()
	}
	statuses, err := api.Lister.ListRefCacheStatus(ctx, req.AccountID, req.CatalogID, req.Namespace, req.TableName, req.StaleAfter, now)
	if err != nil {
		return MOIRefCacheStatusResponse{}, err
	}
	resp := MOIRefCacheStatusResponse{
		AccountID: req.AccountID,
		CatalogID: req.CatalogID,
		Namespace: req.Namespace,
		TableName: req.TableName,
		Refs:      make([]MOIRefCacheEntry, 0, len(statuses)),
	}
	for _, status := range statuses {
		resp.Refs = append(resp.Refs, moiRefCacheEntry(status))
	}
	return resp, nil
}

func moiRefCacheEntry(status RefCacheStatus) MOIRefCacheEntry {
	lastSeen := ""
	if !status.LastSeenAt.IsZero() {
		lastSeen = status.LastSeenAt.UTC().Format(time.RFC3339Nano)
	}
	ageSeconds := int64(status.Age / time.Second)
	state := "fresh"
	if status.Stale {
		state = "stale"
	}
	return MOIRefCacheEntry{
		RefName:        status.RefName,
		RefType:        status.RefType,
		SnapshotID:     status.SnapshotID,
		LastSeenAt:     lastSeen,
		AgeSeconds:     ageSeconds,
		Stale:          status.Stale,
		Source:         status.Source,
		Status:         state,
		Version:        status.Version,
		DisplayCaption: status.RefType + ":" + status.RefName,
	}
}

var _ MOIRefCacheStatusLister = (*DAO)(nil)
