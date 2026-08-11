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
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type MOIRefCacheHTTPHandler struct {
	API MOIRefCacheAPI
}

func NewMOIRefCacheHTTPHandler(api MOIRefCacheAPI) http.Handler {
	return MOIRefCacheHTTPHandler{API: api}
}

func (h MOIRefCacheHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, err := parseMOIRefCacheHTTPRequest(r)
	if err != nil {
		writeMOIRefCacheHTTPError(w, http.StatusBadRequest, err)
		return
	}
	resp, err := h.API.ListRefCacheStatus(r.Context(), req)
	if err != nil {
		writeMOIRefCacheHTTPError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func parseMOIRefCacheHTTPRequest(r *http.Request) (MOIRefCacheStatusRequest, error) {
	q := r.URL.Query()
	accountID, err := parsePositiveUint32Query(q.Get("account_id"))
	if err != nil {
		return MOIRefCacheStatusRequest{}, err
	}
	catalogID, err := parsePositiveUint64Query(q.Get("catalog_id"))
	if err != nil {
		return MOIRefCacheStatusRequest{}, err
	}
	namespace := strings.TrimSpace(q.Get("namespace"))
	tableName := strings.TrimSpace(q.Get("table"))
	if tableName == "" {
		tableName = strings.TrimSpace(q.Get("table_name"))
	}
	staleAfter := time.Duration(0)
	if raw := strings.TrimSpace(q.Get("stale_after_seconds")); raw != "" {
		seconds, err := parsePositiveInt64Query(raw)
		if err != nil {
			return MOIRefCacheStatusRequest{}, err
		}
		staleAfter = time.Duration(seconds) * time.Second
	}
	return MOIRefCacheStatusRequest{
		AccountID:  accountID,
		CatalogID:  catalogID,
		Namespace:  namespace,
		TableName:  tableName,
		StaleAfter: staleAfter,
	}, nil
}

func parsePositiveUint32Query(raw string) (uint32, error) {
	value, err := parsePositiveUint64Query(raw)
	if err != nil {
		return 0, err
	}
	if value > uint64(^uint32(0)) {
		return 0, strconv.ErrRange
	}
	return uint32(value), nil
}

func parsePositiveUint64Query(raw string) (uint64, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, strconv.ErrSyntax
	}
	value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, err
	}
	if value == 0 {
		return 0, strconv.ErrSyntax
	}
	return value, nil
}

func parsePositiveInt64Query(raw string) (int64, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, strconv.ErrSyntax
	}
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, strconv.ErrSyntax
	}
	return value, nil
}

func writeMOIRefCacheHTTPError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": err.Error(),
	})
}

var _ http.Handler = MOIRefCacheHTTPHandler{}
