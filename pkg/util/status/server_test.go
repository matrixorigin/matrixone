// Copyright 2021 -2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package status

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func TestServeHTTPCatalog(t *testing.T) {
	server := NewServer()
	server.SetEngine("cn1", &disttae.Engine{})

	req := httptest.NewRequest(http.MethodGet, "/debug/status/catalog", nil)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload struct {
		CNUUID  string         `json:"cn_uuid"`
		Catalog map[string]any `json:"catalog"`
	}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &payload))
	require.Equal(t, "cn1", payload.CNUUID)
}

func TestServeHTTPCatalogRejectsInvalidSnapshot(t *testing.T) {
	server := NewServer()
	server.SetEngine("cn1", &disttae.Engine{})

	req := httptest.NewRequest(http.MethodGet, "/debug/status/catalog?snapshot=bad:value", nil)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)
	require.Equal(t, http.StatusBadRequest, resp.Code)
}

func TestServeHTTPCatalogRequiresCNWhenMultipleInstancesRegistered(t *testing.T) {
	server := NewServer()
	server.SetEngine("cn1", &disttae.Engine{})
	server.SetEngine("cn2", &disttae.Engine{})

	req := httptest.NewRequest(http.MethodGet, "/debug/status/catalog", nil)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)
	require.Equal(t, http.StatusBadRequest, resp.Code)

	req = httptest.NewRequest(http.MethodGet, "/debug/status/catalog?cn=cn2", nil)
	resp = httptest.NewRecorder()
	server.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
}

func TestServeHTTPCatalogCache(t *testing.T) {
	server := NewServer()
	server.SetEngine("cn1", &disttae.Engine{})

	req := httptest.NewRequest(http.MethodGet, "/debug/status/catalog-cache?account=1&snapshot=10:2&limit=5", nil)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload struct {
		CNUUID       string         `json:"cn_uuid"`
		CatalogCache map[string]any `json:"catalog_cache"`
	}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &payload))
	require.Equal(t, "cn1", payload.CNUUID)
}

func TestServeHTTPCatalogCacheRequiresAccount(t *testing.T) {
	server := NewServer()
	server.SetEngine("cn1", &disttae.Engine{})

	req := httptest.NewRequest(http.MethodGet, "/debug/status/catalog-cache", nil)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)
	require.Equal(t, http.StatusBadRequest, resp.Code)
}

func TestServeHTTPCatalogActivation(t *testing.T) {
	server := NewServer()
	server.SetEngine("cn1", &disttae.Engine{})

	req := httptest.NewRequest(http.MethodGet, "/debug/status/catalog-activation?limit=3", nil)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload struct {
		CNUUID            string         `json:"cn_uuid"`
		CatalogActivation map[string]any `json:"catalog_activation"`
	}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &payload))
	require.Equal(t, "cn1", payload.CNUUID)
}

func TestServeHTTPPartitions(t *testing.T) {
	server := NewServer()
	server.SetEngine("cn1", &disttae.Engine{})

	req := httptest.NewRequest(http.MethodGet, "/debug/status/partitions?limit=10", nil)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var payload struct {
		CNUUID     string         `json:"cn_uuid"`
		Partitions map[string]any `json:"partitions"`
	}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &payload))
	require.Equal(t, "cn1", payload.CNUUID)
}
