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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func newRequestWithQuery(t *testing.T, q string) *http.Request {
	t.Helper()
	url := "/x"
	if q != "" {
		url += "?" + q
	}
	return httptest.NewRequest(http.MethodGet, url, nil)
}

func TestOptionalUint32Param(t *testing.T) {
	v, err := optionalUint32Param(newRequestWithQuery(t, ""), "k")
	require.NoError(t, err)
	require.Nil(t, v)

	v, err = optionalUint32Param(newRequestWithQuery(t, "k=42"), "k")
	require.NoError(t, err)
	require.NotNil(t, v)
	require.Equal(t, uint32(42), *v)

	_, err = optionalUint32Param(newRequestWithQuery(t, "k=abc"), "k")
	require.Error(t, err)

	_, err = optionalUint32Param(newRequestWithQuery(t, "k=99999999999"), "k")
	require.Error(t, err)
}

func TestOptionalUint64Param(t *testing.T) {
	v, err := optionalUint64Param(newRequestWithQuery(t, ""), "k")
	require.NoError(t, err)
	require.Nil(t, v)

	v, err = optionalUint64Param(newRequestWithQuery(t, "k=12345"), "k")
	require.NoError(t, err)
	require.NotNil(t, v)
	require.Equal(t, uint64(12345), *v)

	_, err = optionalUint64Param(newRequestWithQuery(t, "k=oops"), "k")
	require.Error(t, err)
}

func TestRequiredUint32Param(t *testing.T) {
	_, err := requiredUint32Param(newRequestWithQuery(t, ""), "k")
	require.Error(t, err)

	v, err := requiredUint32Param(newRequestWithQuery(t, "k=7"), "k")
	require.NoError(t, err)
	require.Equal(t, uint32(7), v)

	_, err = requiredUint32Param(newRequestWithQuery(t, "k=bad"), "k")
	require.Error(t, err)
}

func TestOptionalTimestampParam(t *testing.T) {
	ts, err := optionalTimestampParam(newRequestWithQuery(t, ""), "k")
	require.NoError(t, err)
	require.Nil(t, ts)

	ts, err = optionalTimestampParam(newRequestWithQuery(t, "k=10"), "k")
	require.NoError(t, err)
	require.NotNil(t, ts)
	require.Equal(t, int64(10), ts.PhysicalTime)
	require.Equal(t, uint32(0), ts.LogicalTime)

	ts, err = optionalTimestampParam(newRequestWithQuery(t, "k=10:5"), "k")
	require.NoError(t, err)
	require.NotNil(t, ts)
	require.Equal(t, int64(10), ts.PhysicalTime)
	require.Equal(t, uint32(5), ts.LogicalTime)

	_, err = optionalTimestampParam(newRequestWithQuery(t, "k=1:2:3"), "k")
	require.Error(t, err)

	_, err = optionalTimestampParam(newRequestWithQuery(t, "k=bad"), "k")
	require.Error(t, err)

	_, err = optionalTimestampParam(newRequestWithQuery(t, "k=-1"), "k")
	require.Error(t, err)

	_, err = optionalTimestampParam(newRequestWithQuery(t, "k=1:bad"), "k")
	require.Error(t, err)
}

func TestIntParam(t *testing.T) {
	v, err := intParam(newRequestWithQuery(t, ""), "k", 7)
	require.NoError(t, err)
	require.Equal(t, 7, v)

	v, err = intParam(newRequestWithQuery(t, "k=42"), "k", 0)
	require.NoError(t, err)
	require.Equal(t, 42, v)

	_, err = intParam(newRequestWithQuery(t, "k=bad"), "k", 0)
	require.Error(t, err)

	_, err = intParam(newRequestWithQuery(t, "k=0"), "k", 0)
	require.Error(t, err)

	_, err = intParam(newRequestWithQuery(t, "k=-1"), "k", 0)
	require.Error(t, err)
}

func TestSelectCNInstance(t *testing.T) {
	server := NewServer()

	// no instances registered.
	_, _, err := server.selectCNInstance("")
	require.Error(t, err)

	// explicit cn, not found.
	_, _, err = server.selectCNInstance("missing")
	require.Error(t, err)

	server.SetEngine("cn1", &disttae.Engine{})
	uuid, inst, err := server.selectCNInstance("")
	require.NoError(t, err)
	require.Equal(t, "cn1", uuid)
	require.NotNil(t, inst)

	uuid, inst, err = server.selectCNInstance("cn1")
	require.NoError(t, err)
	require.Equal(t, "cn1", uuid)
	require.NotNil(t, inst)

	// multiple instances - empty cn must fail.
	server.SetEngine("cn2", &disttae.Engine{})
	_, _, err = server.selectCNInstance("")
	require.Error(t, err)

	uuid, _, err = server.selectCNInstance("cn2")
	require.NoError(t, err)
	require.Equal(t, "cn2", uuid)
}

func TestServeHTTPNotFound(t *testing.T) {
	server := NewServer()
	req := httptest.NewRequest(http.MethodGet, "/debug/status/unknown", nil)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)
	require.Equal(t, http.StatusNotFound, resp.Code)
}

func TestServeHTTPDump(t *testing.T) {
	server := NewServer()
	req := httptest.NewRequest(http.MethodGet, "/debug/status", nil)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
}

func TestServeHTTPCatalogRejectsInvalidAccount(t *testing.T) {
	server := NewServer()
	server.SetEngine("cn1", &disttae.Engine{})

	req := httptest.NewRequest(http.MethodGet, "/debug/status/catalog?account=bad", nil)
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)
	require.Equal(t, http.StatusBadRequest, resp.Code)
}

func TestServeHTTPCatalogCacheRejectsInvalidParams(t *testing.T) {
	server := NewServer()
	server.SetEngine("cn1", &disttae.Engine{})

	for _, q := range []string{
		"account=bad",
		"account=1&snapshot=bad",
		"account=1&limit=bad",
	} {
		req := httptest.NewRequest(http.MethodGet, "/debug/status/catalog-cache?"+q, nil)
		resp := httptest.NewRecorder()
		server.ServeHTTP(resp, req)
		require.Equalf(t, http.StatusBadRequest, resp.Code, "query=%s", q)
	}
}

func TestServeHTTPCatalogActivationRejectsInvalidParams(t *testing.T) {
	server := NewServer()
	server.SetEngine("cn1", &disttae.Engine{})

	for _, q := range []string{
		"account=bad",
		"limit=bad",
	} {
		req := httptest.NewRequest(http.MethodGet, "/debug/status/catalog-activation?"+q, nil)
		resp := httptest.NewRecorder()
		server.ServeHTTP(resp, req)
		require.Equalf(t, http.StatusBadRequest, resp.Code, "query=%s", q)
	}
}

func TestServeHTTPPartitionsRejectsInvalidParams(t *testing.T) {
	server := NewServer()
	server.SetEngine("cn1", &disttae.Engine{})

	for _, q := range []string{
		"db=bad",
		"table=bad",
		"limit=bad",
	} {
		req := httptest.NewRequest(http.MethodGet, "/debug/status/partitions?"+q, nil)
		resp := httptest.NewRecorder()
		server.ServeHTTP(resp, req)
		require.Equalf(t, http.StatusBadRequest, resp.Code, "query=%s", q)
	}
}

func TestServeHTTPHandlersRejectMissingEngine(t *testing.T) {
	// Register an instance with no Engine to hit the "engine nil" branches.
	server := NewServer()
	server.mu.CNInstances["cn1"] = &CNInstance{}

	for _, p := range []string{
		"/debug/status/catalog",
		"/debug/status/catalog-cache?account=1",
		"/debug/status/catalog-activation",
		"/debug/status/partitions",
	} {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		resp := httptest.NewRecorder()
		server.ServeHTTP(resp, req)
		require.Equalf(t, http.StatusBadRequest, resp.Code, "path=%s", p)
	}
}
