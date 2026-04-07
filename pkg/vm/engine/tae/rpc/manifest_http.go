// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
)

// writeJSONError writes a JSON error response with proper escaping.
func writeJSONError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// manifestHTTPHandler returns an HTTP handler that serves manifest JSON.
func manifestHTTPHandler(d *db.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		table := r.URL.Query().Get("table")
		if table == "" {
			writeJSONError(w, "missing ?table=db.table parameter", http.StatusBadRequest)
			return
		}

		tbl, err := parseTableTarget(table, nil, d)
		if err != nil {
			if me, ok := err.(*moerr.Error); ok {
				switch me.ErrorCode() {
				case moerr.ErrInvalidInput:
					writeJSONError(w, err.Error(), http.StatusBadRequest)
				case moerr.ErrBadDB, moerr.ErrNoSuchTable, moerr.OkExpectedEOB:
					writeJSONError(w, err.Error(), http.StatusNotFound)
				default:
					writeJSONError(w, err.Error(), http.StatusInternalServerError)
				}
			} else {
				writeJSONError(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		dataDir := sharedFSRootPath(d)
		data, err := GenerateManifestPretty(tbl, dataDir)
		if err != nil {
			writeJSONError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}
}

var registerManifestOnce sync.Once

// RegisterManifestHTTP registers /debug/tae/manifest on the default HTTP mux.
// The endpoint is served by the debug HTTP server (-debug-http flag).
// Safe to call multiple times; the handler is registered only once.
//
// Usage:
//
//	GET /debug/tae/manifest?table=db.table
//
// Returns the DuckDB TAE scanner manifest as JSON.
func RegisterManifestHTTP(d *db.DB) {
	registerManifestOnce.Do(func() {
		http.HandleFunc("/debug/tae/manifest", manifestHTTPHandler(d))
		logutil.Infof("registered /debug/tae/manifest HTTP endpoint")
	})
}
