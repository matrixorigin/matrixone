// Copyright 2024 Matrix Origin
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

package casgc

import (
	"context"
	"net/url"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/datalink"
)

// columnRef identifies a single column within a database table.
type columnRef struct {
	DBName    string
	TableName string
	ColName   string
}

// rowScanner reads the non-null string values of one column, optionally at a
// snapshot. snapshotHint is "" for live data, or "{snapshot = 'name'}".
type rowScanner interface {
	scanColumn(ctx context.Context, ref columnRef, snapshotHint string) ([]string, error)
}

// parseContentHash extracts a validated sha256 hex contenthash from a datalink
// URL string. It returns ("", false) if the URL cannot be parsed, if the
// contenthash query parameter is absent, or if the hash is not a valid sha256
// hex digest.
func parseContentHash(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	h := u.Query().Get(datalink.ContentHashKey)
	if h == "" {
		return "", false
	}
	if datalink.ValidateContentHash(h) != nil {
		return "", false
	}
	return h, true
}

// isDatalinkType reports whether the binary-encoded types.Type stored in
// atttyp represents a T_datalink column. atttyp must be the raw bytes returned
// by types.EncodeType. Returns false for nil or empty input.
func isDatalinkType(atttyp []byte) bool {
	if len(atttyp) == 0 {
		return false
	}
	return types.DecodeType(atttyp).Oid == types.T_datalink
}

// collectHashesFromColumns scans each column in cols, parses contenthashes
// from the returned datalink URL strings, and returns the deduplicated set of
// all valid hashes. The returned map is never nil. The first scan error is
// returned immediately.
func collectHashesFromColumns(
	ctx context.Context,
	sc rowScanner,
	cols []columnRef,
	snapshotHint string,
) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	for _, col := range cols {
		rows, err := sc.scanColumn(ctx, col, snapshotHint)
		if err != nil {
			return nil, err
		}
		for _, raw := range rows {
			if h, ok := parseContentHash(raw); ok {
				result[h] = struct{}{}
			}
		}
	}
	return result, nil
}
