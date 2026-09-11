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

package catalog

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
)

// IndexMetadataRow formats one row of an index metadata table: the four columns every version has,
// plus nrow and build_ts when the table carries them.
//
// hnsw, cagra and ivfpq share the column list, so they share this -- named from the shared
// IndexMetadata_TblCol_* set rather than any one algo's aliases of it.
func IndexMetadataRow(provenance bool, indexID, checksum string, ts, filesize, nrow, buildTS int64) string {
	if !provenance {
		return fmt.Sprintf("(%s, %s, %d, %d)",
			sqlquote.String(indexID), sqlquote.String(checksum), ts, filesize)
	}
	return fmt.Sprintf("(%s, %s, %d, %d, %d, %d)",
		sqlquote.String(indexID), sqlquote.String(checksum), ts, filesize, nrow, buildTS)
}

// IndexMetadataInsertSql builds the metadata INSERT for rows from IndexMetadataRow.
//
// It NAMES its columns rather than relying on position, which is what lets one CN write to both
// table shapes during a rolling upgrade: a metadata table is created per index at CREATE INDEX,
// so an index created before nrow/build_ts existed keeps the four-column shape until its
// tenant's v4_0_7 migration widens it, and that migration is asynchronous while this CN is
// already serving the tenant. A positional INSERT is wrong on exactly one of the two shapes,
// whichever it is written for; a named one is right on both.
//
// Omitting the provenance columns leaves them at their default 0, which is already the
// documented "unknown" sentinel, so the conservative direction costs provenance and nothing else.
func IndexMetadataInsertSql(db, table string, provenance bool, rows []string) string {
	cols := []string{
		IndexMetadata_TblCol_Index_Id,
		IndexMetadata_TblCol_Checksum,
		IndexMetadata_TblCol_Timestamp,
		IndexMetadata_TblCol_Filesize,
	}
	if provenance {
		cols = append(cols,
			IndexMetadata_TblCol_Nrow,
			IndexMetadata_TblCol_Build_Ts)
	}
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = sqlquote.Ident(c)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		sqlquote.QualifiedIdent(db, table), strings.Join(quoted, ", "), strings.Join(rows, ", "))
}
