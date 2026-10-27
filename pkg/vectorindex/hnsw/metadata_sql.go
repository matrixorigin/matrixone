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

package hnsw

import (
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
)

// metadataRow is catalog.IndexMetadataRow followed by the bounds of an int8/uint8 model.
func metadataRow(provenance bool, indexID, checksum string, ts, filesize, nrow, buildTS int64, bounds QuantizeBounds) string {
	row := catalog.IndexMetadataRow(provenance, indexID, checksum, ts, filesize, nrow, buildTS)
	if !bounds.Set {
		return row
	}
	return row[:len(row)-1] + ", " + strconv.FormatFloat(bounds.Min, 'g', -1, 64) +
		", " + strconv.FormatFloat(bounds.Max, 'g', -1, 64) + ")"
}

// metadataInsertSql is catalog.IndexMetadataInsertSql naming the bounds columns too when the
// rows carry them.
func metadataInsertSql(db, table string, provenance, quantized bool, rows []string) string {
	if !quantized {
		return catalog.IndexMetadataInsertSql(db, table, provenance, rows)
	}
	prefix := strings.TrimSuffix(catalog.IndexMetadataInsertSql(db, table, provenance, nil), ") VALUES ")
	return prefix + ", " + sqlquote.Ident(catalog.Hnsw_TblCol_Metadata_Quantize_Min) +
		", " + sqlquote.Ident(catalog.Hnsw_TblCol_Metadata_Quantize_Max) + ") VALUES " + strings.Join(rows, ", ")
}
