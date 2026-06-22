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

package cuvs

import (
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// PendingRecord is one source-table row buffered for CDC tag=1
// emission when the cuvs build can't take it (small dataset or
// partial trailing chunk below intermediate_graph_degree / lists).
// Vec must be a self-owned copy — the table-function row buffer
// underneath argVecs is reused per call.
// Include is the already-encoded INCLUDE-column payload (matches
// includeBytesPerRow); pass nil when the index has no INCLUDE
// columns.
type PendingRecord struct {
	Pkid    int64
	Vec     []float32
	Include []byte
}

// SaveSmallTailAsCdc encodes rows as cuvs CDC tag=1 INSERT records
// under tblcfg.IndexTable with index_id = vectorindex.CdcTailId.
// Used by cagra_create / ivfpq_create when the trailing partial
// chunk is smaller than the cuvs minimum — those rows land in the
// event log so brute-force replay can serve them until the next
// rebuild lifts the tail back above threshold.
//
// Returns the INSERT SQL strings the caller must run inside the
// build txn. chunk_id starts at 0; the build txn already wipes the
// storage table for this index slice (ALTER REINDEX) or is run
// once at CREATE INDEX with the table empty.
//
// colMetaJSON (cuvscdc.ResolveIncludeColumns output) is embedded in
// the frame header section of every emitted chunk so the search side
// can recover the INCLUDE-column layout for tag=1 replay even when no
// tag=0 sub-index exists. Pass "" for indexes with no INCLUDE columns.
//
// When rows is empty, returns nil. Empty source + INCLUDE columns is
// handled by the first ongoing CDC iteration (CagraSync.Save /
// IvfpqSync.Save), which also embeds colMetaJSON in its frames.
func SaveSmallTailAsCdc(
	tblcfg vectorindex.IndexTableConfig,
	rows []PendingRecord,
	dim int,
	includeBytesPerRow int,
	colMetaJSON string,
) ([]string, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	// Pre-size the buffer: 9 (op + pkid) + 4*dim + ibpr bytes per
	// INSERT record. Avoids ~len(rows) reallocs in EncodeEventRecord.
	perRow := 9 + 4*dim + includeBytesPerRow
	records := make([]byte, 0, perRow*len(rows))
	sizes := make([]int, 0, len(rows))

	for _, r := range rows {
		before := len(records)
		out, err := EncodeEventRecord(records, CdcOpInsert,
			r.Pkid, r.Vec, r.Include, dim, includeBytesPerRow)
		if err != nil {
			return nil, err
		}
		records = out
		sizes = append(sizes, len(records)-before)
	}

	return CdcAppendEventsSql(tblcfg, vectorindex.CdcTailId, 0, records, sizes, colMetaJSON)
}
