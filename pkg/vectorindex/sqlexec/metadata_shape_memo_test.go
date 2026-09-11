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

package sqlexec

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"

	"github.com/stretchr/testify/require"
)

// A rolling upgrade puts a new CN in front of both table shapes: tables it created carry
// nrow/build_ts, tables created before the columns existed do not until their tenant's v4_0_7
// migration runs. One writer has to be right on both, which is why it names its columns.
func TestHasProvenanceColumnsMemoizesBothAnswers(t *testing.T) {
	const db, tbl = "shapedb", "shapetbl"
	t.Cleanup(func() { ForgetProvenanceShape(db, tbl) })

	// No session: nothing can be asked, so the writer takes the shape that works on both.
	require.False(t, HasProvenanceColumns(nil, db, tbl, catalog.Hnsw_TblCol_Metadata_Build_Ts))
	require.False(t, HasProvenanceColumns(nil, "", "", ""))

	MarkProvenanceColumns(db, tbl)
	require.True(t, HasProvenanceColumns(nil, db, tbl, catalog.Hnsw_TblCol_Metadata_Build_Ts))

	ForgetProvenanceShape(db, tbl)
	require.False(t, HasProvenanceColumns(nil, db, tbl, catalog.Hnsw_TblCol_Metadata_Build_Ts))
}

// A memo that never expires is wrong in BOTH directions, and one TTL closes both.
//
// A RESTORE / PITR / CLONE can put a pre-widening generation back under the same qualified
// name. While a stale "has them" stands, every writer names build_ts and every CDC flush for
// that index fails with "unknown column" -- until a CN restart, when the memo was permanent.
// And the negative answer was not cached at all, so an un-migrated table paid a
// mo_columns round-trip on every single flush.
func TestProvenanceShapeMemoExpires(t *testing.T) {
	const db, tbl = "shapedb-ttl", "shapetbl-ttl"
	t.Cleanup(func() { ForgetProvenanceShape(db, tbl) })
	key := db + "." + tbl

	// The negative answer is remembered, so a flush on an un-migrated table does not re-probe.
	provenanceShape.Store(key, provenanceShapeEntry{has: false, fetched: time.Now()})
	require.False(t, HasProvenanceColumns(nil, db, tbl, catalog.Hnsw_TblCol_Metadata_Build_Ts))
	e, ok := provenanceShape.Load(key)
	require.True(t, ok, "a fresh negative is not discarded by the read")
	require.False(t, e.(provenanceShapeEntry).has)

	// Aged past the TTL, neither answer stands: the writer re-probes. With no session to probe
	// with, that degrades to the shape that works on both -- never to the stale answer.
	provenanceShape.Store(key, provenanceShapeEntry{has: true, fetched: time.Now().Add(-provenanceShapeTTL - time.Second)})
	require.False(t, HasProvenanceColumns(nil, db, tbl, catalog.Hnsw_TblCol_Metadata_Build_Ts),
		"an expired positive must be re-read, or a restored narrow table stays broken until restart")

	provenanceShape.Store(key, provenanceShapeEntry{has: false, fetched: time.Now().Add(-provenanceShapeTTL - time.Second)})
	require.False(t, HasProvenanceColumns(nil, db, tbl, catalog.Hnsw_TblCol_Metadata_Build_Ts))
}
