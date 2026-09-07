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

	"github.com/matrixorigin/matrixone/pkg/catalog"

	"github.com/stretchr/testify/require"
)

// A rolling upgrade puts a new CN in front of both table shapes: tables it created carry
// nrow/build_ts, tables created before the columns existed do not until their tenant's v4_0_7
// migration runs. One writer has to be right on both, which is why it names its columns.
func TestHasProvenanceColumnsMemoizesOnlyThePositive(t *testing.T) {
	const db, tbl = "shapedb", "shapetbl"
	t.Cleanup(func() { ForgetProvenanceShape(db, tbl) })

	// No session: nothing can be asked, so the writer takes the shape that works on both.
	require.False(t, HasProvenanceColumns(nil, db, tbl, catalog.Hnsw_TblCol_Metadata_Build_Ts))
	require.False(t, HasProvenanceColumns(nil, "", "", ""))

	provenanceShape.Store(db+"."+tbl, struct{}{})
	require.True(t, HasProvenanceColumns(nil, db, tbl, catalog.Hnsw_TblCol_Metadata_Build_Ts),
		"a table never loses the columns, so the positive answer needs no re-read")

	ForgetProvenanceShape(db, tbl)
	require.False(t, HasProvenanceColumns(nil, db, tbl, catalog.Hnsw_TblCol_Metadata_Build_Ts))
}
