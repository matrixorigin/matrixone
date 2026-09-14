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
	"testing"

	"github.com/stretchr/testify/require"
)

// hnsw, cagra and ivfpq share one metadata table shape. Their per-algo names are aliases of the
// shared set, and three separate writers depend on that: each algo's CREATE, the INSERT builder,
// and the v4_0_7 ALTER. Replacing one alias with its own literal splits them -- the CREATE moves
// and the other two do not -- and the break surfaces at runtime as "unknown column", not here.
// So it is pinned here.
func TestVectorIndexMetadataColumnsAreOneSharedSet(t *testing.T) {
	for _, algo := range []struct {
		name                                                string
		indexId, timestamp, checksum, filesize, nrow, build string
	}{
		{"hnsw", Hnsw_TblCol_Metadata_Index_Id, Hnsw_TblCol_Metadata_Timestamp,
			Hnsw_TblCol_Metadata_Checksum, Hnsw_TblCol_Metadata_Filesize,
			Hnsw_TblCol_Metadata_Nrow, Hnsw_TblCol_Metadata_Build_Ts},
		{"cagra", Cagra_TblCol_Metadata_Index_Id, Cagra_TblCol_Metadata_Timestamp,
			Cagra_TblCol_Metadata_Checksum, Cagra_TblCol_Metadata_Filesize,
			Cagra_TblCol_Metadata_Nrow, Cagra_TblCol_Metadata_Build_Ts},
		{"ivfpq", Ivfpq_TblCol_Metadata_Index_Id, Ivfpq_TblCol_Metadata_Timestamp,
			Ivfpq_TblCol_Metadata_Checksum, Ivfpq_TblCol_Metadata_Filesize,
			Ivfpq_TblCol_Metadata_Nrow, Ivfpq_TblCol_Metadata_Build_Ts},
	} {
		t.Run(algo.name, func(t *testing.T) {
			require.Equal(t, IndexMetadata_TblCol_Index_Id, algo.indexId)
			require.Equal(t, IndexMetadata_TblCol_Timestamp, algo.timestamp)
			require.Equal(t, IndexMetadata_TblCol_Checksum, algo.checksum)
			require.Equal(t, IndexMetadata_TblCol_Filesize, algo.filesize)
			require.Equal(t, IndexMetadata_TblCol_Nrow, algo.nrow)
			require.Equal(t, IndexMetadata_TblCol_Build_Ts, algo.build)
		})
	}

	// fulltext2 is NOT in the set: its metadata table carries an extra recency column and it
	// builds its own SQL from its own constants. Only the two provenance columns are shared,
	// and the migration names fulltext2's own build_ts for exactly that reason.
	require.Equal(t, IndexMetadata_TblCol_Build_Ts, FullText2Index_TblCol_Metadata_Build_Ts)
	require.Equal(t, IndexMetadata_TblCol_Nrow, FullText2Index_TblCol_Metadata_Nrow)
}
