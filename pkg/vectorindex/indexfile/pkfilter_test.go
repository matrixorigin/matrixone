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

package indexfile_test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/indexfile"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestChunkRangeIsPrimaryKeyFilter(t *testing.T) {
	ref := &indexfile.Ref{DB: "db", Table: "idx", IndexID: "idx:1:0", FirstChunk: 3, Size: 5*indexfile.ChunkSize + 1, Checksum: "c"}
	def := indexfile.StorageTableDef()
	filter, err := indexfile.ChunkRangeFilter(context.Background(), def, ref)
	require.NoError(t, err)

	pk, err := readutil.ConstructBasePKFilter(filter, def, mpool.MustNewZero())
	require.NoError(t, err)
	require.True(t, pk.Valid)
	require.Equal(t, indexfile.SerialKey(ref.IndexID, 3), pk.LB)
	require.Equal(t, indexfile.SerialKey(ref.IndexID, 8), pk.UB)
}
