// Copyright 2025 Matrix Origin
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

package table_function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func TestAppendNativeTailBatchUsesRealRowIDs(t *testing.T) {
	mp := mpool.MustNewZero()

	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	pkVec := vector.NewVec(types.T_int64.ToType())
	contentVec := vector.NewVec(types.T_varchar.ToType())
	defer rowIDVec.Free(mp)
	defer pkVec.Free(mp)
	defer contentVec.Free(mp)

	obj1 := objectio.NewObjectid()
	obj2 := objectio.NewObjectid()
	rows := []types.Rowid{
		objectio.NewRowIDWithObjectIDBlkNumAndRowID(obj1, 0, 10),
		objectio.NewRowIDWithObjectIDBlkNumAndRowID(obj1, 0, 11),
		objectio.NewRowIDWithObjectIDBlkNumAndRowID(obj2, 1, 5),
	}
	pks := []int64{1, 1, 2}
	values := [][]byte{
		[]byte("mmmnnnppp"),
		[]byte("cccxxxzzz"),
		[]byte("cccxxxzzz"),
	}
	for i := range rows {
		require.NoError(t, vector.AppendFixed(rowIDVec, rows[i], false, mp))
		require.NoError(t, vector.AppendFixed(pkVec, pks[i], false, mp))
		require.NoError(t, vector.AppendBytes(contentVec, values[i], false, mp))
	}

	bat := batch.NewWithSize(3)
	bat.Attrs = []string{catalog.Row_ID, "id", "content"}
	bat.Vecs[0] = rowIDVec
	bat.Vecs[1] = pkVec
	bat.Vecs[2] = contentVec
	bat.SetRowCount(len(rows))

	resolved, err := resolveNativeTailBatchAttrs(bat, "id", []string{"content"})
	require.NoError(t, err)

	builders := make(map[string]*nativeTailSegmentBuilder)
	err = appendNativeTailBatch(
		builders,
		bat,
		resolved,
		types.T_int64,
		fulltext.FullTextParserParam{Parser: "ngram"},
	)
	require.NoError(t, err)

	objects, totalDocs, totalTokens := buildNativeTailSegmentsFromBuilders(builders)
	require.Len(t, objects, 2)
	require.Equal(t, int64(3), totalDocs)
	require.Greater(t, totalTokens, int64(0))

	obj1Name := objectio.BuildObjectNameWithObjectID(&obj1).String()
	obj2Name := objectio.BuildObjectNameWithObjectID(&obj2).String()
	var seg1, seg2 *nativeObjectSegment
	for i := range objects {
		require.True(t, objects[i].applyTombstones)
		switch objects[i].key {
		case obj1Name:
			seg1 = &objects[i]
		case obj2Name:
			seg2 = &objects[i]
		}
	}
	require.NotNil(t, seg1)
	require.NotNil(t, seg2)

	oldPostings := seg1.segment.Lookup("mmmnnnppp")
	require.Len(t, oldPostings, 1)
	require.Equal(t, uint16(0), oldPostings[0].Ref.Block)
	require.Equal(t, uint32(10), oldPostings[0].Ref.Row)

	newPostings1 := seg1.segment.Lookup("cccxxxzzz")
	require.Len(t, newPostings1, 1)
	require.Equal(t, uint16(0), newPostings1[0].Ref.Block)
	require.Equal(t, uint32(11), newPostings1[0].Ref.Row)

	newPostings2 := seg2.segment.Lookup("cccxxxzzz")
	require.Len(t, newPostings2, 1)
	require.Equal(t, uint16(1), newPostings2[0].Ref.Block)
	require.Equal(t, uint32(5), newPostings2[0].Ref.Row)
}
