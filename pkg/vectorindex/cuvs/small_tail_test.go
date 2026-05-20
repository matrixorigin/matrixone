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
	"encoding/hex"
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

func smallTailTblcfg() vectorindex.IndexTableConfig {
	return vectorindex.IndexTableConfig{
		DbName:     "db",
		IndexTable: "__mo_cuvs_storage",
	}
}

// TestSaveSmallTailAsCdc_Empty: no rows → no SQL.
func TestSaveSmallTailAsCdc_Empty(t *testing.T) {
	sqls, err := SaveSmallTailAsCdc(smallTailTblcfg(), nil, 4, 0, "")
	require.NoError(t, err)
	require.Empty(t, sqls)
}

// TestSaveSmallTailAsCdc_NoInclude: a handful of rows without
// INCLUDE columns; the unhex'd payload must round-trip via
// UnframeCdcChunk + DecodeEventRecord and yield exactly the input.
func TestSaveSmallTailAsCdc_NoInclude(t *testing.T) {
	const dim = 3
	rows := []PendingRecord{
		{Pkid: 1, Vec: []float32{1, 2, 3}},
		{Pkid: 2, Vec: []float32{4, 5, 6}},
		{Pkid: -3, Vec: []float32{math.MaxFloat32, 0, -1}},
	}

	sqls, err := SaveSmallTailAsCdc(smallTailTblcfg(), rows, dim, 0, "")
	require.NoError(t, err)
	require.NotEmpty(t, sqls)

	// Extract every unhex('...') payload from the emitted SQL and
	// round-trip each framed chunk back to records.
	re := regexp.MustCompile(`unhex\('([0-9a-fA-F]*)'\)`)
	matches := re.FindAllStringSubmatch(strings.Join(sqls, " "), -1)
	require.NotEmpty(t, matches, "expected at least one unhex blob")

	got := make([]CdcEventRecord, 0, len(rows))
	for _, m := range matches {
		framed, err := hex.DecodeString(m[1])
		require.NoError(t, err)
		records, err := UnframeCdcChunk(framed)
		require.NoError(t, err)
		pos := 0
		for pos < len(records) {
			rec, n, ok := DecodeEventRecord(records[pos:], dim, 0)
			require.True(t, ok)
			got = append(got, rec)
			pos += n
		}
	}

	require.Equal(t, len(rows), len(got))
	for i, in := range rows {
		require.Equal(t, CdcOpInsert, got[i].Op)
		require.Equal(t, in.Pkid, got[i].Pkid)
		require.Len(t, got[i].Vec, dim)
		for j, v := range in.Vec {
			require.Equal(t, math.Float32bits(v), math.Float32bits(got[i].Vec[j]),
				"row %d vec[%d] mismatch", i, j)
		}
	}
}

// TestSaveSmallTailAsCdc_WithInclude: rows carry an INCLUDE payload;
// the payload bytes round-trip through DecodeEventRecord intact.
func TestSaveSmallTailAsCdc_WithInclude(t *testing.T) {
	const dim = 2
	const ibpr = 8 // one int64-shaped INCLUDE col + zero-mask byte rounded
	rows := []PendingRecord{
		{Pkid: 10, Vec: []float32{0.1, 0.2}, Include: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
		{Pkid: 11, Vec: []float32{0.3, 0.4}, Include: []byte{9, 10, 11, 12, 13, 14, 15, 16}},
	}

	// Empty colMetaJSON to keep this test focused on tag=1 INSERT
	// round-trip; the header-emission case has its own test below.
	sqls, err := SaveSmallTailAsCdc(smallTailTblcfg(), rows, dim, ibpr, "")
	require.NoError(t, err)
	require.NotEmpty(t, sqls)

	re := regexp.MustCompile(`unhex\('([0-9a-fA-F]*)'\)`)
	matches := re.FindAllStringSubmatch(strings.Join(sqls, " "), -1)
	require.NotEmpty(t, matches)

	got := make([]CdcEventRecord, 0, len(rows))
	for _, m := range matches {
		framed, err := hex.DecodeString(m[1])
		require.NoError(t, err)
		records, err := UnframeCdcChunk(framed)
		require.NoError(t, err)
		pos := 0
		for pos < len(records) {
			rec, n, ok := DecodeEventRecord(records[pos:], dim, ibpr)
			require.True(t, ok)
			got = append(got, rec)
			pos += n
		}
	}

	require.Equal(t, len(rows), len(got))
	for i, in := range rows {
		require.Equal(t, in.Pkid, got[i].Pkid)
		require.Equal(t, in.Include, got[i].Include)
	}
}

// TestSaveSmallTailAsCdc_IncludeMismatchErrors: include payload size
// must match includeBytesPerRow; mismatches surface from
// EncodeEventRecord.
func TestSaveSmallTailAsCdc_IncludeMismatchErrors(t *testing.T) {
	const dim = 2
	const ibpr = 8
	rows := []PendingRecord{
		{Pkid: 1, Vec: []float32{0.1, 0.2}, Include: []byte{1, 2, 3}}, // wrong length
	}
	_, err := SaveSmallTailAsCdc(smallTailTblcfg(), rows, dim, ibpr, "")
	require.Error(t, err)
}

// TestSaveSmallTailAsCdc_UsesCdcTailId: index_id in the emitted SQL
// must be the well-known CdcTailId sentinel so the search-side
// replay finds it.
func TestSaveSmallTailAsCdc_UsesCdcTailId(t *testing.T) {
	rows := []PendingRecord{{Pkid: 1, Vec: []float32{1, 2, 3, 4}}}
	sqls, err := SaveSmallTailAsCdc(smallTailTblcfg(), rows, 4, 0, "")
	require.NoError(t, err)
	require.NotEmpty(t, sqls)
	require.Contains(t, sqls[0], "'"+vectorindex.CdcTailId+"'")
}

// TestSaveSmallTailAsCdc_EmitsHeaderRecord: when colMetaJSON is set,
// the writer emits a CdcOpHeader record as the first record of
// chunk_id=0 so the search side can recover the INCLUDE-column layout
// even with no tag=0 sub-index loaded.
func TestSaveSmallTailAsCdc_EmitsHeaderRecord(t *testing.T) {
	const dim = 2
	const ibpr = 8
	colMetaJSON := `[{"name":"a","type":1}]`
	rows := []PendingRecord{
		{Pkid: 1, Vec: []float32{0.1, 0.2}, Include: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
	}
	sqls, err := SaveSmallTailAsCdc(smallTailTblcfg(), rows, dim, ibpr, colMetaJSON)
	require.NoError(t, err)
	require.NotEmpty(t, sqls)

	// Parse the first emitted chunk back, walk its records: the first
	// MUST be CdcOpHeader carrying colMetaJSON; the second is the
	// INSERT record.
	re := regexp.MustCompile(`unhex\('([0-9a-fA-F]*)'\)`)
	m := re.FindStringSubmatch(sqls[0])
	require.NotNil(t, m, "first chunk must have a unhex payload")
	framed, err := hex.DecodeString(m[1])
	require.NoError(t, err)
	records, err := UnframeCdcChunk(framed)
	require.NoError(t, err)

	rec, n, ok := DecodeEventRecord(records, dim, ibpr)
	require.True(t, ok)
	require.Equal(t, CdcOpHeader, rec.Op)
	require.Equal(t, colMetaJSON, string(rec.Header))

	rec, _, ok = DecodeEventRecord(records[n:], dim, ibpr)
	require.True(t, ok)
	require.Equal(t, CdcOpInsert, rec.Op)
	require.Equal(t, int64(1), rec.Pkid)
}

// TestSaveSmallTailAsCdc_HeaderOnlyNoRows: a CREATE INDEX on an
// empty source can still emit just the header so future CDC
// iterations write events under a known layout.
func TestSaveSmallTailAsCdc_HeaderOnlyNoRows(t *testing.T) {
	colMetaJSON := `[{"name":"a","type":1}]`
	sqls, err := SaveSmallTailAsCdc(smallTailTblcfg(), nil, 4, 8, colMetaJSON)
	require.NoError(t, err)
	require.Len(t, sqls, 1)

	re := regexp.MustCompile(`unhex\('([0-9a-fA-F]*)'\)`)
	m := re.FindStringSubmatch(sqls[0])
	require.NotNil(t, m)
	framed, err := hex.DecodeString(m[1])
	require.NoError(t, err)
	records, err := UnframeCdcChunk(framed)
	require.NoError(t, err)
	rec, _, ok := DecodeEventRecord(records, 4, 8)
	require.True(t, ok)
	require.Equal(t, CdcOpHeader, rec.Op)
	require.Equal(t, colMetaJSON, string(rec.Header))
}

// TestPeekColMetaJSON_RoundTrip: SaveSmallTailAsCdc → PeekColMetaJSON
// recovers the colMetaJSON without needing dim or ibpr (the header
// record is self-describing).
func TestPeekColMetaJSON_RoundTrip(t *testing.T) {
	colMetaJSON := `[{"name":"a","type":1},{"name":"b","type":2}]`
	sqls, err := SaveSmallTailAsCdc(smallTailTblcfg(), nil, 4, 0, colMetaJSON)
	require.NoError(t, err)
	require.Len(t, sqls, 1)

	re := regexp.MustCompile(`unhex\('([0-9a-fA-F]*)'\)`)
	m := re.FindStringSubmatch(sqls[0])
	require.NotNil(t, m)
	framed, err := hex.DecodeString(m[1])
	require.NoError(t, err)

	got, err := PeekColMetaJSON([]EventChunk{{ChunkId: 0, Data: framed}})
	require.NoError(t, err)
	require.Equal(t, colMetaJSON, got)
}

// TestPeekColMetaJSON_NoHeader: when no header was emitted (empty
// colMetaJSON) PeekColMetaJSON returns "" without error.
func TestPeekColMetaJSON_NoHeader(t *testing.T) {
	rows := []PendingRecord{{Pkid: 1, Vec: []float32{1, 2, 3, 4}}}
	sqls, err := SaveSmallTailAsCdc(smallTailTblcfg(), rows, 4, 0, "")
	require.NoError(t, err)

	re := regexp.MustCompile(`unhex\('([0-9a-fA-F]*)'\)`)
	m := re.FindStringSubmatch(sqls[0])
	require.NotNil(t, m)
	framed, err := hex.DecodeString(m[1])
	require.NoError(t, err)

	got, err := PeekColMetaJSON([]EventChunk{{ChunkId: 0, Data: framed}})
	require.NoError(t, err)
	require.Equal(t, "", got)
}
