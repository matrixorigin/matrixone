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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/stretchr/testify/require"
)

// TestValidateReindexParams covers the fulltext2 ALTER REINDEX param merge — in
// particular that max_postings_capacity is APPLIED (not the silent no-op it was
// before), so `ALTER … REINDEX … max_postings_capacity=N` actually re-bounds the
// rebuild.
func TestValidateReindexParams(t *testing.T) {
	up := func(m map[string]string) compileplugin.ReindexParamUpdate {
		return compileplugin.ReindexParamUpdate{Params: m}
	}

	// No honored key → old returned unchanged (a bare reindex / MERGE).
	old := map[string]string{"parser": "ngram", catalog.IndexAlgoParamMaxIndexCapacity: "1000000"}
	got, err := Hooks{}.ValidateReindexParams(old, up(nil))
	require.NoError(t, err)
	require.Equal(t, old, got)

	// max_postings_capacity present → merged into the rebuild's algo_params.
	got, err = Hooks{}.ValidateReindexParams(old, up(map[string]string{
		catalog.IndexAlgoParamMaxPostingsCapacity: "4000000",
	}))
	require.NoError(t, err)
	require.Equal(t, "4000000", got[catalog.IndexAlgoParamMaxPostingsCapacity])
	require.Equal(t, "ngram", got["parser"])                                 // untouched
	require.Equal(t, "1000000", got[catalog.IndexAlgoParamMaxIndexCapacity]) // untouched
	require.NotContains(t, old, catalog.IndexAlgoParamMaxPostingsCapacity)   // old not mutated

	// position_free + max_postings_capacity together (gojieba): both applied.
	oldG := map[string]string{"parser": "gojieba"}
	got, err = Hooks{}.ValidateReindexParams(oldG, up(map[string]string{
		catalog.IndexAlgoParamPositionFree:        "true",
		catalog.IndexAlgoParamMaxPostingsCapacity: "2000000",
	}))
	require.NoError(t, err)
	require.Equal(t, "true", got[catalog.IndexAlgoParamPositionFree])
	require.Equal(t, "2000000", got[catalog.IndexAlgoParamMaxPostingsCapacity])

	// position_free=true on a non-gojieba parser is still rejected, even when a
	// posting-capacity override rides along.
	_, err = Hooks{}.ValidateReindexParams(old, up(map[string]string{
		catalog.IndexAlgoParamPositionFree:        "true",
		catalog.IndexAlgoParamMaxPostingsCapacity: "2000000",
	}))
	require.Error(t, err)

	// max_index_capacity alone is NOT honored on a reindex (pre-existing fulltext2
	// limitation): old returned unchanged.
	got, err = Hooks{}.ValidateReindexParams(old, up(map[string]string{
		catalog.IndexAlgoParamMaxIndexCapacity: "5",
	}))
	require.NoError(t, err)
	require.Equal(t, old, got)
}

// TestValidateReindexParamsMergePositionFree covers the data-loss guard: a MERGE compacts the tail
// into the EXISTING base in place and cannot change the positional format (turning POSITION_FREE
// OFF has no positions to re-derive; CompactSegments would commit an empty base). So a
// `MERGE POSITION_FREE=…` that flips the format is rejected BEFORE the params are persisted, while
// a REBUILD (Merge=false) with the same change is allowed and a format-preserving MERGE passes.
func TestValidateReindexParamsMergePositionFree(t *testing.T) {
	upM := func(m map[string]string, merge bool) compileplugin.ReindexParamUpdate {
		return compileplugin.ReindexParamUpdate{Params: m, Merge: merge}
	}
	posFree := map[string]string{"parser": "gojieba", catalog.IndexAlgoParamPositionFree: "true"}
	positional := map[string]string{"parser": "gojieba"} // position_free absent ⇒ positional

	// MERGE turning position_free OFF (the reported data-loss case) → rejected.
	_, err := Hooks{}.ValidateReindexParams(posFree, upM(map[string]string{
		catalog.IndexAlgoParamPositionFree: "false"}, true))
	require.ErrorContains(t, err, "REBUILD")

	// MERGE turning position_free ON (also a format change) → rejected.
	_, err = Hooks{}.ValidateReindexParams(positional, upM(map[string]string{
		catalog.IndexAlgoParamPositionFree: "true"}, true))
	require.ErrorContains(t, err, "REBUILD")

	// REBUILD (Merge=false) with the same OFF change → allowed (re-derives from source).
	got, err := Hooks{}.ValidateReindexParams(posFree, upM(map[string]string{
		catalog.IndexAlgoParamPositionFree: "false"}, false))
	require.NoError(t, err)
	require.NotContains(t, got, catalog.IndexAlgoParamPositionFree) // false ⇒ positional (absent)

	// MERGE that does NOT change the format (re-asserts the current true) → allowed.
	got, err = Hooks{}.ValidateReindexParams(posFree, upM(map[string]string{
		catalog.IndexAlgoParamPositionFree: "true"}, true))
	require.NoError(t, err)
	require.Equal(t, "true", got[catalog.IndexAlgoParamPositionFree])

	// bare MERGE (no position_free param) → allowed.
	got, err = Hooks{}.ValidateReindexParams(posFree, upM(nil, true))
	require.NoError(t, err)
	require.Equal(t, posFree, got)
}
