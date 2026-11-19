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
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestValidateIndexMaintenance(t *testing.T) {
	spec := &executor.IndexMaintenanceSpec{
		Function: "fulltext2_create", Database: "db", DatabaseID: 1, BaseName: "source", BaseID: 2,
		Targets: []executor.IndexMaintenanceTarget{
			{Role: catalog.FullText2Index_TblType_Storage, Name: "store", ID: 3},
			{Role: catalog.FullText2Index_TblType_Metadata, Name: "meta", ID: 4},
		},
	}
	cfg := fulltext2.TableConfig{DbName: "db", SrcTable: "source", IndexTable: "store", MetadataTable: "meta"}
	encoded, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, (Hooks{}).ValidateIndexMaintenance("fulltext2_create", []string{"{}", string(encoded)}, spec))
	cfg.IndexTable = "other"
	encoded, err = json.Marshal(cfg)
	require.NoError(t, err)
	require.Error(t, (Hooks{}).ValidateIndexMaintenance("fulltext2_create", []string{"{}", string(encoded)}, spec))
	spec.Function = "fulltext2_compact"
	require.NoError(t, (Hooks{}).ValidateIndexMaintenance("fulltext2_compact", []string{"db", "store", "meta"}, spec))
	require.Error(t, (Hooks{}).ValidateIndexMaintenance("fulltext2_compact", []string{"db", "meta", "store"}, spec))
	defs := map[string]*plan.IndexDef{catalog.FullText2Index_TblType_Storage: {}, catalog.FullText2Index_TblType_Metadata: {}}
	for _, function := range []string{"fulltext2_create", "fulltext2_compact"} {
		roles, err := (Hooks{}).IndexMaintenanceRoles(function, defs)
		require.NoError(t, err)
		require.Equal(t, []string{catalog.FullText2Index_TblType_Storage, catalog.FullText2Index_TblType_Metadata}, roles)
		_, err = (Hooks{}).IndexMaintenanceRoles(function, nil)
		require.Error(t, err)
	}

	// fulltext2_create writes the ignored table of an index that has one; compaction does not.
	defs[catalog.IndexIgnored_TblType] = &plan.IndexDef{}
	roles, err := (Hooks{}).IndexMaintenanceRoles("fulltext2_create", defs)
	require.NoError(t, err)
	require.Equal(t, []string{catalog.FullText2Index_TblType_Storage, catalog.FullText2Index_TblType_Metadata, catalog.IndexIgnored_TblType}, roles)
	roles, err = (Hooks{}).IndexMaintenanceRoles("fulltext2_compact", defs)
	require.NoError(t, err)
	require.Equal(t, []string{catalog.FullText2Index_TblType_Storage, catalog.FullText2Index_TblType_Metadata}, roles)

	spec.Function = "fulltext2_create"
	cfg.IndexTable, cfg.IgnoredTable = "store", "ignored"
	encoded, err = json.Marshal(cfg)
	require.NoError(t, err)
	require.Error(t, (Hooks{}).ValidateIndexMaintenance("fulltext2_create", []string{"{}", string(encoded)}, spec),
		"a config naming an ignored table needs it among the targets")
	spec.Targets = append(spec.Targets, executor.IndexMaintenanceTarget{Role: catalog.IndexIgnored_TblType, Name: "ignored", ID: 5})
	require.NoError(t, (Hooks{}).ValidateIndexMaintenance("fulltext2_create", []string{"{}", string(encoded)}, spec))
	cfg.IgnoredTable = "other"
	encoded, err = json.Marshal(cfg)
	require.NoError(t, err)
	require.Error(t, (Hooks{}).ValidateIndexMaintenance("fulltext2_create", []string{"{}", string(encoded)}, spec))
}

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

	// REBUILD turning position_free OFF on a token_filter index → rejected: it is
	// bag-of-words only, and a positional rebuild would leave NL/boolean queries unable
	// to match the stripped stopwords (this closes the CREATE-guard bypass via REINDEX).
	tokFilter := map[string]string{
		"parser":                           "gojieba",
		catalog.IndexAlgoParamPositionFree: "true",
		catalog.IndexAlgoParamTokenFilter:  "stop",
	}
	_, err = Hooks{}.ValidateReindexParams(tokFilter, upM(map[string]string{
		catalog.IndexAlgoParamPositionFree: "false"}, false))
	require.ErrorContains(t, err, "token_filter")

	// REBUILD re-asserting position_free TRUE on a token_filter index → allowed.
	got, err = Hooks{}.ValidateReindexParams(tokFilter, upM(map[string]string{
		catalog.IndexAlgoParamPositionFree: "true"}, false))
	require.NoError(t, err)
	require.Equal(t, "stop", got[catalog.IndexAlgoParamTokenFilter])
	require.Equal(t, "true", got[catalog.IndexAlgoParamPositionFree])
}
