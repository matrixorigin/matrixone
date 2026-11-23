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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func ignoredDef(maxBadRecords string) *plan.IndexDef {
	return &plan.IndexDef{IndexName: "idx", IndexTableName: "__mo_ign", Parts: []string{"body"},
		IndexAlgoTableType: catalog.IndexIgnored_TblType,
		IndexAlgoParams:    `{"parser":"json","max_bad_records":"` + maxBadRecords + `"}`}
}

// The build is given the ignored table and its limit; a REBUILD starts the ignored rows over.
func TestFulltext2BuildRecordsIgnoredRows(t *testing.T) {
	defs := indexDefs(`{"parser":"json"}`)
	defs[catalog.IndexIgnored_TblType] = ignoredDef("4")
	ctx := newStubCtx()
	require.NoError(t, Hooks{}.HandleReindex(ctx, defs, false, false))
	joined := strings.Join(ctx.sqls, ";")
	require.Contains(t, joined, "DELETE FROM `db`.`__mo_ign` WHERE TRUE")
	require.Contains(t, joined, `"ignored":"__mo_ign"`)
	require.Contains(t, joined, `"max_bad_records":4`)

	sql, err := genFulltext2BuildFromSourceSQL(srcTableDef(), defs[catalog.FullText2Index_TblType_Storage],
		defs[catalog.FullText2Index_TblType_Metadata], "db", DefaultMaxIndexCapacity, DefaultMaxPostingsCapacity, 0, nil)
	require.NoError(t, err)
	require.NotContains(t, sql, `"ignored"`, "an index without an ignored table builds as before")

	_, err = genFulltext2BuildFromSourceSQL(srcTableDef(), defs[catalog.FullText2Index_TblType_Storage],
		defs[catalog.FullText2Index_TblType_Metadata], "db", DefaultMaxIndexCapacity, DefaultMaxPostingsCapacity, 0, ignoredDef("x"))
	require.Error(t, err)
}

func TestCountTextRows(t *testing.T) {
	ctx := newStubCtx()
	n, err := countTextRows(ctx, &plan.IndexDef{Parts: []string{"a", "b"}}, srcTableDef(), "db")
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
}

func TestValidateReindexParams_MaxBadRecords(t *testing.T) {
	old := map[string]string{"parser": "ngram"}
	merged, err := Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{Params: map[string]string{catalog.IndexAlgoParamMaxBadRecords: "7"}})
	require.NoError(t, err)
	require.Equal(t, "7", merged[catalog.IndexAlgoParamMaxBadRecords])
	require.Equal(t, "ngram", merged["parser"])
	require.Empty(t, old[catalog.IndexAlgoParamMaxBadRecords], "the old params are not changed")
}
