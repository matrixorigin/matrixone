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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// TestReindexSpecifiedParams verifies that the build options the user wrote on
// ALTER TABLE ... ALTER REINDEX are extracted off the parse tree and keyed by
// the catalog IndexAlgoParam* name, that unset (zero/empty) fields are skipped,
// and that force_sync (a plan-node flag, not an algo param) is not included.
func TestReindexSpecifiedParams(t *testing.T) {
	ro := &tree.AlterOptionAlterReIndex{
		Name:                    tree.Identifier("idx1"),
		AlgoParamList:           8,
		AlgoParamVectorOpType:   "vector_l2_ops",
		AlgoParamM:              16,
		HnswEfConstruction:      200,
		HnswEfSearch:            64,
		BitsPerCode:             8,
		IntermediateGraphDegree: 128,
		GraphDegree:             64,
		ITopkSize:               256,
		Quantization:            "Float16", // mixed case -> normalized to lowercase
		KmeansTrainPercent:      5,
		KmeansMaxIteration:      30,
		MaxIndexCapacity:        2000,
		ForceSync:               true, // must NOT appear in the param map
	}
	at := &tree.AlterTable{Options: tree.AlterTableOptions{ro}}

	got := reindexSpecifiedParams(at, "idx1")
	want := map[string]string{
		catalog.IndexAlgoParamLists:              "8",
		catalog.IndexAlgoParamOpType:             "vector_l2_ops",
		catalog.HnswM:                            "16",
		catalog.HnswEfConstruction:               "200",
		catalog.HnswEfSearch:                     "64",
		catalog.BitsPerCode:                      "8",
		catalog.IntermediateGraphDegree:          "128",
		catalog.GraphDegree:                      "64",
		catalog.ITopkSize:                        "256",
		catalog.Quantization:                     "float16", // normalized from "Float16"
		catalog.IndexAlgoParamKmeansTrainPercent: "5",
		catalog.IndexAlgoParamKmeansMaxIteration: "30",
		catalog.IndexAlgoParamMaxIndexCapacity:   "2000",
	}
	require.Equal(t, want, got)
	require.NotContains(t, got, "force_sync")
}

// TestReindexSpecifiedParams_SkipsUnset: only options the user actually
// specified are emitted; the rest stay absent so ValidateReindexParams does
// not overwrite existing algo params with zero values.
func TestReindexSpecifiedParams_SkipsUnset(t *testing.T) {
	ro := &tree.AlterOptionAlterReIndex{
		Name:          tree.Identifier("idx1"),
		AlgoParamList: 4,
	}
	at := &tree.AlterTable{Options: tree.AlterTableOptions{ro}}

	got := reindexSpecifiedParams(at, "idx1")
	require.Equal(t, map[string]string{catalog.IndexAlgoParamLists: "4"}, got)
}

// TestReindexSpecifiedParams_NoMatch covers the defensive paths: a statement
// that is not an ALTER TABLE, and an ALTER TABLE whose REINDEX option targets a
// different index name, both yield nil.
func TestReindexSpecifiedParams_NoMatch(t *testing.T) {
	require.Nil(t, reindexSpecifiedParams(&tree.Select{}, "idx1"))

	ro := &tree.AlterOptionAlterReIndex{Name: tree.Identifier("other"), AlgoParamList: 4}
	at := &tree.AlterTable{Options: tree.AlterTableOptions{ro}}
	require.Nil(t, reindexSpecifiedParams(at, "idx1"))
}
