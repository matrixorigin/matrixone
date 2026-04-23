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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func makeVectorIndexDefForLogicalTest(name, algo, algoTableType string, parts, included []string) *planpb.IndexDef {
	return &planpb.IndexDef{
		IndexName:          name,
		IndexAlgo:          algo,
		IndexAlgoTableType: algoTableType,
		IndexAlgoParams:    `{"op_type":"vector_l2_ops"}`,
		Parts:              append([]string(nil), parts...),
		IncludedColumns:    append([]string(nil), included...),
		Comment:            "logical-comment",
		Visible:            true,
	}
}

func TestCollectVectorIndexesConstructsLogicalDef(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	metaDef := makeVectorIndexDefForLogicalTest("idx_vec", catalog.MoIndexIvfFlatAlgo.ToString(), catalog.SystemSI_IVFFLAT_TblType_Metadata, []string{"embedding"}, []string{"title", "category"})
	centroidsDef := makeVectorIndexDefForLogicalTest("idx_vec", catalog.MoIndexIvfFlatAlgo.ToString(), catalog.SystemSI_IVFFLAT_TblType_Centroids, []string{"embedding"}, []string{"title", "category"})
	entriesDef := makeVectorIndexDefForLogicalTest("idx_vec", catalog.MoIndexIvfFlatAlgo.ToString(), catalog.SystemSI_IVFFLAT_TblType_Entries, []string{"embedding"}, []string{"title", "category"})

	indexes := builder.collectVectorIndexes(&planpb.Node{
		TableDef: &planpb.TableDef{
			Indexes: []*planpb.IndexDef{metaDef, centroidsDef, entriesDef},
		},
	})

	multi, ok := indexes["idx_vec"]
	require.True(t, ok)
	require.NotNil(t, multi.LogicalDef)
	require.NotSame(t, metaDef, multi.LogicalDef)
	require.NotSame(t, centroidsDef, multi.LogicalDef)
	require.NotSame(t, entriesDef, multi.LogicalDef)
	require.Equal(t, catalog.MoIndexIvfFlatAlgo.ToString(), getVectorIndexLogicalAlgo(multi))
	require.Equal(t, []string{"embedding"}, getVectorIndexLogicalParts(multi))
	require.Equal(t, []string{"title", "category"}, getVectorIndexIncludedColumns(multi))

	entriesDef.IncludedColumns[0] = "mutated"
	require.Equal(t, []string{"title", "category"}, multi.LogicalDef.IncludedColumns)

	included := getVectorIndexIncludedColumns(multi)
	included[0] = "mutated-again"
	require.Equal(t, []string{"title", "category"}, multi.LogicalDef.IncludedColumns)
}

func TestCollectVectorIndexesRejectsInconsistentLogicalDef(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	metaDef := makeVectorIndexDefForLogicalTest("idx_vec", catalog.MoIndexIvfFlatAlgo.ToString(), catalog.SystemSI_IVFFLAT_TblType_Metadata, []string{"embedding"}, []string{"title", "category"})
	centroidsDef := makeVectorIndexDefForLogicalTest("idx_vec", catalog.MoIndexIvfFlatAlgo.ToString(), catalog.SystemSI_IVFFLAT_TblType_Centroids, []string{"embedding"}, []string{"title", "category"})
	entriesDef := makeVectorIndexDefForLogicalTest("idx_vec", catalog.MoIndexIvfFlatAlgo.ToString(), catalog.SystemSI_IVFFLAT_TblType_Entries, []string{"embedding"}, []string{"title"})

	indexes := builder.collectVectorIndexes(&planpb.Node{
		TableDef: &planpb.TableDef{
			Indexes: []*planpb.IndexDef{metaDef, centroidsDef, entriesDef},
		},
	})

	require.NotContains(t, indexes, "idx_vec")
}

func TestVectorIndexLogicalHelpersConstructFromExistingGroup(t *testing.T) {
	multi := &MultiTableIndex{
		IndexDefs: map[string]*planpb.IndexDef{
			catalog.Hnsw_TblType_Metadata: makeVectorIndexDefForLogicalTest("idx_hnsw", catalog.MoIndexHnswAlgo.ToString(), catalog.Hnsw_TblType_Metadata, []string{"embedding"}, []string{"title"}),
			catalog.Hnsw_TblType_Storage:  makeVectorIndexDefForLogicalTest("idx_hnsw", catalog.MoIndexHnswAlgo.ToString(), catalog.Hnsw_TblType_Storage, []string{"embedding"}, []string{"title"}),
		},
	}

	require.Equal(t, catalog.MoIndexHnswAlgo.ToString(), getVectorIndexLogicalAlgo(multi))
	require.Equal(t, []string{"embedding"}, getVectorIndexLogicalParts(multi))
	require.Equal(t, []string{"title"}, getVectorIndexIncludedColumns(multi))
	require.NotNil(t, multi.LogicalDef)
}

func TestVectorIndexLogicalHelpersDoNotFallBackToPhysicalDefs(t *testing.T) {
	multi := &MultiTableIndex{
		IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(),
		IndexDefs: map[string]*planpb.IndexDef{
			catalog.SystemSI_IVFFLAT_TblType_Metadata:  makeVectorIndexDefForLogicalTest("idx_vec", catalog.MoIndexIvfFlatAlgo.ToString(), catalog.SystemSI_IVFFLAT_TblType_Metadata, []string{"embedding"}, []string{"title"}),
			catalog.SystemSI_IVFFLAT_TblType_Centroids: makeVectorIndexDefForLogicalTest("idx_vec", catalog.MoIndexIvfFlatAlgo.ToString(), catalog.SystemSI_IVFFLAT_TblType_Centroids, []string{"embedding"}, []string{"title"}),
			catalog.SystemSI_IVFFLAT_TblType_Entries:   makeVectorIndexDefForLogicalTest("idx_vec", catalog.MoIndexIvfFlatAlgo.ToString(), catalog.SystemSI_IVFFLAT_TblType_Entries, []string{"embedding"}, []string{"category"}),
		},
	}

	require.Nil(t, ensureVectorIndexLogicalDef(multi))
	require.Nil(t, getVectorIndexLogicalParts(multi))
	require.Nil(t, getVectorIndexIncludedColumns(multi))
	require.Equal(t, catalog.MoIndexIvfFlatAlgo.ToString(), getVectorIndexLogicalAlgo(multi))
}
