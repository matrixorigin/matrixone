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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestQueryHasPartitionedIndexSearchScan(t *testing.T) {
	scan := func(algo string) *plan.Query {
		return &plan.Query{Nodes: []*plan.Node{
			{NodeType: plan.Node_TABLE_SCAN},
			{NodeType: plan.Node_INDEX_SEARCH_SCAN, IndexSearchScan: &plan.IndexSearchScan{
				Index: &plan.IndexDef{IndexAlgo: algo},
			}},
		}}
	}
	require.False(t, queryHasPartitionedIndexSearchScan(nil))
	require.False(t, queryHasPartitionedIndexSearchScan(&plan.Query{Nodes: []*plan.Node{{NodeType: plan.Node_TABLE_SCAN}}}))
	require.True(t, queryHasPartitionedIndexSearchScan(scan(catalog.MoIndexIvfFlatAlgo.ToString())))
	require.False(t, queryHasPartitionedIndexSearchScan(scan(catalog.MoIndexHnswAlgo.ToString())))
	require.False(t, queryHasPartitionedIndexSearchScan(scan(catalog.MOIndexFullTextAlgo.ToString())))
}
