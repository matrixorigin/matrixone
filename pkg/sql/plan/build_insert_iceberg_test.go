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
	"context"
	"strings"
	"testing"

	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBuildInsertBuildsIcebergAppendIntentBeforeExternalReadOnlyCheck(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "insert into gold_orders values (1)", 1)
	require.NoError(t, err)

	p, err := buildInsert(stmt.(*tree.Insert), newIcebergTestCompilerContext(t, nil), false, false)
	require.NoError(t, err)

	var insertNode *pbplan.Node
	for _, node := range p.GetQuery().GetNodes() {
		if node.GetNodeType() == pbplan.Node_INSERT {
			insertNode = node
			break
		}
	}
	require.NotNil(t, insertNode, "expected Iceberg append insert intent")
	require.NotNil(t, insertNode.GetInsertCtx())
	require.NotNil(t, insertNode.GetInsertCtx().GetTableDef())
	isIceberg, err := IsIcebergTableDef(context.Background(), insertNode.GetInsertCtx().GetTableDef())
	require.NoError(t, err)
	require.True(t, isIceberg)
}

func TestBuildInsertOverwriteBuildsIcebergOverwriteIntent(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "insert overwrite gold_orders select 1", 1)
	require.NoError(t, err)

	p, err := buildInsert(stmt.(*tree.Insert), newIcebergTestCompilerContext(t, nil), false, false)
	require.NoError(t, err)

	var insertNode *pbplan.Node
	for _, node := range p.GetQuery().GetNodes() {
		if node.GetNodeType() == pbplan.Node_INSERT {
			insertNode = node
			break
		}
	}
	require.NotNil(t, insertNode, "expected Iceberg overwrite insert intent")
	require.Equal(t, icebergapi.DMLOverwritePlanExtraOptions, insertNode.GetExtraOptions())
	require.NotNil(t, insertNode.GetInsertCtx())
}

func TestBuildInsertOverwritePartitionBuildsIcebergOverwriteIntent(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "insert overwrite gold_orders partition(region = 'ksa', day = 20260624) select 1", 1)
	require.NoError(t, err)

	p, err := buildInsert(stmt.(*tree.Insert), newIcebergTestCompilerContext(t, nil), false, false)
	require.NoError(t, err)

	var insertNode *pbplan.Node
	for _, node := range p.GetQuery().GetNodes() {
		if node.GetNodeType() == pbplan.Node_INSERT {
			insertNode = node
			break
		}
	}
	require.NotNil(t, insertNode, "expected Iceberg partition overwrite insert intent")
	opts, err := icebergapi.DecodeDMLPlanExtraOptions(insertNode.GetExtraOptions())
	require.NoError(t, err)
	require.Equal(t, icebergapi.DMLOverwritePlanExtraOptions, opts.Kind)
	require.Equal(t, "partition", opts.OverwriteScope)
	require.Equal(t, "ksa", opts.OverwritePartition["region"])
	require.Equal(t, int64(20260624), opts.OverwritePartition["day"])
	require.NotNil(t, insertNode.GetInsertCtx())
}

func TestBuildInsertOverwriteRejectsOrdinaryTable(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "insert overwrite dim_orders select 1", 1)
	require.NoError(t, err)

	_, err = buildInsert(stmt.(*tree.Insert), newIcebergTestCompilerContext(t, nil), false, false)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "INSERT OVERWRITE currently supports Iceberg table mappings"), err.Error())
}

func TestBuildInsertRejectsPartitionValueSyntaxOutsideIcebergOverwrite(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "insert into gold_orders partition(region = 'ksa') select 1", 1)
	require.NoError(t, err)

	_, err = buildInsert(stmt.(*tree.Insert), newIcebergTestCompilerContext(t, nil), false, false)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "INSERT PARTITION value syntax currently supports Iceberg INSERT OVERWRITE only"), err.Error())
}

func TestBuildInsertOverwriteRejectsPartitionScope(t *testing.T) {
	stmt, err := mysql.ParseOne(context.Background(), "insert overwrite gold_orders partition(p0) select 1", 1)
	require.NoError(t, err)

	_, err = buildInsert(stmt.(*tree.Insert), newIcebergTestCompilerContext(t, nil), false, false)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "PARTITION name syntax cannot express an Iceberg partition tuple"), err.Error())
}
