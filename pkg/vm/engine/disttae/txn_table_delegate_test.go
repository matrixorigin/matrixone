// Copyright 2021-2024 Matrix Origin
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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTxnTableDelegateIsPartitionIndexTable(t *testing.T) {
	parent := &txnTableDelegate{origin: &txnTable{
		extraInfo: &api.SchemaExtra{FeatureFlag: features.Partitioned},
	}}

	tests := []struct {
		name      string
		tableName string
		tableType string
		expected  bool
	}{
		{
			name:      "regular partition index",
			tableName: catalog.IndexTableNamePrefix + "regular",
			tableType: catalog.SystemIndexRel,
			expected:  true,
		},
		{
			name:      "fulltext table type uses global table",
			tableName: catalog.IndexTableNamePrefix + "fulltext",
			tableType: catalog.FullTextIndex_TblType,
		},
		{
			name:      "legacy fulltext name uses global table",
			tableName: catalog.FullTextIndexTableNamePrefix + "legacy",
			tableType: catalog.SystemIndexRel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := &txnTableDelegate{
				origin: &txnTable{
					tableName: tt.tableName,
					relKind:   tt.tableType,
				},
				parent: parent,
			}
			require.Equal(t, tt.expected, table.IsPartitionIndexTable())
		})
	}
}

func TestUseOrdinaryTableForLegacyPartition(t *testing.T) {
	sharedDef := &plan.TableDef{
		FeatureFlag: features.Partitioned | features.IndexTable,
		Partition:   &plan.Partition{},
	}
	sharedExtra := &api.SchemaExtra{
		FeatureFlag: features.Partitioned | features.IndexTable,
	}
	table := &txnTableDelegate{origin: &txnTable{
		tableDef:    sharedDef,
		extraInfo:   sharedExtra,
		partitioned: 1,
		partition:   "partition by range (id)",
	}}

	table.useOrdinaryTableForLegacyPartition()

	require.NotSame(t, sharedDef, table.origin.tableDef)
	require.NotSame(t, sharedExtra, table.origin.extraInfo)
	require.False(t, features.IsPartitioned(table.origin.tableDef.FeatureFlag))
	require.False(t, features.IsPartitioned(table.origin.extraInfo.FeatureFlag))
	require.True(t, features.IsIndexTable(table.origin.tableDef.FeatureFlag))
	require.True(t, features.IsIndexTable(table.origin.extraInfo.FeatureFlag))
	require.Nil(t, table.origin.tableDef.Partition)
	require.Zero(t, table.origin.partitioned)
	require.Empty(t, table.origin.partition)

	// The relation-local downgrade must not mutate the shared catalog entry.
	require.True(t, features.IsPartitioned(sharedDef.FeatureFlag))
	require.True(t, features.IsPartitioned(sharedExtra.FeatureFlag))
	require.NotNil(t, sharedDef.Partition)
}

func TestConfigurePartitionTableMetadataBoundary(t *testing.T) {
	newTable := func() *txnTableDelegate {
		return &txnTableDelegate{origin: &txnTable{
			tableDef: &plan.TableDef{
				FeatureFlag: features.Partitioned,
				Partition:   &plan.Partition{},
			},
			extraInfo:   &api.SchemaExtra{FeatureFlag: features.Partitioned},
			partitioned: 1,
			partition:   "partition by range (id)",
		}}
	}

	t.Run("missing metadata is a legacy ordinary table", func(t *testing.T) {
		table := newTable()
		combined, err := table.configurePartitionTable(
			context.Background(),
			partition.PartitionMetadata{},
			nil,
			nil,
		)
		require.NoError(t, err)
		require.Nil(t, combined)
		require.False(t, table.combined.is)
		require.Nil(t, table.combined.tbl)
		require.False(t, features.IsPartitioned(table.origin.tableDef.FeatureFlag))
	})

	t.Run("published empty metadata is corruption", func(t *testing.T) {
		table := newTable()
		combined, err := table.configurePartitionTable(
			context.Background(),
			partition.PartitionMetadata{TableID: 42},
			nil,
			nil,
		)
		require.Nil(t, combined)
		require.ErrorContains(t, err, "partition metadata for table 42 has no partitions")
		require.False(t, table.combined.is)
		require.True(t, features.IsPartitioned(table.origin.tableDef.FeatureFlag))
	})

	t.Run("published metadata keeps physical partition routing", func(t *testing.T) {
		table := newTable()
		combined, err := table.configurePartitionTable(
			context.Background(),
			partition.PartitionMetadata{
				TableID:    42,
				Partitions: []partition.Partition{{PartitionID: 43}},
			},
			nil,
			nil,
		)
		require.NoError(t, err)
		require.NotNil(t, combined)
		require.Same(t, table.origin, combined.primary)
		require.True(t, features.IsPartitioned(table.origin.tableDef.FeatureFlag))
	})
}

func TestTxnTableDelegate_CollectChanges(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.CollectChanges(
			context.Background(),
			types.TS{},
			types.TS{},
			false,
			&mpool.MPool{},
		)
	})
}

func TestTxnTableDelegate_MergeObjects(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.MergeObjects(
			context.Background(),
			[]objectio.ObjectStats{},
			1024,
		)
	})
}

func TestTxnTableDelegate_UpdateConstraint(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.UpdateConstraint(
			context.Background(),
			&engine.ConstraintDef{},
		)
	})
}

func TestTxnTableDelegate_TableRenameInTxn(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.TableRenameInTxn(
			context.Background(),
			[][]byte{},
		)
	})
}

func TestTxnTableDelegate_MaxAndMinValues(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.MaxAndMinValues(context.Background())
	})
}

func TestTxnTableDelegate_Write(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "BUG: cannot write data to partition primary table", func() {
		table.Write(context.Background(), &batch.Batch{})
	})
}

func TestTxnTableDelegate_Delete(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "BUG: cannot delete data to partition primary table", func() {
		table.Delete(context.Background(), &batch.Batch{}, "")
	})
}
