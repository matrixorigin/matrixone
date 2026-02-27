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
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/assert"
)

func newMockCombinedTxnTable() *combinedTxnTable {
	return &combinedTxnTable{
		primary:     nil, // Will be set in individual tests
		pruneFunc:   func(ctx context.Context, param engine.RangesParam) ([]engine.Relation, error) { return nil, nil },
		tablesFunc:  func() ([]engine.Relation, error) { return nil, nil },
		prunePKFunc: func(bat *batch.Batch, partitionIndex int32) ([]engine.Relation, error) { return nil, nil },
	}
}

func TestCombinedTxnTable_BuildShardingReaders(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "Not Support", func() {
		table.BuildShardingReaders(
			context.Background(),
			nil,
			nil,
			nil,
			0,
			0,
			false,
			engine.Policy_SkipCommittedS3,
		)
	})
}

func TestCombinedTxnTable_CollectChanges(t *testing.T) {
	table := newMockCombinedTxnTable()

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

func TestCombinedTxnTable_MergeObjects(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.MergeObjects(
			context.Background(),
			[]objectio.ObjectStats{},
			1024,
		)
	})
}

func TestCombinedTxnTable_UpdateConstraint(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.UpdateConstraint(
			context.Background(),
			&engine.ConstraintDef{},
		)
	})
}

func TestCombinedTxnTable_TableRenameInTxn(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.TableRenameInTxn(
			context.Background(),
			[][]byte{},
		)
	})
}

func TestCombinedTxnTable_MaxAndMinValues(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.MaxAndMinValues(context.Background())
	})
}

func TestCombinedTxnTable_Write(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "BUG: cannot write data to partition primary table", func() {
		table.Write(context.Background(), &batch.Batch{})
	})
}

func TestCombinedTxnTable_Delete(t *testing.T) {
	table := newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "BUG: cannot delete data to partition primary table", func() {
		table.Delete(context.Background(), &batch.Batch{}, "")
	})
}

func TestCombinedTxnTable_BuildReaders(t *testing.T) {
	ctx := context.Background()

	t.Run("RelDataNilSuccess", func(t *testing.T) {
		reader1 := &mockReader{}
		reader2 := &mockReader{}
		rel1Called := false
		rel2Called := false

		mockRel1 := &mockRelation{
			buildReadersFunc: func(ctx context.Context, proc any, expr *plan.Expr, relData engine.RelData, num int, txnOffset int, orderBy bool, policy engine.TombstoneApplyPolicy, filterHint engine.FilterHint) ([]engine.Reader, error) {
				rel1Called = true
				assert.Nil(t, relData)
				assert.Equal(t, 1, num)
				return []engine.Reader{reader1}, nil
			},
		}
		mockRel2 := &mockRelation{
			buildReadersFunc: func(ctx context.Context, proc any, expr *plan.Expr, relData engine.RelData, num int, txnOffset int, orderBy bool, policy engine.TombstoneApplyPolicy, filterHint engine.FilterHint) ([]engine.Reader, error) {
				rel2Called = true
				assert.Nil(t, relData)
				return []engine.Reader{reader2}, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.BuildReaders(ctx, nil, nil, nil, 1, 0, false, engine.Policy_CheckAll, engine.FilterHint{})
		assert.NoError(t, err)
		assert.Equal(t, []engine.Reader{reader1, reader2}, result)
		assert.True(t, rel1Called)
		assert.True(t, rel2Called)
	})

	t.Run("TablesFuncError", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result, err := table.BuildReaders(ctx, nil, nil, nil, 1, 0, false, engine.Policy_CheckAll, engine.FilterHint{})
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	t.Run("RelationBuildReadersError", func(t *testing.T) {
		mockRel := &mockRelation{
			buildReadersFunc: func(ctx context.Context, proc any, expr *plan.Expr, relData engine.RelData, num int, txnOffset int, orderBy bool, policy engine.TombstoneApplyPolicy, filterHint engine.FilterHint) ([]engine.Reader, error) {
				return nil, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.BuildReaders(ctx, nil, nil, nil, 1, 0, false, engine.Policy_CheckAll, engine.FilterHint{})
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})
}

func TestCombinedTxnTable_GetColumMetadataScanInfo(t *testing.T) {
	// Test case 1: Success case with multiple tables
	t.Run("Success with multiple tables", func(t *testing.T) {
		// Create mock metadata scan info
		mockInfo1 := &plan.MetadataScanInfo{
			ColName:      "col1",
			ObjectName:   "obj1",
			IsHidden:     false,
			RowCnt:       100,
			NullCnt:      5,
			CompressSize: 1024,
			OriginSize:   2048,
		}
		mockInfo2 := &plan.MetadataScanInfo{
			ColName:      "col1",
			ObjectName:   "obj2",
			IsHidden:     false,
			RowCnt:       200,
			NullCnt:      10,
			CompressSize: 2048,
			OriginSize:   4096,
		}

		// Create mock relations that return metadata scan info
		mockRel1 := &mockRelation{
			getColumMetadataScanInfoFunc: func(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error) {
				return []*plan.MetadataScanInfo{mockInfo1}, nil
			},
		}
		mockRel2 := &mockRelation{
			getColumMetadataScanInfoFunc: func(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error) {
				return []*plan.MetadataScanInfo{mockInfo2}, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.GetColumMetadataScanInfo(context.Background(), "col1", false)
		assert.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, mockInfo1, result[0])
		assert.Equal(t, mockInfo2, result[1])
	})

	// Test case 2: Error when tablesFunc returns error
	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result, err := table.GetColumMetadataScanInfo(context.Background(), "col1", false)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 3: Error from individual table's GetColumMetadataScanInfo
	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			getColumMetadataScanInfoFunc: func(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error) {
				return nil, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.GetColumMetadataScanInfo(context.Background(), "col1", false)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 4: Empty tables list
	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		result, err := table.GetColumMetadataScanInfo(context.Background(), "col1", false)
		assert.NoError(t, err)
		assert.Len(t, result, 0)
	})

	// Test case 5: Multiple tables with mixed results
	t.Run("Multiple tables with mixed results", func(t *testing.T) {
		mockInfo1 := &plan.MetadataScanInfo{
			ColName:    "col1",
			ObjectName: "obj1",
			IsHidden:   false,
			RowCnt:     100,
		}
		mockInfo2 := &plan.MetadataScanInfo{
			ColName:    "col1",
			ObjectName: "obj2",
			IsHidden:   true,
			RowCnt:     200,
		}
		mockInfo3 := &plan.MetadataScanInfo{
			ColName:    "col1",
			ObjectName: "obj3",
			IsHidden:   false,
			RowCnt:     300,
		}

		mockRel1 := &mockRelation{
			getColumMetadataScanInfoFunc: func(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error) {
				return []*plan.MetadataScanInfo{mockInfo1}, nil
			},
		}
		mockRel2 := &mockRelation{
			getColumMetadataScanInfoFunc: func(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error) {
				return []*plan.MetadataScanInfo{mockInfo2, mockInfo3}, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.GetColumMetadataScanInfo(context.Background(), "col1", true)
		assert.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, mockInfo1, result[0])
		assert.Equal(t, mockInfo2, result[1])
		assert.Equal(t, mockInfo3, result[2])
	})
}

func TestCombinedTxnTable_GetNonAppendableObjectStats(t *testing.T) {
	// Test case 1: Success case with multiple tables
	t.Run("Success with multiple tables", func(t *testing.T) {
		// Create mock object stats
		mockStats1 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats1, 1024)
		objectio.SetObjectStatsOriginSize(mockStats1, 2048)
		objectio.SetObjectStatsRowCnt(mockStats1, 100)

		mockStats2 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats2, 2048)
		objectio.SetObjectStatsOriginSize(mockStats2, 4096)
		objectio.SetObjectStatsRowCnt(mockStats2, 200)

		// Create mock relations that return object stats
		mockRel1 := &mockRelation{
			getNonAppendableObjectStatsFunc: func(ctx context.Context) ([]objectio.ObjectStats, error) {
				return []objectio.ObjectStats{*mockStats1}, nil
			},
		}
		mockRel2 := &mockRelation{
			getNonAppendableObjectStatsFunc: func(ctx context.Context) ([]objectio.ObjectStats, error) {
				return []objectio.ObjectStats{*mockStats2}, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.GetNonAppendableObjectStats(context.Background())
		assert.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, *mockStats1, result[0])
		assert.Equal(t, *mockStats2, result[1])
	})

	// Test case 2: Error when tablesFunc returns error
	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result, err := table.GetNonAppendableObjectStats(context.Background())
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 3: Error from individual table's GetNonAppendableObjectStats
	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			getNonAppendableObjectStatsFunc: func(ctx context.Context) ([]objectio.ObjectStats, error) {
				return nil, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.GetNonAppendableObjectStats(context.Background())
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 4: Empty tables list
	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		result, err := table.GetNonAppendableObjectStats(context.Background())
		assert.NoError(t, err)
		assert.Len(t, result, 0)
	})

	// Test case 5: Multiple tables with mixed results
	t.Run("Multiple tables with mixed results", func(t *testing.T) {
		mockStats1 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats1, 1024)
		objectio.SetObjectStatsRowCnt(mockStats1, 100)

		mockStats2 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats2, 2048)
		objectio.SetObjectStatsRowCnt(mockStats2, 200)

		mockStats3 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats3, 3072)
		objectio.SetObjectStatsRowCnt(mockStats3, 300)

		mockRel1 := &mockRelation{
			getNonAppendableObjectStatsFunc: func(ctx context.Context) ([]objectio.ObjectStats, error) {
				return []objectio.ObjectStats{*mockStats1}, nil
			},
		}
		mockRel2 := &mockRelation{
			getNonAppendableObjectStatsFunc: func(ctx context.Context) ([]objectio.ObjectStats, error) {
				return []objectio.ObjectStats{*mockStats2, *mockStats3}, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.GetNonAppendableObjectStats(context.Background())
		assert.NoError(t, err)
		assert.Len(t, result, 3)
		assert.Equal(t, *mockStats1, result[0])
		assert.Equal(t, *mockStats2, result[1])
		assert.Equal(t, *mockStats3, result[2])
	})

	// Test case 6: Single table with multiple object stats
	t.Run("Single table with multiple object stats", func(t *testing.T) {
		mockStats1 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats1, 1024)
		objectio.SetObjectStatsRowCnt(mockStats1, 100)

		mockStats2 := objectio.NewObjectStats()
		objectio.SetObjectStatsSize(mockStats2, 2048)
		objectio.SetObjectStatsRowCnt(mockStats2, 200)

		mockRel := &mockRelation{
			getNonAppendableObjectStatsFunc: func(ctx context.Context) ([]objectio.ObjectStats, error) {
				return []objectio.ObjectStats{*mockStats1, *mockStats2}, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.GetNonAppendableObjectStats(context.Background())
		assert.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Equal(t, *mockStats1, result[0])
		assert.Equal(t, *mockStats2, result[1])
	})
}

func TestCombinedTxnTable_ApproxObjectsNum(t *testing.T) {
	// Test case 1: Success case with multiple tables
	t.Run("Success with multiple tables", func(t *testing.T) {
		mockRel1 := &mockRelation{
			approxObjectsNumFunc: func(ctx context.Context) int {
				return 5
			},
		}
		mockRel2 := &mockRelation{
			approxObjectsNumFunc: func(ctx context.Context) int {
				return 10
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result := table.ApproxObjectsNum(context.Background())
		assert.Equal(t, 15, result)
	})

	// Test case 2: Error when tablesFunc returns error
	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result := table.ApproxObjectsNum(context.Background())
		assert.Equal(t, 0, result)
	})

	// Test case 3: Empty tables list
	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		result := table.ApproxObjectsNum(context.Background())
		assert.Equal(t, 0, result)
	})

	// Test case 4: Single table
	t.Run("Single table", func(t *testing.T) {
		mockRel := &mockRelation{
			approxObjectsNumFunc: func(ctx context.Context) int {
				return 25
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result := table.ApproxObjectsNum(context.Background())
		assert.Equal(t, 25, result)
	})
}

func TestCombinedTxnTable_CollectTombstones(t *testing.T) {
	// Test case 1: Success case with multiple tables
	t.Run("Success with multiple tables", func(t *testing.T) {
		// Create a tombstone that will be modified after merge
		hasInMemory := true
		hasFile := false

		mockTombstone1 := &mockTombstoner{
			hasAnyInMemoryTombstoneFunc: func() bool { return hasInMemory },
			hasAnyTombstoneFileFunc:     func() bool { return hasFile },
			mergeFunc: func(other engine.Tombstoner) error {
				// After merge, the first tombstone should have both properties
				hasInMemory = true
				hasFile = true
				return nil
			},
		}
		mockTombstone2 := &mockTombstoner{
			hasAnyInMemoryTombstoneFunc: func() bool { return false },
			hasAnyTombstoneFileFunc:     func() bool { return true },
		}

		mockRel1 := &mockRelation{
			collectTombstonesFunc: func(ctx context.Context, txnOffset int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
				return mockTombstone1, nil
			},
		}
		mockRel2 := &mockRelation{
			collectTombstonesFunc: func(ctx context.Context, txnOffset int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
				return mockTombstone2, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.CollectTombstones(context.Background(), 0, engine.Policy_CollectAllTombstones)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// After merging, the first tombstone should have both properties
		assert.True(t, result.HasAnyInMemoryTombstone())
		assert.True(t, result.HasAnyTombstoneFile())
	})

	// Test case 2: Error when tablesFunc returns error
	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result, err := table.CollectTombstones(context.Background(), 0, engine.Policy_CollectAllTombstones)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 3: Error from individual table's CollectTombstones
	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			collectTombstonesFunc: func(ctx context.Context, txnOffset int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
				return nil, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.CollectTombstones(context.Background(), 0, engine.Policy_CollectAllTombstones)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 4: Empty tables list
	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		result, err := table.CollectTombstones(context.Background(), 0, engine.Policy_CollectAllTombstones)
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	// Test case 5: Single table
	t.Run("Single table", func(t *testing.T) {
		mockTombstone := &mockTombstoner{
			hasAnyInMemoryTombstoneFunc: func() bool { return true },
			hasAnyTombstoneFileFunc:     func() bool { return false },
		}

		mockRel := &mockRelation{
			collectTombstonesFunc: func(ctx context.Context, txnOffset int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
				return mockTombstone, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.CollectTombstones(context.Background(), 0, engine.Policy_CollectAllTombstones)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.True(t, result.HasAnyInMemoryTombstone())
		assert.False(t, result.HasAnyTombstoneFile())
	})
}

func TestCombinedTxnTable_Size(t *testing.T) {
	// Test case 1: Success case with multiple tables
	t.Run("Success with multiple tables", func(t *testing.T) {
		mockRel1 := &mockRelation{
			sizeFunc: func(ctx context.Context, columnName string) (uint64, error) {
				return 1024, nil
			},
		}
		mockRel2 := &mockRelation{
			sizeFunc: func(ctx context.Context, columnName string) (uint64, error) {
				return 2048, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.Size(context.Background(), "col1")
		assert.NoError(t, err)
		assert.Equal(t, uint64(3072), result)
	})

	// Test case 2: Error when tablesFunc returns error
	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result, err := table.Size(context.Background(), "col1")
		assert.Error(t, err)
		assert.Equal(t, uint64(0), result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 3: Error from individual table's Size
	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			sizeFunc: func(ctx context.Context, columnName string) (uint64, error) {
				return 0, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.Size(context.Background(), "col1")
		assert.Error(t, err)
		assert.Equal(t, uint64(0), result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 4: Empty tables list
	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		result, err := table.Size(context.Background(), "col1")
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), result)
	})

	// Test case 5: Single table
	t.Run("Single table", func(t *testing.T) {
		mockRel := &mockRelation{
			sizeFunc: func(ctx context.Context, columnName string) (uint64, error) {
				return 4096, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.Size(context.Background(), "col1")
		assert.NoError(t, err)
		assert.Equal(t, uint64(4096), result)
	})
}

func TestCombinedTxnTable_StarCount(t *testing.T) {
	ctx := context.Background()

	t.Run("Success with multiple tables", func(t *testing.T) {
		mockRel1 := &mockRelation{
			starCountFunc: func(ctx context.Context) (uint64, error) {
				return 100, nil
			},
		}
		mockRel2 := &mockRelation{
			starCountFunc: func(ctx context.Context) (uint64, error) {
				return 200, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		count, err := table.StarCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(300), count)
	})

	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		count, err := table.StarCount(ctx)
		assert.Error(t, err)
		assert.Equal(t, uint64(0), count)
		assert.Equal(t, assert.AnError, err)
	})

	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			starCountFunc: func(ctx context.Context) (uint64, error) {
				return 0, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		count, err := table.StarCount(ctx)
		assert.Error(t, err)
		assert.Equal(t, uint64(0), count)
		assert.Equal(t, assert.AnError, err)
	})

	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		count, err := table.StarCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), count)
	})

	t.Run("Single table", func(t *testing.T) {
		mockRel := &mockRelation{
			starCountFunc: func(ctx context.Context) (uint64, error) {
				return 42, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		count, err := table.StarCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, uint64(42), count)
	})
}

func TestCombinedTxnTable_EstimateCommittedTombstoneCount(t *testing.T) {
	ctx := context.Background()

	t.Run("Success with multiple tables", func(t *testing.T) {
		mockRel1 := &mockRelation{
			estimateCommittedTombstoneCountFunc: func(ctx context.Context) (int, error) {
				return 10, nil
			},
		}
		mockRel2 := &mockRelation{
			estimateCommittedTombstoneCountFunc: func(ctx context.Context) (int, error) {
				return 20, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		count, err := table.EstimateCommittedTombstoneCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 30, count)
	})

	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		count, err := table.EstimateCommittedTombstoneCount(ctx)
		assert.Error(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, assert.AnError, err)
	})

	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			estimateCommittedTombstoneCountFunc: func(ctx context.Context) (int, error) {
				return 0, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		count, err := table.EstimateCommittedTombstoneCount(ctx)
		assert.Error(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, assert.AnError, err)
	})

	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		count, err := table.EstimateCommittedTombstoneCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("Single table", func(t *testing.T) {
		mockRel := &mockRelation{
			estimateCommittedTombstoneCountFunc: func(ctx context.Context) (int, error) {
				return 5, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		count, err := table.EstimateCommittedTombstoneCount(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 5, count)
	})
}

func TestCombinedTxnTable_Rows(t *testing.T) {
	// Test case 1: Success case with multiple tables
	t.Run("Success with multiple tables", func(t *testing.T) {
		mockRel1 := &mockRelation{
			rowsFunc: func(ctx context.Context) (uint64, error) {
				return 100, nil
			},
		}
		mockRel2 := &mockRelation{
			rowsFunc: func(ctx context.Context) (uint64, error) {
				return 200, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel1, mockRel2}, nil
			},
		}

		result, err := table.Rows(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, uint64(300), result)
	})

	// Test case 2: Error when tablesFunc returns error
	t.Run("Error from tablesFunc", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return nil, assert.AnError
			},
		}

		result, err := table.Rows(context.Background())
		assert.Error(t, err)
		assert.Equal(t, uint64(0), result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 3: Error from individual table's Rows
	t.Run("Error from individual table", func(t *testing.T) {
		mockRel := &mockRelation{
			rowsFunc: func(ctx context.Context) (uint64, error) {
				return 0, assert.AnError
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.Rows(context.Background())
		assert.Error(t, err)
		assert.Equal(t, uint64(0), result)
		assert.Equal(t, assert.AnError, err)
	})

	// Test case 4: Empty tables list
	t.Run("Empty tables list", func(t *testing.T) {
		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{}, nil
			},
		}

		result, err := table.Rows(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), result)
	})

	// Test case 5: Single table
	t.Run("Single table", func(t *testing.T) {
		mockRel := &mockRelation{
			rowsFunc: func(ctx context.Context) (uint64, error) {
				return 500, nil
			},
		}

		table := &combinedTxnTable{
			tablesFunc: func() ([]engine.Relation, error) {
				return []engine.Relation{mockRel}, nil
			},
		}

		result, err := table.Rows(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, uint64(500), result)
	})
}

// Test CombinedRelData panic methods
func TestCombinedRelData_GetType(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.GetType()
	})
}

func TestCombinedRelData_MarshalBinary(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.MarshalBinary()
	})
}

func TestCombinedRelData_UnmarshalBinary(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.UnmarshalBinary([]byte{})
	})
}

func TestCombinedRelData_GetTombstones(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.GetTombstones()
	})
}

func TestCombinedRelData_DataSlice(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.DataSlice(0, 1)
	})
}

func TestCombinedRelData_GetShardIDList(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.GetShardIDList()
	})
}

func TestCombinedRelData_GetShardID(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.GetShardID(0)
	})
}

func TestCombinedRelData_SetShardID(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.SetShardID(0, 1)
	})
}

func TestCombinedRelData_AppendShardID(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.AppendShardID(1)
	})
}

func TestCombinedRelData_SetBlockInfo(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.SetBlockInfo(0, &objectio.BlockInfo{})
	})
}

func TestCombinedRelData_GetBlockInfo(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.GetBlockInfo(0)
	})
}

func TestCombinedRelData_AppendBlockInfo(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.AppendBlockInfo(&objectio.BlockInfo{})
	})
}

func TestCombinedRelData_AppendBlockInfoSlice(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.AppendBlockInfoSlice(objectio.BlockInfoSlice{})
	})
}

func TestCombinedRelData_Split(t *testing.T) {
	data := &CombinedRelData{}

	assert.PanicsWithValue(t, "not implemented", func() {
		data.Split(0)
	})
}

// Test CombinedRelData non-panic methods
func TestCombinedRelData_String(t *testing.T) {
	data := &CombinedRelData{}
	assert.Equal(t, "PartitionedRelData", data.String())
}

func TestCombinedRelData_DataCnt(t *testing.T) {
	data := &CombinedRelData{
		cnt: 5,
	}
	assert.Equal(t, 5, data.DataCnt())
}

func TestCombinedRelData_GetBlockInfoSlice(t *testing.T) {
	data := &CombinedRelData{
		blocks: objectio.BlockInfoSlice{},
	}
	assert.Equal(t, objectio.BlockInfoSlice{}, data.GetBlockInfoSlice())
}

func TestCombinedRelData_AttachTombstones(t *testing.T) {
	data := &CombinedRelData{
		tables: []engine.RelData{},
	}
	// Should not panic when tables is empty
	assert.NoError(t, data.AttachTombstones(nil))
}

func TestCombinedRelData_BuildEmptyRelData(t *testing.T) {
	data := &CombinedRelData{
		tables: []engine.RelData{},
	}

	assert.PanicsWithValue(t, "BUG: no partitions", func() {
		data.BuildEmptyRelData(10)
	})
}

// Test newCombinedRelData
func TestNewCombinedRelData(t *testing.T) {
	data := newCombinedRelData()
	assert.NotNil(t, data)
	assert.Equal(t, 0, data.cnt)
	assert.Nil(t, data.blocks)
	assert.Nil(t, data.tables)
	assert.Nil(t, data.relations)
}

// Test add method with mock relation
func TestCombinedRelData_Add(t *testing.T) {
	data := newCombinedRelData()

	// Create a mock relation that returns a simple RelData
	mockRel := &mockRelation{
		rangesFunc: func(ctx context.Context, param engine.RangesParam) (engine.RelData, error) {
			return &mockRelData{
				dataCnt: 1,
				blocks:  objectio.BlockInfoSlice{},
			}, nil
		},
	}

	err := data.add(context.Background(), mockRel, engine.RangesParam{})
	assert.NoError(t, err)
	assert.Equal(t, 1, data.cnt)
	assert.Len(t, data.relations, 1)
	assert.Len(t, data.tables, 1)
}

// Mock implementations for testing

// Mock Tombstoner implementation
type mockTombstoner struct {
	hasAnyInMemoryTombstoneFunc func() bool
	hasAnyTombstoneFileFunc     func() bool
	mergeFunc                   func(other engine.Tombstoner) error
}

func (m *mockTombstoner) Type() engine.TombstoneType {
	return engine.TombstoneData
}

func (m *mockTombstoner) HasAnyInMemoryTombstone() bool {
	if m.hasAnyInMemoryTombstoneFunc != nil {
		return m.hasAnyInMemoryTombstoneFunc()
	}
	return false
}

func (m *mockTombstoner) HasAnyTombstoneFile() bool {
	if m.hasAnyTombstoneFileFunc != nil {
		return m.hasAnyTombstoneFileFunc()
	}
	return false
}

func (m *mockTombstoner) String() string {
	return "MockTombstoner"
}

func (m *mockTombstoner) StringWithPrefix(prefix string) string {
	return prefix + "MockTombstoner"
}

func (m *mockTombstoner) HasBlockTombstone(ctx context.Context, id *objectio.Blockid, fs fileservice.FileService) (bool, error) {
	return false, nil
}

func (m *mockTombstoner) MarshalBinaryWithBuffer(w *bytes.Buffer) error {
	return nil
}

func (m *mockTombstoner) UnmarshalBinary(buf []byte) error {
	return nil
}

func (m *mockTombstoner) PrefetchTombstones(srvId string, fs fileservice.FileService, bid []objectio.Blockid) {
}

func (m *mockTombstoner) ApplyInMemTombstones(bid *types.Blockid, rowsOffset []int64, deleted *objectio.Bitmap) (left []int64) {
	return rowsOffset
}

func (m *mockTombstoner) ApplyPersistedTombstones(ctx context.Context, fs fileservice.FileService, snapshot *types.TS, bid *types.Blockid, rowsOffset []int64, deletedMask *objectio.Bitmap) (left []int64, err error) {
	return rowsOffset, nil
}

func (m *mockTombstoner) Merge(other engine.Tombstoner) error {
	if m.mergeFunc != nil {
		return m.mergeFunc(other)
	}
	return nil
}

func (m *mockTombstoner) SortInMemory() {
}

type mockReader struct{}

func (m *mockReader) Close() error {
	return nil
}

func (m *mockReader) Read(context.Context, []string, *plan.Expr, *mpool.MPool, *batch.Batch) (bool, error) {
	return false, nil
}

func (m *mockReader) SetOrderBy([]*plan.OrderBySpec) {}

func (m *mockReader) GetOrderBy() []*plan.OrderBySpec {
	return nil
}

func (m *mockReader) SetIndexParam(param *plan.IndexReaderParam) {}

func (m *mockReader) SetFilterZM(objectio.ZoneMap) {}

type mockRelation struct {
	rangesFunc                          func(ctx context.Context, param engine.RangesParam) (engine.RelData, error)
	getColumMetadataScanInfoFunc        func(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error)
	getNonAppendableObjectStatsFunc     func(ctx context.Context) ([]objectio.ObjectStats, error)
	approxObjectsNumFunc                func(ctx context.Context) int
	collectTombstonesFunc               func(ctx context.Context, txnOffset int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error)
	sizeFunc                            func(ctx context.Context, columnName string) (uint64, error)
	rowsFunc                            func(ctx context.Context) (uint64, error)
	starCountFunc                       func(ctx context.Context) (uint64, error)
	estimateCommittedTombstoneCountFunc func(ctx context.Context) (int, error)
	buildReadersFunc                    func(ctx context.Context, proc any, expr *plan.Expr, relData engine.RelData, num int, txnOffset int, orderBy bool, policy engine.TombstoneApplyPolicy, filterHint engine.FilterHint) ([]engine.Reader, error)
}

func (m *mockRelation) Ranges(ctx context.Context, param engine.RangesParam) (engine.RelData, error) {
	return m.rangesFunc(ctx, param)
}

// Implement other required methods with empty implementations
func (m *mockRelation) BuildReaders(ctx context.Context, proc any, expr *plan.Expr, relData engine.RelData, num int, txnOffset int, orderBy bool, policy engine.TombstoneApplyPolicy, filterHint engine.FilterHint) ([]engine.Reader, error) {
	if m.buildReadersFunc != nil {
		return m.buildReadersFunc(ctx, proc, expr, relData, num, txnOffset, orderBy, policy, filterHint)
	}
	return nil, nil
}

func (m *mockRelation) BuildShardingReaders(ctx context.Context, proc any, expr *plan.Expr, relData engine.RelData, num int, txnOffset int, orderBy bool, policy engine.TombstoneApplyPolicy) ([]engine.Reader, error) {
	return nil, nil
}

func (m *mockRelation) Rows(ctx context.Context) (uint64, error) {
	if m.rowsFunc != nil {
		return m.rowsFunc(ctx)
	}
	return 0, nil
}

func (m *mockRelation) Stats(ctx context.Context, sync bool) (*statsinfo.StatsInfo, error) {
	return nil, nil
}

func (m *mockRelation) Size(ctx context.Context, columnName string) (uint64, error) {
	if m.sizeFunc != nil {
		return m.sizeFunc(ctx, columnName)
	}
	return 0, nil
}

func (m *mockRelation) CollectTombstones(ctx context.Context, txnOffset int, policy engine.TombstoneCollectPolicy) (engine.Tombstoner, error) {
	if m.collectTombstonesFunc != nil {
		return m.collectTombstonesFunc(ctx, txnOffset, policy)
	}
	return nil, nil
}

func (m *mockRelation) StarCount(ctx context.Context) (uint64, error) {
	if m.starCountFunc != nil {
		return m.starCountFunc(ctx)
	}
	return 0, nil
}

func (m *mockRelation) EstimateCommittedTombstoneCount(ctx context.Context) (int, error) {
	if m.estimateCommittedTombstoneCountFunc != nil {
		return m.estimateCommittedTombstoneCountFunc(ctx)
	}
	return 0, nil
}

func (m *mockRelation) CollectChanges(ctx context.Context, from, to types.TS, _ bool, mp *mpool.MPool) (engine.ChangesHandle, error) {
	return nil, nil
}

func (m *mockRelation) ApproxObjectsNum(ctx context.Context) int {
	if m.approxObjectsNumFunc != nil {
		return m.approxObjectsNumFunc(ctx)
	}
	return 0
}

func (m *mockRelation) MergeObjects(ctx context.Context, objstats []objectio.ObjectStats, targetObjSize uint32) (*api.MergeCommitEntry, error) {
	return nil, nil
}

func (m *mockRelation) GetNonAppendableObjectStats(ctx context.Context) ([]objectio.ObjectStats, error) {
	if m.getNonAppendableObjectStatsFunc != nil {
		return m.getNonAppendableObjectStatsFunc(ctx)
	}
	return nil, nil
}

func (m *mockRelation) GetColumMetadataScanInfo(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error) {
	if m.getColumMetadataScanInfoFunc != nil {
		return m.getColumMetadataScanInfoFunc(ctx, name, visitTombstone)
	}
	return nil, nil
}

func (m *mockRelation) UpdateConstraint(ctx context.Context, constraint *engine.ConstraintDef) error {
	return nil
}

func (m *mockRelation) AlterTable(ctx context.Context, c *engine.ConstraintDef, reqs []*api.AlterTableReq) error {
	return nil
}

func (m *mockRelation) TableRenameInTxn(ctx context.Context, constraint [][]byte) error {
	return nil
}

func (m *mockRelation) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	return nil, nil, nil
}

func (m *mockRelation) TableDefs(ctx context.Context) ([]engine.TableDef, error) {
	return nil, nil
}

func (m *mockRelation) GetTableDef(ctx context.Context) *plan.TableDef {
	return nil
}

func (m *mockRelation) CopyTableDef(ctx context.Context) *plan.TableDef {
	return nil
}

func (m *mockRelation) GetPrimaryKeys(ctx context.Context) ([]*engine.Attribute, error) {
	return nil, nil
}

func (m *mockRelation) AddTableDef(ctx context.Context, def engine.TableDef) error {
	return nil
}

func (m *mockRelation) DelTableDef(ctx context.Context, def engine.TableDef) error {
	return nil
}

func (m *mockRelation) GetTableID(ctx context.Context) uint64 {
	return 0
}

func (m *mockRelation) GetTableName() string {
	return ""
}

func (m *mockRelation) GetDBID(ctx context.Context) uint64 {
	return 0
}

func (m *mockRelation) TableColumns(ctx context.Context) ([]*engine.Attribute, error) {
	return nil, nil
}

func (m *mockRelation) GetEngineType() engine.EngineType {
	return engine.Disttae
}

func (m *mockRelation) GetProcess() any {
	return nil
}

func (m *mockRelation) PrimaryKeysMayBeModified(ctx context.Context, from types.TS, to types.TS, bat *batch.Batch, pkIndex int32, partitionIndex int32) (bool, error) {
	return false, nil
}

func (m *mockRelation) Write(ctx context.Context, bat *batch.Batch) error {
	return nil
}

func (m *mockRelation) Delete(ctx context.Context, bat *batch.Batch, s string) error {
	return nil
}

func (m *mockRelation) PrimaryKeysMayBeUpserted(ctx context.Context, from types.TS, to types.TS, bat *batch.Batch, pkIndex int32) (bool, error) {
	return false, nil
}

func (m *mockRelation) Reset(op client.TxnOperator) error {
	return nil
}

func (m *mockRelation) GetExtraInfo() *api.SchemaExtra {
	return nil
}

type mockRelData struct {
	dataCnt int
	blocks  objectio.BlockInfoSlice
}

func (m *mockRelData) GetType() engine.RelDataType {
	return engine.RelDataEmpty
}

func (m *mockRelData) String() string {
	return "MockRelData"
}

func (m *mockRelData) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (m *mockRelData) UnmarshalBinary(buf []byte) error {
	return nil
}

func (m *mockRelData) AttachTombstones(tombstones engine.Tombstoner) error {
	return nil
}

func (m *mockRelData) GetTombstones() engine.Tombstoner {
	return nil
}

func (m *mockRelData) DataSlice(begin, end int) engine.RelData {
	return m
}

func (m *mockRelData) BuildEmptyRelData(preAllocSize int) engine.RelData {
	return m
}

func (m *mockRelData) DataCnt() int {
	return m.dataCnt
}

func (m *mockRelData) GetShardIDList() []uint64 {
	return nil
}

func (m *mockRelData) GetShardID(i int) uint64 {
	return 0
}

func (m *mockRelData) SetShardID(i int, id uint64) {
}

func (m *mockRelData) AppendShardID(id uint64) {
}

func (m *mockRelData) Split(i int) []engine.RelData {
	return nil
}

func (m *mockRelData) GetBlockInfoSlice() objectio.BlockInfoSlice {
	return m.blocks
}

func (m *mockRelData) GetBlockInfo(i int) objectio.BlockInfo {
	return objectio.BlockInfo{}
}

func (m *mockRelData) SetBlockInfo(i int, blk *objectio.BlockInfo) {
}

func (m *mockRelData) AppendBlockInfo(blk *objectio.BlockInfo) {
}

func (m *mockRelData) AppendBlockInfoSlice(objectio.BlockInfoSlice) {
}
