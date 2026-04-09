// Copyright 2024 Matrix Origin
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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoverage_genCdcTaskJobID(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple index name", "idx1", "index_idx1"},
		{"empty string", "", "index_"},
		{"complex name", "my_hnsw_index_123", "index_my_hnsw_index_123"},
		{"with special chars", "idx-test.1", "index_idx-test.1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := genCdcTaskJobID(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCoverage_getSinkerTypeFromAlgo(t *testing.T) {
	t.Run("hnsw returns IndexSync", func(t *testing.T) {
		result := getSinkerTypeFromAlgo("hnsw")
		assert.Equal(t, int8(iscp.ConsumerType_IndexSync), result)
	})

	t.Run("ivfflat returns IndexSync", func(t *testing.T) {
		result := getSinkerTypeFromAlgo("ivfflat")
		assert.Equal(t, int8(iscp.ConsumerType_IndexSync), result)
	})

	t.Run("fulltext returns IndexSync", func(t *testing.T) {
		result := getSinkerTypeFromAlgo("fulltext")
		assert.Equal(t, int8(iscp.ConsumerType_IndexSync), result)
	})

	t.Run("invalid algo panics", func(t *testing.T) {
		assert.Panics(t, func() {
			getSinkerTypeFromAlgo("invalid_algo")
		})
	})
}

func TestCoverage_checkValidIndexUpdateByIndexdef(t *testing.T) {
	t.Run("ivfflat with table exist returns true", func(t *testing.T) {
		idx := &plan.IndexDef{
			TableExist: true,
			IndexAlgo:  "ivfflat",
		}
		valid, err := checkValidIndexUpdateByIndexdef(idx)
		require.Nil(t, err)
		assert.True(t, valid)
	})

	t.Run("ivfflat without table exist returns false", func(t *testing.T) {
		idx := &plan.IndexDef{
			TableExist: false,
			IndexAlgo:  "ivfflat",
		}
		valid, err := checkValidIndexUpdateByIndexdef(idx)
		require.Nil(t, err)
		assert.False(t, valid)
	})

	t.Run("hnsw returns false", func(t *testing.T) {
		idx := &plan.IndexDef{
			TableExist: true,
			IndexAlgo:  "hnsw",
		}
		valid, err := checkValidIndexUpdateByIndexdef(idx)
		require.Nil(t, err)
		assert.False(t, valid)
	})

	t.Run("fulltext returns false", func(t *testing.T) {
		idx := &plan.IndexDef{
			TableExist: true,
			IndexAlgo:  "fulltext",
		}
		valid, err := checkValidIndexUpdateByIndexdef(idx)
		require.Nil(t, err)
		assert.False(t, valid)
	})

	t.Run("empty algo returns false", func(t *testing.T) {
		idx := &plan.IndexDef{
			TableExist: true,
			IndexAlgo:  "",
		}
		valid, err := checkValidIndexUpdateByIndexdef(idx)
		require.Nil(t, err)
		assert.False(t, valid)
	})
}

func TestCoverage_checkValidIndexCdcByIndexdef_HNSW(t *testing.T) {
	// HNSW always returns true when TableExist is true
	idx := &plan.IndexDef{
		TableExist:      true,
		IndexAlgo:       "hnsw",
		IndexAlgoParams: `{}`,
	}
	valid, err := checkValidIndexCdcByIndexdef(idx)
	require.Nil(t, err)
	assert.True(t, valid)
}

func TestCoverage_checkValidIndexCdcByIndexdef_NonExistTable(t *testing.T) {
	idx := &plan.IndexDef{
		TableExist:      false,
		IndexAlgo:       "hnsw",
		IndexAlgoParams: `{}`,
	}
	valid, err := checkValidIndexCdcByIndexdef(idx)
	require.Nil(t, err)
	assert.False(t, valid)
}

func TestCoverage_checkValidIndexCdcByIndexdef_Fulltext(t *testing.T) {
	t.Run("fulltext async true", func(t *testing.T) {
		idx := &plan.IndexDef{
			TableExist:      true,
			IndexAlgo:       "fulltext",
			IndexAlgoParams: `{"async":"true"}`,
		}
		valid, err := checkValidIndexCdcByIndexdef(idx)
		require.Nil(t, err)
		assert.True(t, valid)
	})

	t.Run("fulltext async false", func(t *testing.T) {
		idx := &plan.IndexDef{
			TableExist:      true,
			IndexAlgo:       "fulltext",
			IndexAlgoParams: `{"async":"false"}`,
		}
		valid, err := checkValidIndexCdcByIndexdef(idx)
		require.Nil(t, err)
		assert.False(t, valid)
	})
}

func TestCoverage_checkValidIndexCdcByIndexdef_RegularIndex(t *testing.T) {
	// Non-vector index should return false
	idx := &plan.IndexDef{
		TableExist: true,
		IndexAlgo:  "",
	}
	valid, err := checkValidIndexCdcByIndexdef(idx)
	require.Nil(t, err)
	assert.False(t, valid)
}

func TestCoverage_checkValidIndexCdc_NotFound(t *testing.T) {
	tbldef := &plan.TableDef{
		Indexes: []*plan.IndexDef{
			{
				TableExist:      true,
				IndexName:       "a",
				IndexAlgo:       "hnsw",
				IndexAlgoParams: `{}`,
			},
		},
	}
	// Looking for non-existent index name
	valid, err := checkValidIndexCdc(tbldef, "nonexistent")
	require.Nil(t, err)
	assert.False(t, valid)
}

func TestCoverage_checkValidIndexCdc_EmptyIndexes(t *testing.T) {
	tbldef := &plan.TableDef{
		Indexes: []*plan.IndexDef{},
	}
	valid, err := checkValidIndexCdc(tbldef, "a")
	require.Nil(t, err)
	assert.False(t, valid)
}

func TestCoverage_checkValidIndexCdc_MatchButInvalid(t *testing.T) {
	// Index found but it's not a valid CDC index (regular btree)
	tbldef := &plan.TableDef{
		Indexes: []*plan.IndexDef{
			{
				TableExist: true,
				IndexName:  "idx1",
				IndexAlgo:  "",
			},
		},
	}
	valid, err := checkValidIndexCdc(tbldef, "idx1")
	require.Nil(t, err)
	assert.False(t, valid)
}

func TestCoverage_CreateIndexCdcTask_SkipForPublication(t *testing.T) {
	iscpRegisterJobFunc = func(ctx context.Context, cnUUID string, txn client.TxnOperator, spec *iscp.JobSpec, job *iscp.JobID, startFromNow bool) (bool, error) {
		t.Fatal("should not be called for publication table")
		return false, nil
	}
	isTableInCCPRFunc = mockIsTableInCCPRFalse
	defer func() {
		iscpRegisterJobFunc = iscp.RegisterJob
		isTableInCCPRFunc = isTableInCCPRImpl
	}()

	c := &Compile{}
	c.proc = testutil.NewProcess(t)

	// Create table def with from_publication property
	td := &plan.TableDef{
		Defs: []*plan.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{Key: "from_publication", Value: "true"},
						},
					},
				},
			},
		},
	}

	err := CreateIndexCdcTask(c, "db", "tbl", 1, "idx1", 0, true, "", td)
	require.Nil(t, err)
}

func TestCoverage_CreateIndexCdcTask_SkipForCCPR(t *testing.T) {
	registerCalled := false
	iscpRegisterJobFunc = func(ctx context.Context, cnUUID string, txn client.TxnOperator, spec *iscp.JobSpec, job *iscp.JobID, startFromNow bool) (bool, error) {
		registerCalled = true
		return true, nil
	}
	isTableInCCPRFunc = func(c *Compile, tableid uint64) bool {
		return true // Table is in CCPR
	}
	defer func() {
		iscpRegisterJobFunc = iscp.RegisterJob
		isTableInCCPRFunc = isTableInCCPRImpl
	}()

	c := &Compile{}
	c.proc = testutil.NewProcess(t)

	err := CreateIndexCdcTask(c, "db", "tbl", 1, "idx1", 0, true, "", nil)
	require.Nil(t, err)
	assert.False(t, registerCalled, "register should not be called for CCPR table")
}

func TestCoverage_CreateIndexCdcTask_AlreadyExists(t *testing.T) {
	iscpRegisterJobFunc = func(ctx context.Context, cnUUID string, txn client.TxnOperator, spec *iscp.JobSpec, job *iscp.JobID, startFromNow bool) (bool, error) {
		return false, nil // ok=false means already exists
	}
	isTableInCCPRFunc = mockIsTableInCCPRFalse
	defer func() {
		iscpRegisterJobFunc = iscp.RegisterJob
		isTableInCCPRFunc = isTableInCCPRImpl
	}()

	c := &Compile{}
	c.proc = testutil.NewProcess(t)

	err := CreateIndexCdcTask(c, "db", "tbl", 1, "idx1", 0, true, "", nil)
	require.Nil(t, err)
}

func TestCoverage_DropIndexCdcTask_InvalidIndex(t *testing.T) {
	iscpUnregisterJobFunc = func(ctx context.Context, cnUUID string, txn client.TxnOperator, job *iscp.JobID) (bool, error) {
		t.Fatal("should not be called for invalid index")
		return false, nil
	}
	defer func() {
		iscpUnregisterJobFunc = iscp.UnregisterJob
	}()

	c := &Compile{}
	c.proc = testutil.NewProcess(t)

	// Regular (non-CDC) index
	tbldef := &plan.TableDef{
		Indexes: []*plan.IndexDef{
			{
				TableExist: true,
				IndexName:  "regular_idx",
				IndexAlgo:  "",
			},
		},
	}
	err := DropIndexCdcTask(c, tbldef, "db", "tbl", "regular_idx")
	require.Nil(t, err)
}

func TestCoverage_DropAllIndexCdcTasks_DuplicateNames(t *testing.T) {
	dropCount := 0
	iscpUnregisterJobFunc = func(ctx context.Context, cnUUID string, txn client.TxnOperator, job *iscp.JobID) (bool, error) {
		dropCount++
		return true, nil
	}
	defer func() {
		iscpUnregisterJobFunc = iscp.UnregisterJob
	}()

	c := &Compile{}
	c.proc = testutil.NewProcess(t)

	// Table with duplicate index names - should only drop once
	tbldef := &plan.TableDef{
		Indexes: []*plan.IndexDef{
			{
				TableExist:      true,
				IndexName:       "idx1",
				IndexAlgo:       "hnsw",
				IndexAlgoParams: `{}`,
			},
			{
				TableExist:      true,
				IndexName:       "idx1",
				IndexAlgo:       "hnsw",
				IndexAlgoParams: `{}`,
			},
		},
	}
	err := DropAllIndexCdcTasks(c, tbldef, "db", "tbl")
	require.Nil(t, err)
	assert.Equal(t, 1, dropCount, "duplicate index names should be deduplicated")
}

func TestCoverage_DropAllIndexCdcTasks_MixedIndexes(t *testing.T) {
	dropCount := 0
	iscpUnregisterJobFunc = func(ctx context.Context, cnUUID string, txn client.TxnOperator, job *iscp.JobID) (bool, error) {
		dropCount++
		return true, nil
	}
	defer func() {
		iscpUnregisterJobFunc = iscp.UnregisterJob
	}()

	c := &Compile{}
	c.proc = testutil.NewProcess(t)

	tbldef := &plan.TableDef{
		Indexes: []*plan.IndexDef{
			{
				TableExist:      true,
				IndexName:       "hnsw_idx",
				IndexAlgo:       "hnsw",
				IndexAlgoParams: `{}`,
			},
			{
				TableExist: true,
				IndexName:  "regular_idx",
				IndexAlgo:  "",
			},
			{
				TableExist:      true,
				IndexName:       "ivf_idx",
				IndexAlgo:       "ivfflat",
				IndexAlgoParams: `{"async":"true"}`,
			},
		},
	}
	err := DropAllIndexCdcTasks(c, tbldef, "db", "tbl")
	require.Nil(t, err)
	assert.Equal(t, 2, dropCount, "should only drop hnsw and ivfflat indexes")
}

func TestCoverage_CreateAllIndexCdcTasks_DuplicateNames(t *testing.T) {
	createCount := 0
	iscpRegisterJobFunc = func(ctx context.Context, cnUUID string, txn client.TxnOperator, spec *iscp.JobSpec, job *iscp.JobID, startFromNow bool) (bool, error) {
		createCount++
		return true, nil
	}
	isTableInCCPRFunc = mockIsTableInCCPRFalse
	defer func() {
		iscpRegisterJobFunc = iscp.RegisterJob
		isTableInCCPRFunc = isTableInCCPRImpl
	}()

	c := &Compile{}
	c.proc = testutil.NewProcess(t)

	indexes := []*plan.IndexDef{
		{
			TableExist:      true,
			IndexName:       "idx1",
			IndexAlgo:       "hnsw",
			IndexAlgoParams: `{}`,
		},
		{
			TableExist:      true,
			IndexName:       "idx1",
			IndexAlgo:       "hnsw",
			IndexAlgoParams: `{}`,
		},
	}
	err := CreateAllIndexCdcTasks(c, indexes, "db", "tbl", 1, false, nil)
	require.Nil(t, err)
	assert.Equal(t, 1, createCount, "duplicate index names should be deduplicated")
}

func TestCoverage_CreateAllIndexCdcTasks_NoValidIndexes(t *testing.T) {
	iscpRegisterJobFunc = func(ctx context.Context, cnUUID string, txn client.TxnOperator, spec *iscp.JobSpec, job *iscp.JobID, startFromNow bool) (bool, error) {
		t.Fatal("should not be called when no valid indexes")
		return false, nil
	}
	isTableInCCPRFunc = mockIsTableInCCPRFalse
	defer func() {
		iscpRegisterJobFunc = iscp.RegisterJob
		isTableInCCPRFunc = isTableInCCPRImpl
	}()

	c := &Compile{}
	c.proc = testutil.NewProcess(t)

	indexes := []*plan.IndexDef{
		{
			TableExist: true,
			IndexName:  "regular_idx",
			IndexAlgo:  "",
		},
	}
	err := CreateAllIndexCdcTasks(c, indexes, "db", "tbl", 1, false, nil)
	require.Nil(t, err)
}
