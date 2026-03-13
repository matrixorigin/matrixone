// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineColumnInfo(t *testing.T) {
	ec := &engineColumnInfo{
		name: "test_column",
		typ:  types.New(types.T_int64, 0, 0),
	}

	assert.Equal(t, "test_column", ec.GetName())
	assert.Equal(t, types.T_int64, ec.GetType())
}

func TestInternalCmdFieldList(t *testing.T) {
	icfl := &InternalCmdFieldList{
		tableName: "test_table",
	}

	t.Run("Free", func(t *testing.T) {
		icfl.Free() // Should not panic
	})

	t.Run("String", func(t *testing.T) {
		s := icfl.String()
		assert.NotEmpty(t, s)
	})

	t.Run("StmtKind", func(t *testing.T) {
		kind := icfl.StmtKind()
		expected := tree.MakeStmtKind(tree.OUTPUT_STATUS, tree.RESP_STATUS, tree.EXEC_IN_FRONTEND)
		assert.Equal(t, expected, kind)
	})

	t.Run("GetStatementType", func(t *testing.T) {
		assert.Equal(t, "InternalCmd", icfl.GetStatementType())
	})

	t.Run("GetQueryType", func(t *testing.T) {
		assert.Equal(t, tree.QueryTypeDQL, icfl.GetQueryType())
	})
}


func TestInternalCmdGetSnapshotTs(t *testing.T) {
	ic := &InternalCmdGetSnapshotTs{}

	t.Run("Free", func(t *testing.T) {
		ic.Free() // Should not panic
	})

	t.Run("String", func(t *testing.T) {
		s := ic.String()
		assert.NotEmpty(t, s)
	})

	t.Run("StmtKind", func(t *testing.T) {
		kind := ic.StmtKind()
		expected := tree.MakeStmtKind(tree.OUTPUT_RESULT_ROW, tree.RESP_PREBUILD_RESULT_ROW, tree.EXEC_IN_FRONTEND)
		assert.Equal(t, expected, kind)
	})

	t.Run("GetStatementType", func(t *testing.T) {
		assert.Equal(t, "InternalCmd", ic.GetStatementType())
	})

	t.Run("GetQueryType", func(t *testing.T) {
		assert.Equal(t, tree.QueryTypeDQL, ic.GetQueryType())
	})
}

func TestInternalCmdGetDatabases(t *testing.T) {
	ic := &InternalCmdGetDatabases{}

	t.Run("Free", func(t *testing.T) {
		ic.Free() // Should not panic
	})

	t.Run("String", func(t *testing.T) {
		s := ic.String()
		assert.NotEmpty(t, s)
	})

	t.Run("StmtKind", func(t *testing.T) {
		kind := ic.StmtKind()
		expected := tree.MakeStmtKind(tree.OUTPUT_RESULT_ROW, tree.RESP_PREBUILD_RESULT_ROW, tree.EXEC_IN_FRONTEND)
		assert.Equal(t, expected, kind)
	})

	t.Run("GetStatementType", func(t *testing.T) {
		assert.Equal(t, "InternalCmd", ic.GetStatementType())
	})

	t.Run("GetQueryType", func(t *testing.T) {
		assert.Equal(t, tree.QueryTypeDQL, ic.GetQueryType())
	})
}

func TestInternalCmdGetMoIndexes(t *testing.T) {
	ic := &InternalCmdGetMoIndexes{
		tableId:                 123,
		subscriptionAccountName: "test_account",
		publicationName:         "test_pub",
		snapshotName:            "test_snapshot",
	}

	t.Run("Free", func(t *testing.T) {
		ic.Free() // Should not panic
	})

	t.Run("String", func(t *testing.T) {
		s := ic.String()
		assert.NotEmpty(t, s)
	})

	t.Run("StmtKind", func(t *testing.T) {
		kind := ic.StmtKind()
		expected := tree.MakeStmtKind(tree.OUTPUT_RESULT_ROW, tree.RESP_PREBUILD_RESULT_ROW, tree.EXEC_IN_FRONTEND)
		assert.Equal(t, expected, kind)
	})

	t.Run("GetStatementType", func(t *testing.T) {
		assert.Equal(t, "InternalCmd", ic.GetStatementType())
	})

	t.Run("GetQueryType", func(t *testing.T) {
		assert.Equal(t, tree.QueryTypeDQL, ic.GetQueryType())
	})
}

func TestInternalCmdGetDdl(t *testing.T) {
	ic := &InternalCmdGetDdl{
		snapshotName:            "test_snapshot",
		subscriptionAccountName: "test_account",
		publicationName:         "test_pub",
		level:                   "database",
		dbName:                  "testdb",
		tableName:               "testtable",
	}

	t.Run("Free", func(t *testing.T) {
		ic.Free() // Should not panic
	})

	t.Run("String", func(t *testing.T) {
		s := ic.String()
		assert.NotEmpty(t, s)
	})

	t.Run("StmtKind", func(t *testing.T) {
		kind := ic.StmtKind()
		expected := tree.MakeStmtKind(tree.OUTPUT_RESULT_ROW, tree.RESP_PREBUILD_RESULT_ROW, tree.EXEC_IN_FRONTEND)
		assert.Equal(t, expected, kind)
	})

	t.Run("GetStatementType", func(t *testing.T) {
		assert.Equal(t, "InternalCmd", ic.GetStatementType())
	})

	t.Run("GetQueryType", func(t *testing.T) {
		assert.Equal(t, tree.QueryTypeDQL, ic.GetQueryType())
	})
}

func TestInternalCmdGetObject(t *testing.T) {
	ic := &InternalCmdGetObject{
		subscriptionAccountName: "test_account",
		publicationName:         "test_pub",
		objectName:              "test_object",
		chunkIndex:              0,
	}

	t.Run("Free", func(t *testing.T) {
		ic.Free() // Should not panic
	})

	t.Run("String", func(t *testing.T) {
		s := ic.String()
		assert.NotEmpty(t, s)
	})

	t.Run("StmtKind", func(t *testing.T) {
		kind := ic.StmtKind()
		expected := tree.MakeStmtKind(tree.OUTPUT_RESULT_ROW, tree.RESP_PREBUILD_RESULT_ROW, tree.EXEC_IN_FRONTEND)
		assert.Equal(t, expected, kind)
	})

	t.Run("GetStatementType", func(t *testing.T) {
		assert.Equal(t, "InternalCmd", ic.GetStatementType())
	})

	t.Run("GetQueryType", func(t *testing.T) {
		assert.Equal(t, tree.QueryTypeDQL, ic.GetQueryType())
	})
}

func TestInternalCmdObjectList(t *testing.T) {
	ic := &InternalCmdObjectList{
		snapshotName:            "test_snapshot",
		againstSnapshotName:     "against_snapshot",
		subscriptionAccountName: "test_account",
		publicationName:         "test_pub",
	}

	t.Run("Free", func(t *testing.T) {
		ic.Free() // Should not panic
	})

	t.Run("String", func(t *testing.T) {
		s := ic.String()
		assert.NotEmpty(t, s)
	})

	t.Run("StmtKind", func(t *testing.T) {
		kind := ic.StmtKind()
		expected := tree.MakeStmtKind(tree.OUTPUT_RESULT_ROW, tree.RESP_PREBUILD_RESULT_ROW, tree.EXEC_IN_FRONTEND)
		assert.Equal(t, expected, kind)
	})

	t.Run("GetStatementType", func(t *testing.T) {
		assert.Equal(t, "InternalCmd", ic.GetStatementType())
	})

	t.Run("GetQueryType", func(t *testing.T) {
		assert.Equal(t, tree.QueryTypeDQL, ic.GetQueryType())
	})
}

func TestInternalCmdCheckSnapshotFlushed(t *testing.T) {
	ic := &InternalCmdCheckSnapshotFlushed{
		snapshotName:            "test_snapshot",
		subscriptionAccountName: "test_account",
		publicationName:         "test_pub",
	}

	t.Run("Free", func(t *testing.T) {
		ic.Free() // Should not panic
	})

	t.Run("String", func(t *testing.T) {
		s := ic.String()
		assert.NotEmpty(t, s)
	})

	t.Run("StmtKind", func(t *testing.T) {
		kind := ic.StmtKind()
		expected := tree.MakeStmtKind(tree.OUTPUT_RESULT_ROW, tree.RESP_PREBUILD_RESULT_ROW, tree.EXEC_IN_FRONTEND)
		assert.Equal(t, expected, kind)
	})

	t.Run("GetStatementType", func(t *testing.T) {
		assert.Equal(t, "InternalCmd", ic.GetStatementType())
	})

	t.Run("GetQueryType", func(t *testing.T) {
		assert.Equal(t, tree.QueryTypeDQL, ic.GetQueryType())
	})
}


func TestExecResultArrayHasData(t *testing.T) {
	t.Run("nil array", func(t *testing.T) {
		assert.False(t, execResultArrayHasData(nil))
	})

	t.Run("empty array", func(t *testing.T) {
		assert.False(t, execResultArrayHasData([]ExecResult{}))
	})
}

func TestUnknownStatementType(t *testing.T) {
	var ust unknownStatementType

	t.Run("GetStatementType", func(t *testing.T) {
		assert.Equal(t, "Unknown", ust.GetStatementType())
	})

	t.Run("GetQueryType", func(t *testing.T) {
		assert.Equal(t, tree.QueryTypeOth, ust.GetQueryType())
	})
}

func TestGetStatementType(t *testing.T) {
	t.Run("nil statement", func(t *testing.T) {
		result := getStatementType(nil)
		assert.NotNil(t, result)
	})

	t.Run("select statement", func(t *testing.T) {
		stmt := &tree.Select{}
		result := getStatementType(stmt)
		assert.NotNil(t, result)
	})

	t.Run("insert statement", func(t *testing.T) {
		stmt := &tree.Insert{}
		result := getStatementType(stmt)
		assert.NotNil(t, result)
	})

	t.Run("update statement", func(t *testing.T) {
		stmt := &tree.Update{}
		result := getStatementType(stmt)
		assert.NotNil(t, result)
	})

	t.Run("delete statement", func(t *testing.T) {
		stmt := &tree.Delete{}
		result := getStatementType(stmt)
		assert.NotNil(t, result)
	})
}

func TestNewSessionAllocator(t *testing.T) {
	t.Run("with valid parameter unit", func(t *testing.T) {
		pu := &config.ParameterUnit{
			SV: &config.FrontendParameters{
				GuestMmuLimitation: 1 << 30,
			},
		}
		allocator := NewSessionAllocator(pu)
		require.NotNil(t, allocator)
	})

	t.Run("nil parameter unit", func(t *testing.T) {
		allocator := NewSessionAllocator(nil)
		// Should handle nil gracefully
		assert.NotNil(t, allocator)
	})
}

func TestSessionAllocator_AllocAndFree(t *testing.T) {
	pu := &config.ParameterUnit{
		SV: &config.FrontendParameters{
			GuestMmuLimitation: 1 << 30,
		},
	}
	allocator := NewSessionAllocator(pu)
	require.NotNil(t, allocator)

	t.Run("alloc small", func(t *testing.T) {
		data, err := allocator.Alloc(100)
		require.NoError(t, err)
		assert.Len(t, data, 100)
		allocator.Free(data)
	})

	t.Run("alloc large", func(t *testing.T) {
		data, err := allocator.Alloc(1024 * 1024)
		require.NoError(t, err)
		assert.Len(t, data, 1024*1024)
		allocator.Free(data)
	})

	t.Run("alloc zero", func(t *testing.T) {
		data, err := allocator.Alloc(0)
		require.NoError(t, err)
		assert.Len(t, data, 0)
	})

	t.Run("free nil", func(t *testing.T) {
		allocator.Free(nil) // Should not panic
	})
}

func TestPrepareStmt_Close(t *testing.T) {
	ps := &PrepareStmt{
		Name: "test_stmt",
	}

	// Should not panic
	ps.Close()
}

func BenchmarkSessionAllocator(b *testing.B) {
	allocator := NewSessionAllocator(&config.ParameterUnit{
		SV: &config.FrontendParameters{
			GuestMmuLimitation: 1 << 30,
		},
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		slice, err := allocator.Alloc(i%(1<<20) + 1)
		if err != nil {
			b.Fatal(err)
		}
		allocator.Free(slice)
	}
}

func BenchmarkParallelSessionAllocator(b *testing.B) {
	allocator := NewSessionAllocator(&config.ParameterUnit{
		SV: &config.FrontendParameters{
			GuestMmuLimitation: 1 << 30,
		},
	})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			slice, err := allocator.Alloc(i%(32*(1<<10)) + 1)
			if err != nil {
				b.Fatal(err)
			}
			allocator.Free(slice)
		}
	})
}
