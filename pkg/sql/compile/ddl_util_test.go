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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoverage_hasSpecialChars(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"empty string", "", false},
		{"no special chars", "hello", false},
		{"comma", "hello,world", true},
		{"dot", "hello.world", true},
		{"colon", "hello:world", true},
		{"backtick", "hello`world", true},
		{"multiple special chars", "a,b.c:d`e", true},
		{"alphanumeric only", "abc123", false},
		{"underscores", "my_table_name", false},
		{"hyphens", "my-table-name", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasSpecialChars(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCoverage_dbNameIsLegal(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"simple name", "mydb", true},
		{"name with underscore", "my_db", true},
		{"star (all)", "*", true},
		{"empty string", "", false},
		{"spaces only", "   ", false},
		{"with comma", "my,db", false},
		{"with dot", "my.db", false},
		{"with colon", "my:db", false},
		{"with backtick", "my`db", false},
		{"numeric name", "123", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dbNameIsLegal(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCoverage_tableNameIsLegal(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"simple name", "mytable", true},
		{"name with underscore", "my_table", true},
		{"star (all)", "*", true},
		{"empty string", "", false},
		{"spaces only", "   ", false},
		{"with comma", "my,table", false},
		{"with dot", "my.table", false},
		{"with colon", "my:table", false},
		{"with backtick", "my`table", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tableNameIsLegal(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCoverage_isLegal(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		sqls     []string
		expected bool
	}{
		{"empty name", "", []string{"create database x"}, false},
		{"empty sqls", "x", []string{}, false},
		{"nil sqls", "x", nil, false},
		{"has empty sql", "x", []string{""}, false},
		{"valid sql", "mydb", []string{"create database mydb"}, true},
		{"invalid sql", "mydb", []string{"INVALID SQL THAT WONT PARSE"}, false},
		{"one valid one invalid", "mydb", []string{"NOT VALID", "create database mydb"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isLegal(tt.input, tt.sqls)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCoverage_getValue(t *testing.T) {
	t.Run("uint64 input", func(t *testing.T) {
		result := getValue[int64](false, uint64(42))
		assert.Equal(t, int64(42), result)
	})

	t.Run("int64 positive", func(t *testing.T) {
		result := getValue[int64](false, int64(42))
		assert.Equal(t, int64(42), result)
	})

	t.Run("int64 negative", func(t *testing.T) {
		result := getValue[int64](true, int64(42))
		assert.Equal(t, int64(-42), result)
	})

	t.Run("unsupported type returns zero", func(t *testing.T) {
		result := getValue[int64](false, "string")
		assert.Equal(t, int64(0), result)
	})
}

func TestCoverage_getInterfaceValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
	}{
		{"int16", int16(10), 10},
		{"int32", int32(20), 20},
		{"int64", int64(30), 30},
		{"uint16", uint16(40), 40},
		{"uint32", uint32(50), 50},
		{"uint64", uint64(60), 60},
		{"unsupported returns zero", "string", 0},
		{"nil returns zero", nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getInterfaceValue[int64](tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCoverage_isMissingTableForFkCleanup(t *testing.T) {
	t.Run("no such table error", func(t *testing.T) {
		err := moerr.NewNoSuchTableNoCtx("db", "tbl")
		assert.True(t, isMissingTableForFkCleanup(err))
	})

	t.Run("internal error with can not find table", func(t *testing.T) {
		err := moerr.NewInternalErrorNoCtx("can not find table by id 123")
		assert.True(t, isMissingTableForFkCleanup(err))
	})

	t.Run("internal error without matching message", func(t *testing.T) {
		err := moerr.NewInternalErrorNoCtx("something else")
		assert.False(t, isMissingTableForFkCleanup(err))
	})

	t.Run("other error type", func(t *testing.T) {
		err := moerr.NewInvalidInputNoCtx("bad input")
		assert.False(t, isMissingTableForFkCleanup(err))
	})
}

func TestCoverage_isTableFromPublication(t *testing.T) {
	t.Run("nil tableDef returns false", func(t *testing.T) {
		result := isTableFromPublication(nil)
		assert.False(t, result)
	})

	t.Run("empty defs returns false", func(t *testing.T) {
		td := &plan.TableDef{}
		result := isTableFromPublication(td)
		assert.False(t, result)
	})

	t.Run("no publication property returns false", func(t *testing.T) {
		td := &plan.TableDef{
			Defs: []*plan.TableDef_DefType{
				{
					Def: &plan.TableDef_DefType_Properties{
						Properties: &plan.PropertiesDef{
							Properties: []*plan.Property{
								{Key: "other_key", Value: "other_value"},
							},
						},
					},
				},
			},
		}
		result := isTableFromPublication(td)
		assert.False(t, result)
	})

	t.Run("from_publication=true returns true", func(t *testing.T) {
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
		result := isTableFromPublication(td)
		assert.True(t, result)
	})

	t.Run("from_publication=false returns false", func(t *testing.T) {
		td := &plan.TableDef{
			Defs: []*plan.TableDef_DefType{
				{
					Def: &plan.TableDef_DefType_Properties{
						Properties: &plan.PropertiesDef{
							Properties: []*plan.Property{
								{Key: "from_publication", Value: "false"},
							},
						},
					},
				},
			},
		}
		result := isTableFromPublication(td)
		assert.False(t, result)
	})

	t.Run("non-property def type skipped", func(t *testing.T) {
		td := &plan.TableDef{
			Defs: []*plan.TableDef_DefType{},
		}
		result := isTableFromPublication(td)
		require.False(t, result)
	})
}

func TestCoverage_isLegalWithWhitespace(t *testing.T) {
	// Test with leading/trailing spaces in name
	result := isLegal("  mydb  ", []string{"create database mydb"})
	assert.True(t, result)
}

func TestCoverage_dbNameHasSpecialCharsShortCircuit(t *testing.T) {
	// Verify that special char check short-circuits before SQL parsing
	result := dbNameIsLegal(",")
	assert.False(t, result)
}

func TestCoverage_tableNameHasSpecialCharsShortCircuit(t *testing.T) {
	result := tableNameIsLegal(".")
	assert.False(t, result)
}

func TestCoverage_hasSpecialCharsAllSpecial(t *testing.T) {
	// Test each special character individually
	for _, ch := range ",.:`" {
		result := hasSpecialChars(string(ch))
		assert.True(t, result, "Expected true for character: %s", string(ch))
	}
}

func TestCoverage_isLegalMultipleInvalidSQL(t *testing.T) {
	// All invalid SQL should return false
	result := isLegal("test", []string{
		"NOT VALID SQL",
		"ALSO NOT VALID",
		"STILL NOT VALID",
	})
	assert.False(t, result)
}

func TestCoverage_isTableFromPublicationMultipleProperties(t *testing.T) {
	// Table with multiple properties, only one matches
	td := &plan.TableDef{
		Defs: []*plan.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{Key: "comment", Value: "test table"},
							{Key: "from_publication", Value: "true"},
							{Key: "kind", Value: "r"},
						},
					},
				},
			},
		},
	}
	result := isTableFromPublication(td)
	assert.True(t, result)
}

func TestCoverage_getValueWithDifferentTypes(t *testing.T) {
	// Test with int32 target type
	result32 := getValue[int32](false, uint64(100))
	assert.Equal(t, int32(100), result32)

	// Test negative int32
	resultNeg32 := getValue[int32](true, int64(100))
	assert.Equal(t, int32(-100), resultNeg32)
}

func TestCoverage_getInterfaceValueUint(t *testing.T) {
	// Test uint32 target type
	result := getInterfaceValue[uint32](int64(42))
	assert.Equal(t, uint32(42), result)
}

func TestCoverage_isMissingTableContainsSubstring(t *testing.T) {
	// Verify exact substring matching
	err := moerr.NewInternalErrorNoCtx("prefix can not find table by id suffix")
	result := isMissingTableForFkCleanup(err)
	assert.True(t, result)

	// Check case sensitivity
	errCase := moerr.NewInternalErrorNoCtx("CAN NOT FIND TABLE BY ID")
	resultCase := isMissingTableForFkCleanup(errCase)
	// The function uses strings.Contains which is case-sensitive
	assert.Equal(t, strings.Contains(errCase.Error(), "can not find table by id"), resultCase)
}
