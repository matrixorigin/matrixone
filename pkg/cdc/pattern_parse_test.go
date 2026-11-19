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

package cdc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPatternParserPartialFailure(t *testing.T) {
	ctx := context.Background()
	dup := make(map[string]struct{})
	tuple, err := CDCParseGranularityTuple(ctx, CDCPitrGranularity_Table, "db.t:bad", dup)
	require.ErrorContains(t, err, "invalid tables format")
	require.Equal(t, PatternTable{Database: "db", Table: "t"}, tuple.Source)
	require.Contains(t, dup, GenDbTblKey("db", "t"))
	duplicate, err := CDCParseGranularityTuple(ctx, CDCPitrGranularity_Table, "db.t:db.u", dup)
	require.ErrorContains(t, err, "can't be used as multi sources")
	require.Equal(t, PatternTable{Database: "db", Table: "t"}, duplicate.Source)
	require.Equal(t, PatternTable{}, duplicate.Sink)
	pts, err := CDCParsePitrGranularity(ctx, CDCPitrGranularity_Table, "db.t,db.u:bad")
	require.ErrorContains(t, err, "invalid tables format")
	require.Len(t, pts.Pts, 1)
	require.Equal(t, PatternTable{Database: "db", Table: "t"}, pts.Pts[0].Source)
	require.Equal(t, pts.Pts[0].Source, pts.Pts[0].Sink)
}

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
