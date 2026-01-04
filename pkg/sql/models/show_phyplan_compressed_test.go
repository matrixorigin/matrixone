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

package models

import (
	"bytes"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// TestFormatColumns tests the formatColumns function with various scenarios
func TestFormatColumns(t *testing.T) {
	tests := []struct {
		name     string
		columns  []string
		expected string
	}{
		{
			name:     "empty columns",
			columns:  []string{},
			expected: "",
		},
		{
			name:     "nil columns",
			columns:  nil,
			expected: "",
		},
		{
			name:     "single column",
			columns:  []string{"col1"},
			expected: "[col1]",
		},
		{
			name:     "two columns",
			columns:  []string{"col1", "col2"},
			expected: "[col1 col2]",
		},
		{
			name:     "three columns",
			columns:  []string{"col1", "col2", "col3"},
			expected: "[col1 col2 col3]",
		},
		{
			name:     "four columns - should truncate",
			columns:  []string{"col1", "col2", "col3", "col4"},
			expected: "[col1 col2...+2]",
		},
		{
			name:     "many columns - should truncate",
			columns:  []string{"col1", "col2", "col3", "col4", "col5", "col6", "col7"},
			expected: "[col1 col2...+5]",
		},
		{
			name:     "columns with special characters",
			columns:  []string{"col_1", "col-2", "col.3"},
			expected: "[col_1 col-2 col.3]",
		},
		{
			name:     "columns with spaces",
			columns:  []string{"col 1", "col 2"},
			expected: "[col 1 col 2]",
		},
		{
			name:     "empty string columns",
			columns:  []string{"", ""},
			expected: "[ ]",
		},
		{
			name:     "single empty string column",
			columns:  []string{""},
			expected: "[]",
		},
		{
			name:     "columns with unicode characters",
			columns:  []string{"列1", "列2"},
			expected: "[列1 列2]",
		},
		{
			name:     "very long column names",
			columns:  []string{"very_long_column_name_1", "very_long_column_name_2", "very_long_column_name_3", "very_long_column_name_4"},
			expected: "[very_long_column_name_1 very_long_column_name_2...+2]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatColumns(tt.columns)
			if result != tt.expected {
				t.Errorf("formatColumns() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// TestExplainOperatorChainCompressed tests the explainOperatorChainCompressed function
func TestExplainOperatorChainCompressed(t *testing.T) {
	tests := []struct {
		name        string
		operator    *PhyOperator
		contains    []string // strings that should be present in output
		notContains []string // strings that should NOT be present in output
	}{
		{
			name:        "nil operator",
			operator:    nil,
			contains:    []string{},
			notContains: []string{},
		},
		{
			name:        "operator without stats",
			operator:    &PhyOperator{OpName: "TableScan", NodeIdx: 0},
			contains:    []string{"TableScan"},
			notContains: []string{},
		},
		{
			name: "operator with stats - all metrics below threshold",
			operator: &PhyOperator{
				OpName:  "Filter",
				NodeIdx: 0,
				OpStats: &process.OperatorStats{
					CallNum:      0,
					TimeConsumed: 500000, // < 1ms threshold
					InputRows:    0,
					InputSize:    512, // < 1KB threshold
				},
			},
			contains:    []string{"Filter"},
			notContains: []string{"Calls:", "Time:", "Rows:", "Size:"},
		},
		{
			name: "operator with significant stats",
			operator: &PhyOperator{
				OpName:  "TableScan",
				NodeIdx: 0,
				OpStats: &process.OperatorStats{
					CallNum:      10,
					TimeConsumed: 2000000, // > 1ms
					InputRows:    1000,
					OutputRows:   950,
					InputSize:    2048, // > 1KB
				},
			},
			contains:    []string{"TableScan", "Calls:10", "Time:", "Rows:1000→950", "Size:"},
			notContains: []string{},
		},
		{
			name: "operator chain with children",
			operator: &PhyOperator{
				OpName:  "Projection",
				NodeIdx: 0,
				OpStats: &process.OperatorStats{
					CallNum:      5,
					TimeConsumed: 1500000,
					InputRows:    500,
					OutputRows:   500,
				},
				Children: []*PhyOperator{
					{
						OpName:  "Filter",
						NodeIdx: 0,
						OpStats: &process.OperatorStats{
							CallNum:      5,
							TimeConsumed: 1000000,
							InputRows:    1000,
							OutputRows:   500,
						},
					},
				},
			},
			contains:    []string{"Projection", "Filter", "→"},
			notContains: []string{},
		},
		{
			name: "operator with only CallNum significant",
			operator: &PhyOperator{
				OpName:  "Group",
				NodeIdx: 1,
				OpStats: &process.OperatorStats{
					CallNum:      20,
					TimeConsumed: 500000, // below threshold
					InputRows:    0,
					InputSize:    512, // below threshold
				},
			},
			contains:    []string{"Group", "Calls:20"},
			notContains: []string{"Time:", "Rows:", "Size:"},
		},
		{
			name: "operator with only Time significant",
			operator: &PhyOperator{
				OpName:  "Join",
				NodeIdx: 2,
				OpStats: &process.OperatorStats{
					CallNum:      0,
					TimeConsumed: 5000000, // > 1ms
					InputRows:    0,
					InputSize:    512, // below threshold
				},
			},
			contains:    []string{"Join", "Time:"},
			notContains: []string{"Calls:", "Rows:", "Size:"},
		},
		{
			name: "operator with only Rows significant",
			operator: &PhyOperator{
				OpName:  "Sort",
				NodeIdx: 3,
				OpStats: &process.OperatorStats{
					CallNum:      0,
					TimeConsumed: 500000, // below threshold
					InputRows:    5000,
					OutputRows:   5000,
					InputSize:    512, // below threshold
				},
			},
			contains:    []string{"Sort", "Rows:5000→5000"},
			notContains: []string{"Calls:", "Time:", "Size:"},
		},
		{
			name: "operator with only Size significant",
			operator: &PhyOperator{
				OpName:  "Merge",
				NodeIdx: 4,
				OpStats: &process.OperatorStats{
					CallNum:      0,
					TimeConsumed: 500000, // below threshold
					InputRows:    0,
					InputSize:    2048, // > 1KB
				},
			},
			contains:    []string{"Merge", "Size:"},
			notContains: []string{"Calls:", "Time:", "Rows:"},
		},
		{
			name: "deep operator chain",
			operator: &PhyOperator{
				OpName:  "Output",
				NodeIdx: -1,
				Children: []*PhyOperator{
					{
						OpName:  "Projection",
						NodeIdx: 0,
						Children: []*PhyOperator{
							{
								OpName:  "Filter",
								NodeIdx: 0,
							},
						},
					},
				},
			},
			contains:    []string{"Output", "→", "Projection", "→", "Filter"},
			notContains: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := bytes.NewBuffer(make([]byte, 0, 100))
			explainOperatorChainCompressed(tt.operator, buffer)
			result := buffer.String()

			// Check for expected strings
			for _, expected := range tt.contains {
				if !strings.Contains(result, expected) {
					t.Errorf("explainOperatorChainCompressed() output should contain %q, got: %q", expected, result)
				}
			}

			// Check for strings that should NOT be present
			for _, notExpected := range tt.notContains {
				if strings.Contains(result, notExpected) {
					t.Errorf("explainOperatorChainCompressed() output should NOT contain %q, got: %q", notExpected, result)
				}
			}
		})
	}
}

// TestExplainPhyScopeCompressed tests the explainPhyScopeCompressed function
func TestExplainPhyScopeCompressed(t *testing.T) {
	tests := []struct {
		name        string
		scope       PhyScope
		scopeIdx    int
		depth       int
		contains    []string
		notContains []string
	}{
		{
			name: "basic scope without datasource",
			scope: PhyScope{
				Magic: "Normal",
				Mcpu:  4,
			},
			scopeIdx:    0,
			depth:       0,
			contains:    []string{"Scope 1", "Normal", "mcpu: 4"},
			notContains: []string{},
		},
		{
			name: "scope with receiver",
			scope: PhyScope{
				Magic: "Merge",
				Mcpu:  8,
				Receiver: []PhyReceiver{
					{Idx: 0, RemoteUuid: ""},
					{Idx: 1, RemoteUuid: "uuid-123"},
				},
			},
			scopeIdx:    1,
			depth:       0,
			contains:    []string{"Scope 2", "Merge", "mcpu: 8", "Receiver:"},
			notContains: []string{},
		},
		{
			name: "scope with datasource",
			scope: PhyScope{
				Magic: "Normal",
				Mcpu:  2,
				DataSource: &PhySource{
					SchemaName:   "test_schema",
					RelationName: "test_table",
					Attributes:   []string{"col1", "col2"},
				},
			},
			scopeIdx:    0,
			depth:       0,
			contains:    []string{"Scope 1", "DataSource:", "test_schema", "test_table", "[col1 col2]"},
			notContains: []string{},
		},
		{
			name: "scope with datasource - many columns",
			scope: PhyScope{
				Magic: "Normal",
				Mcpu:  2,
				DataSource: &PhySource{
					SchemaName:   "schema",
					RelationName: "table",
					Attributes:   []string{"col1", "col2", "col3", "col4", "col5"},
				},
			},
			scopeIdx:    0,
			depth:       0,
			contains:    []string{"DataSource:", "[col1 col2...+3]"},
			notContains: []string{},
		},
		{
			name: "scope with root operator",
			scope: PhyScope{
				Magic: "Normal",
				Mcpu:  4,
				RootOperator: &PhyOperator{
					OpName:  "TableScan",
					NodeIdx: 0,
					OpStats: &process.OperatorStats{
						CallNum:      10,
						TimeConsumed: 2000000,
						InputRows:    1000,
						OutputRows:   950,
					},
				},
			},
			scopeIdx:    0,
			depth:       0,
			contains:    []string{"Scope 1", "Pipeline:", "TableScan"},
			notContains: []string{},
		},
		{
			name: "scope with prescopes",
			scope: PhyScope{
				Magic: "Normal",
				Mcpu:  4,
				PreScopes: []PhyScope{
					{
						Magic: "Merge",
						Mcpu:  2,
					},
					{
						Magic: "Normal",
						Mcpu:  2,
					},
				},
			},
			scopeIdx:    0,
			depth:       0,
			contains:    []string{"Scope 1", "PreScopes: 2 scope(s)", "Scope 1", "Scope 2"},
			notContains: []string{},
		},
		{
			name: "scope with nested prescopes",
			scope: PhyScope{
				Magic: "Normal",
				Mcpu:  4,
				PreScopes: []PhyScope{
					{
						Magic: "Merge",
						Mcpu:  2,
						PreScopes: []PhyScope{
							{
								Magic: "Normal",
								Mcpu:  1,
							},
						},
					},
				},
			},
			scopeIdx:    0,
			depth:       0,
			contains:    []string{"PreScopes: 1 scope(s)", "Scope 1"},
			notContains: []string{},
		},
		{
			name: "scope with empty datasource schema",
			scope: PhyScope{
				Magic: "Normal",
				Mcpu:  4,
				DataSource: &PhySource{
					SchemaName:   "",
					RelationName: "table",
					Attributes:   []string{"col1"},
				},
			},
			scopeIdx:    0,
			depth:       0,
			notContains: []string{"DataSource:"},
		},
		{
			name: "scope with indentation at depth 2",
			scope: PhyScope{
				Magic: "Normal",
				Mcpu:  4,
			},
			scopeIdx:    0,
			depth:       2,
			contains:    []string{"Scope 1"},
			notContains: []string{},
		},
		{
			name: "complete scope with all fields",
			scope: PhyScope{
				Magic: "Merge",
				Mcpu:  8,
				Receiver: []PhyReceiver{
					{Idx: 0, RemoteUuid: ""},
				},
				DataSource: &PhySource{
					SchemaName:   "db",
					RelationName: "users",
					Attributes:   []string{"id", "name", "email"},
				},
				RootOperator: &PhyOperator{
					OpName:  "TableScan",
					NodeIdx: 0,
					OpStats: &process.OperatorStats{
						CallNum:      5,
						TimeConsumed: 1500000,
						InputRows:    100,
						OutputRows:   100,
					},
				},
				PreScopes: []PhyScope{
					{
						Magic: "Normal",
						Mcpu:  4,
					},
				},
			},
			scopeIdx:    0,
			depth:       0,
			contains:    []string{"Scope 1", "Merge", "mcpu: 8", "Receiver:", "DataSource:", "db", "users", "[id name email]", "Pipeline:", "TableScan", "PreScopes: 1 scope(s)"},
			notContains: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := bytes.NewBuffer(make([]byte, 0, 200))
			explainPhyScopeCompressed(tt.scope, tt.scopeIdx, tt.depth, buffer)
			result := buffer.String()

			// Check for expected strings
			for _, expected := range tt.contains {
				if !strings.Contains(result, expected) {
					t.Errorf("explainPhyScopeCompressed() output should contain %q, got: %q", expected, result)
				}
			}

			// Check for strings that should NOT be present
			for _, notExpected := range tt.notContains {
				if strings.Contains(result, notExpected) {
					t.Errorf("explainPhyScopeCompressed() output should NOT contain %q, got: %q", notExpected, result)
				}
			}
		})
	}
}

// TestExplainPhyPlanCompressed tests the ExplainPhyPlanCompressed function
func TestExplainPhyPlanCompressed(t *testing.T) {
	// Create test data
	operatorStats := &process.OperatorStats{
		CallNum:      10,
		TimeConsumed: 2000000,
		InputRows:    1000,
		OutputRows:   950,
		InputSize:    2048,
	}

	rootOp := &PhyOperator{
		OpName:  "TableScan",
		NodeIdx: 0,
		OpStats: operatorStats,
	}

	scopeWithDataSource := PhyScope{
		Magic: "Normal",
		Mcpu:  4,
		DataSource: &PhySource{
			SchemaName:   "test_schema",
			RelationName: "test_table",
			Attributes:   []string{"col1", "col2", "col3"},
		},
		RootOperator: rootOp,
	}

	scopeWithReceiver := PhyScope{
		Magic: "Merge",
		Mcpu:  8,
		Receiver: []PhyReceiver{
			{Idx: 0, RemoteUuid: ""},
		},
		RootOperator: &PhyOperator{
			OpName:   "Projection",
			NodeIdx:  1,
			OpStats:  operatorStats,
			Children: []*PhyOperator{rootOp},
		},
	}

	scopeWithPreScopes := PhyScope{
		Magic: "Normal",
		Mcpu:  4,
		PreScopes: []PhyScope{
			scopeWithDataSource,
		},
		RootOperator: &PhyOperator{
			OpName:  "Output",
			NodeIdx: -1,
		},
	}

	tests := []struct {
		name        string
		plan        *PhyPlan
		statsInfo   *statistic.StatsInfo
		option      ExplainOption
		contains    []string
		notContains []string
	}{
		{
			name:        "empty plan",
			plan:        NewPhyPlan(),
			statsInfo:   nil,
			option:      NormalOption,
			contains:    []string{"Physical Plan Deployment (Compressed):"},
			notContains: []string{"LOCAL SCOPES:", "REMOTE SCOPES:"},
		},
		{
			name: "plan with local scope only",
			plan: &PhyPlan{
				Version:    "1.0",
				RetryTime:  0,
				LocalScope: []PhyScope{scopeWithDataSource},
			},
			statsInfo:   nil,
			option:      NormalOption,
			contains:    []string{"LOCAL SCOPES:", "Scope 1", "test_schema", "test_table", "TableScan"},
			notContains: []string{},
		},
		{
			name: "plan with remote scope only",
			plan: &PhyPlan{
				Version:     "1.0",
				RetryTime:   0,
				RemoteScope: []PhyScope{scopeWithReceiver},
			},
			statsInfo:   nil,
			option:      NormalOption,
			contains:    []string{"REMOTE SCOPES:", "Scope 1", "Merge", "Projection"},
			notContains: []string{},
		},
		{
			name: "plan with both local and remote scopes",
			plan: &PhyPlan{
				Version:     "1.0",
				RetryTime:   0,
				LocalScope:  []PhyScope{scopeWithDataSource},
				RemoteScope: []PhyScope{scopeWithReceiver},
			},
			statsInfo:   nil,
			option:      NormalOption,
			contains:    []string{"LOCAL SCOPES:", "REMOTE SCOPES:", "Scope 1"},
			notContains: []string{},
		},
		{
			name: "plan with prescopes",
			plan: &PhyPlan{
				Version:    "1.0",
				RetryTime:  0,
				LocalScope: []PhyScope{scopeWithPreScopes},
			},
			statsInfo:   nil,
			option:      NormalOption,
			contains:    []string{"LOCAL SCOPES:", "PreScopes: 1 scope(s)", "test_schema", "test_table"},
			notContains: []string{},
		},
		{
			name: "plan with multiple local scopes",
			plan: &PhyPlan{
				Version:    "1.0",
				RetryTime:  0,
				LocalScope: []PhyScope{scopeWithDataSource, scopeWithReceiver},
			},
			statsInfo:   nil,
			option:      NormalOption,
			contains:    []string{"LOCAL SCOPES:", "Scope 1", "Scope 2"},
			notContains: []string{},
		},
		{
			name: "plan with stats info and verbose option",
			plan: &PhyPlan{
				Version:    "1.0",
				RetryTime:  1,
				LocalScope: []PhyScope{scopeWithDataSource},
			},
			statsInfo: func() *statistic.StatsInfo {
				s := &statistic.StatsInfo{}
				s.ParseStage.ParseDuration = 1000000
				s.PlanStage.PlanDuration = 2000000
				s.CompileStage.CompileDuration = 500000
				return s
			}(),
			option:      VerboseOption,
			contains:    []string{"Overview:", "LOCAL SCOPES:", "Physical Plan Deployment"},
			notContains: []string{},
		},
		{
			name: "plan with stats info and analyze option",
			plan: &PhyPlan{
				Version:    "1.0",
				RetryTime:  2,
				LocalScope: []PhyScope{scopeWithDataSource},
			},
			statsInfo: func() *statistic.StatsInfo {
				s := &statistic.StatsInfo{}
				s.ParseStage.ParseDuration = 1000000
				s.PlanStage.PlanDuration = 2000000
				s.CompileStage.CompileDuration = 500000
				return s
			}(),
			option:      AnalyzeOption,
			contains:    []string{"Overview:", "LOCAL SCOPES:", "Physical Plan Deployment"},
			notContains: []string{},
		},
		{
			name: "plan with scope containing many columns",
			plan: &PhyPlan{
				Version:   "1.0",
				RetryTime: 0,
				LocalScope: []PhyScope{
					{
						Magic: "Normal",
						Mcpu:  4,
						DataSource: &PhySource{
							SchemaName:   "schema",
							RelationName: "table",
							Attributes:   []string{"col1", "col2", "col3", "col4", "col5", "col6"},
						},
					},
				},
			},
			statsInfo:   nil,
			option:      NormalOption,
			contains:    []string{"[col1 col2...+4]"},
			notContains: []string{},
		},
		{
			name: "plan with operator chain",
			plan: &PhyPlan{
				Version:   "1.0",
				RetryTime: 0,
				LocalScope: []PhyScope{
					{
						Magic: "Normal",
						Mcpu:  4,
						RootOperator: &PhyOperator{
							OpName:  "Output",
							NodeIdx: -1,
							Children: []*PhyOperator{
								{
									OpName:  "Projection",
									NodeIdx: 0,
									OpStats: &process.OperatorStats{
										CallNum:      5,
										TimeConsumed: 1500000,
										InputRows:    100,
										OutputRows:   100,
									},
									Children: []*PhyOperator{
										{
											OpName:  "Filter",
											NodeIdx: 0,
											OpStats: &process.OperatorStats{
												CallNum:      5,
												TimeConsumed: 1000000,
												InputRows:    200,
												OutputRows:   100,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			statsInfo:   nil,
			option:      NormalOption,
			contains:    []string{"Output", "→", "Projection", "→", "Filter"},
			notContains: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExplainPhyPlanCompressed(tt.plan, tt.statsInfo, tt.option)

			// Check for expected strings
			for _, expected := range tt.contains {
				if !strings.Contains(result, expected) {
					t.Errorf("ExplainPhyPlanCompressed() output should contain %q, got: %q", expected, result)
				}
			}

			// Check for strings that should NOT be present
			for _, notExpected := range tt.notContains {
				if strings.Contains(result, notExpected) {
					t.Errorf("ExplainPhyPlanCompressed() output should NOT contain %q, got: %q", notExpected, result)
				}
			}

			// Verify that the result always contains the compressed header
			if !strings.Contains(result, "Physical Plan Deployment (Compressed):") {
				t.Error("ExplainPhyPlanCompressed() output should always contain 'Physical Plan Deployment (Compressed):'")
			}
		})
	}
}

// TestExplainPhyPlanCompressedEdgeCases tests edge cases and boundary conditions
func TestExplainPhyPlanCompressedEdgeCases(t *testing.T) {
	t.Run("nil plan", func(t *testing.T) {
		// Note: The function doesn't handle nil plan, so we skip this test
		// or test that it panics as expected
		defer func() {
			if r := recover(); r == nil {
				t.Error("ExplainPhyPlanCompressed() should panic on nil plan")
			}
		}()
		ExplainPhyPlanCompressed(nil, nil, NormalOption)
	})

	t.Run("plan with empty scopes", func(t *testing.T) {
		plan := &PhyPlan{
			Version:     "1.0",
			LocalScope:  []PhyScope{},
			RemoteScope: []PhyScope{},
		}
		result := ExplainPhyPlanCompressed(plan, nil, NormalOption)
		if !strings.Contains(result, "Physical Plan Deployment (Compressed):") {
			t.Error("ExplainPhyPlanCompressed() should output header even with empty scopes")
		}
	})

	t.Run("scope with nil root operator", func(t *testing.T) {
		plan := &PhyPlan{
			Version: "1.0",
			LocalScope: []PhyScope{
				{
					Magic:        "Normal",
					Mcpu:         4,
					RootOperator: nil,
				},
			},
		}
		result := ExplainPhyPlanCompressed(plan, nil, NormalOption)
		if !strings.Contains(result, "Scope 1") {
			t.Error("ExplainPhyPlanCompressed() should handle nil root operator")
		}
		if strings.Contains(result, "Pipeline:") {
			t.Error("ExplainPhyPlanCompressed() should not output Pipeline when root operator is nil")
		}
	})

	t.Run("scope with empty prescopes", func(t *testing.T) {
		plan := &PhyPlan{
			Version: "1.0",
			LocalScope: []PhyScope{
				{
					Magic:     "Normal",
					Mcpu:      4,
					PreScopes: []PhyScope{},
				},
			},
		}
		result := ExplainPhyPlanCompressed(plan, nil, NormalOption)
		if strings.Contains(result, "PreScopes:") {
			t.Error("ExplainPhyPlanCompressed() should not output PreScopes when empty")
		}
	})

	t.Run("scope with nil datasource", func(t *testing.T) {
		plan := &PhyPlan{
			Version: "1.0",
			LocalScope: []PhyScope{
				{
					Magic:      "Normal",
					Mcpu:       4,
					DataSource: nil,
				},
			},
		}
		result := ExplainPhyPlanCompressed(plan, nil, NormalOption)
		if !strings.Contains(result, "Scope 1") {
			t.Error("ExplainPhyPlanCompressed() should handle nil datasource")
		}
		if strings.Contains(result, "DataSource:") {
			t.Error("ExplainPhyPlanCompressed() should not output DataSource when nil")
		}
	})

	t.Run("scope with empty receiver", func(t *testing.T) {
		plan := &PhyPlan{
			Version: "1.0",
			LocalScope: []PhyScope{
				{
					Magic:    "Normal",
					Mcpu:     4,
					Receiver: []PhyReceiver{},
				},
			},
		}
		result := ExplainPhyPlanCompressed(plan, nil, NormalOption)
		if !strings.Contains(result, "Scope 1") {
			t.Error("ExplainPhyPlanCompressed() should handle empty receiver")
		}
	})

	t.Run("operator with multiple children - only first child shown", func(t *testing.T) {
		plan := &PhyPlan{
			Version: "1.0",
			LocalScope: []PhyScope{
				{
					Magic: "Normal",
					Mcpu:  4,
					RootOperator: &PhyOperator{
						OpName:  "Union",
						NodeIdx: 0,
						Children: []*PhyOperator{
							{
								OpName:  "TableScan1",
								NodeIdx: 0,
							},
							{
								OpName:  "TableScan2",
								NodeIdx: 1,
							},
						},
					},
				},
			},
		}
		result := ExplainPhyPlanCompressed(plan, nil, NormalOption)
		if !strings.Contains(result, "Union") {
			t.Error("ExplainPhyPlanCompressed() should show root operator")
		}
		if !strings.Contains(result, "TableScan1") {
			t.Error("ExplainPhyPlanCompressed() should show first child")
		}
		// Note: The compressed version only shows the first child, so TableScan2 might not appear
	})

	t.Run("operator with stats at exact threshold values", func(t *testing.T) {
		plan := &PhyPlan{
			Version: "1.0",
			LocalScope: []PhyScope{
				{
					Magic: "Normal",
					Mcpu:  4,
					RootOperator: &PhyOperator{
						OpName:  "TableScan",
						NodeIdx: 0,
						OpStats: &process.OperatorStats{
							TimeConsumed: 1000000, // exactly 1ms threshold
							InputSize:    1024,    // exactly 1KB threshold
						},
					},
				},
			},
		}
		result := ExplainPhyPlanCompressed(plan, nil, NormalOption)
		if !strings.Contains(result, "TableScan") {
			t.Error("ExplainPhyPlanCompressed() should show operator name")
		}
		// Time and Size should not appear as they are at threshold (not > threshold)
		if strings.Contains(result, "Time:") {
			t.Error("ExplainPhyPlanCompressed() should not show Time when exactly at threshold")
		}
		if strings.Contains(result, "Size:") {
			t.Error("ExplainPhyPlanCompressed() should not show Size when exactly at threshold")
		}
	})

	t.Run("operator with stats just above threshold", func(t *testing.T) {
		plan := &PhyPlan{
			Version: "1.0",
			LocalScope: []PhyScope{
				{
					Magic: "Normal",
					Mcpu:  4,
					RootOperator: &PhyOperator{
						OpName:  "TableScan",
						NodeIdx: 0,
						OpStats: &process.OperatorStats{
							TimeConsumed: 1000001, // just above 1ms threshold
							InputSize:    1025,    // just above 1KB threshold
						},
					},
				},
			},
		}
		result := ExplainPhyPlanCompressed(plan, nil, NormalOption)
		if !strings.Contains(result, "TableScan") {
			t.Error("ExplainPhyPlanCompressed() should show operator name")
		}
		if !strings.Contains(result, "Time:") {
			t.Error("ExplainPhyPlanCompressed() should show Time when just above threshold")
		}
		if !strings.Contains(result, "Size:") {
			t.Error("ExplainPhyPlanCompressed() should show Size when just above threshold")
		}
	})
}
