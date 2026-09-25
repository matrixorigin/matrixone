// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mvdefinition

import (
	"encoding/base64"
	"encoding/json"
	"slices"
	"strings"
)

type Aggregate struct {
	Kind             string `json:"kind"`
	InputExpression  string `json:"input_expression,omitempty"`
	OutputColumn     string `json:"output_column"`
	StateSumColumn   string `json:"state_sum_column,omitempty"`
	StateCountColumn string `json:"state_count_column,omitempty"`
	StateIndex       int    `json:"state_index,omitempty"`
}

type Group struct {
	Expression   string `json:"expression"`
	OutputColumn string `json:"output_column"`
	NotNullable  bool   `json:"not_nullable,omitempty"`
}

type Incremental struct {
	Version        int         `json:"version,omitempty"`
	Strategy       string      `json:"strategy,omitempty"`
	SourceAlias    string      `json:"source_alias"`
	SourceColumns  []string    `json:"source_columns"`
	Filter         string      `json:"filter,omitempty"`
	Having         string      `json:"having,omitempty"`
	Groups         []Group     `json:"groups"`
	Aggregates     []Aggregate `json:"aggregates"`
	GroupKeyColumn string      `json:"group_key_column,omitempty"`
	RowCountColumn string      `json:"row_count_column"`
	StateColumns   []string    `json:"state_columns"`
	StateTable     string      `json:"state_table,omitempty"`
	BranchID       int         `json:"branch_id,omitempty"`
	SourceDatabase string      `json:"source_database,omitempty"`
	SourceTable    string      `json:"source_table,omitempty"`
	Branches       []Branch    `json:"branches,omitempty"`
}

type Branch struct {
	Description *Incremental `json:"description"`
}

func DecodeIncremental(encoded string) (*Incremental, error) {
	if len(encoded) > base64.StdEncoding.EncodedLen(MaxDefinitionBytes) {
		return nil, Invalid("materialized view definition exceeds size limit")
	}
	b, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, Invalid("invalid materialized view incremental specification encoding: %v", err)
	}
	var desc Incremental
	if err := json.Unmarshal(b, &desc); err != nil {
		return nil, Invalid("invalid materialized view incremental specification: %v", err)
	}
	if desc.Version < 2 || desc.Version > 3 {
		return nil, Invalid("unsupported materialized view incremental specification version %d", desc.Version)
	}
	if err := validateIncremental(&desc, false); err != nil {
		return nil, err
	}
	return &desc, nil
}

func validateIncremental(desc *Incremental, nested bool) error {
	if desc == nil || desc.Version < 2 || desc.Version > 3 {
		return Invalid("incomplete materialized view incremental specification")
	}
	if desc.Strategy == "union-all" {
		if nested || desc.Version != 3 || len(desc.Branches) < 2 || len(desc.Branches) > MaxSources || desc.GroupKeyColumn == "" ||
			desc.RowCountColumn == "" || len(desc.StateColumns) == 0 {
			return Invalid("invalid materialized view UNION ALL incremental specification")
		}
		branchIDs := make(map[int]struct{}, len(desc.Branches))
		for _, item := range desc.Branches {
			branch := item.Description
			if branch == nil || branch.BranchID <= 0 || branch.SourceDatabase == "" || branch.SourceTable == "" ||
				branch.GroupKeyColumn != desc.GroupKeyColumn || branch.RowCountColumn != desc.RowCountColumn ||
				!slices.Equal(branch.StateColumns, desc.StateColumns) {
				return Invalid("invalid materialized view UNION ALL branch specification")
			}
			if _, exists := branchIDs[branch.BranchID]; exists {
				return Invalid("duplicate materialized view UNION ALL branch identity")
			}
			branchIDs[branch.BranchID] = struct{}{}
			if err := validateIncremental(branch, true); err != nil {
				return err
			}
		}
		return nil
	}
	if desc.Strategy != "direct-delta" && desc.Strategy != "hybrid-state" && desc.Strategy != "hybrid-affected-group" {
		return Invalid("unsupported materialized view incremental strategy")
	}
	if len(desc.Branches) != 0 {
		return Invalid("nested materialized view incremental branches are not supported")
	}
	if desc.SourceAlias == "" || len(desc.SourceColumns) == 0 || len(desc.Groups) == 0 ||
		desc.RowCountColumn == "" || len(desc.StateColumns) == 0 {
		return Invalid("incomplete materialized view incremental specification")
	}
	for _, group := range desc.Groups {
		if group.Expression == "" || group.OutputColumn == "" {
			return Invalid("invalid materialized view incremental group")
		}
	}
	for _, agg := range desc.Aggregates {
		switch agg.Kind {
		case "count_star":
		case "count_column":
			if agg.InputExpression == "" {
				return Invalid("incremental COUNT requires an input")
			}
		case "sum":
			if agg.InputExpression == "" || agg.StateCountColumn == "" {
				return Invalid("incremental SUM requires input and state")
			}
			if desc.GroupKeyColumn != "" && agg.StateSumColumn == "" {
				return Invalid("incremental SUM with a group key requires sum state")
			}
		case "avg":
			if agg.InputExpression == "" || agg.StateSumColumn == "" || agg.StateCountColumn == "" {
				return Invalid("incremental AVG requires input and state")
			}
		case "min", "max":
			if agg.InputExpression == "" {
				return Invalid("incremental %s requires an input", strings.ToUpper(agg.Kind))
			}
		case "count_distinct":
			if desc.Version < 2 || desc.StateTable == "" || agg.InputExpression == "" || agg.StateIndex <= 0 {
				return Invalid("incremental COUNT(DISTINCT) requires versioned auxiliary state")
			}
		case "sum_distinct", "avg_distinct":
			if desc.Version < 2 || desc.StateTable == "" || agg.InputExpression == "" || agg.StateIndex <= 0 || agg.StateSumColumn == "" || agg.StateCountColumn == "" {
				return Invalid("incremental %s(DISTINCT) requires versioned auxiliary state", strings.ToUpper(strings.TrimSuffix(agg.Kind, "_distinct")))
			}
		default:
			return Invalid("incremental aggregate %q is not supported", agg.Kind)
		}
	}
	return nil
}
