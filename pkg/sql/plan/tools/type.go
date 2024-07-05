// Copyright 2024 Matrix Origin
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

package tools

import (
	"context"

	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type Aliases plan.UnorderedMap[string, string]

type MatchResult struct {
	IsMatch    bool
	RetAliases Aliases
}

func NewMatchResult(isMatch bool, retAliases Aliases) *MatchResult {
	return &MatchResult{
		IsMatch:    isMatch,
		RetAliases: retAliases,
	}
}

func Matched() *MatchResult {
	return NewMatchResult(true, nil)
}

type Matcher interface {
	// SimpleMatch check the intuitive properties about Node like type, datatype, etc.
	SimpleMatch(*plan2.Node) bool

	// DeepMatch check the internal structure about Node
	DeepMatch(context.Context, *plan2.Node, Aliases) (*MatchResult, error)
}

type RValueMatcher interface {
	GetAssignedVar(*plan2.Node, Aliases) *plan2.ColDef
}

// MatchPattern denotes the structure pattern that the Plan
// should have.
type MatchPattern struct {
	Matchers []Matcher       //matchers for components in Node
	Children []*MatchPattern // children pattern of children nodes
	AnyTree  bool
}

type Domain struct {
}
