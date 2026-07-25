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
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type PhyPlan struct {
	Version     string                             `json:"version"`
	RetryTime   int                                `json:"retryTime,omitempty"`
	LocalScope  []PhyScope                         `json:"scope,omitempty"`
	RemoteScope []PhyScope                         `json:"RemoteScope,omitempty"`
	Resource    *resource.StatementResourceSummary `json:"resource,omitempty"`
}

type PhyScope struct {
	Magic               string        `json:"Magic"`
	Mcpu                int8          `json:"Mcpu,omitempty"`
	Receiver            []PhyReceiver `json:"Receiver,omitempty"`
	DataSource          *PhySource    `json:"DataSource,omitempty"`
	PreScopes           []PhyScope    `json:"PreScopes,omitempty"`
	RootOperator        *PhyOperator  `json:"RootOperator,omitempty"`
	PrepareTimeConsumed int64         `json:"PrepareTimeConsumed,omitempty"`
}

type PhyReceiver struct {
	Idx        int    `json:"Idx"`
	RemoteUuid string `json:"Uuid,omitempty"`
}

type PhySource struct {
	SchemaName   string   `json:"SchemaName"`
	RelationName string   `json:"TableName"`
	Attributes   []string `json:"Columns"`
}

type PhyOperator struct {
	OpName       string                 `json:"OpName"`
	NodeIdx      int                    `json:"NodeIdx"`
	Status       uint8                  `json:"Status"`
	DestReceiver []PhyReceiver          `json:"toMergeReceiver,omitempty"`
	OpStats      *process.OperatorStats `json:"OpStats,omitempty"`
	Children     []*PhyOperator         `json:"Children,omitempty"`
	IsFirst      bool                   `json:"IsFirst,omitempty"`
	IsLast       bool                   `json:"IsLast,omitempty"`
}

func NewPhyPlan() *PhyPlan {
	return &PhyPlan{
		// Assuming the version number is 1.0,
		Version:     "1.0",
		RemoteScope: []PhyScope{},
	}
}

// CloneForExport detaches the physical plan from the completed execution
// generation before it is retained for asynchronous export. Callers must
// invoke it before the source plan is reset or reused.
//
// TestPhyPlanCloneForExportReferenceSchemaIsExplicit is the schema tripwire
// for this semantic clone. Any new reference-bearing field must be classified
// there and detached here before the test fixture can be updated.
func (p *PhyPlan) CloneForExport() *PhyPlan {
	if p == nil {
		return nil
	}

	nodeCount, childCount := countPhyPlanOperators(p)
	cloner := phyPlanExportCloner{
		operators: make(map[*PhyOperator]*PhyOperator, nodeCount),
		stats:     make(map[*process.OperatorStats]*process.OperatorStats),
		nodes:     make([]PhyOperator, nodeCount),
		children:  make([]*PhyOperator, childCount),
	}
	clone := *p
	clone.LocalScope = cloner.cloneScopes(p.LocalScope)
	clone.RemoteScope = cloner.cloneScopes(p.RemoteScope)
	if p.Resource != nil {
		resource := *p.Resource
		clone.Resource = &resource
	}
	return &clone
}

type phyPlanExportCloner struct {
	operators map[*PhyOperator]*PhyOperator
	stats     map[*process.OperatorStats]*process.OperatorStats
	nodes     []PhyOperator
	children  []*PhyOperator
	nextNode  int
	nextChild int
}

func countPhyPlanOperators(p *PhyPlan) (int, int) {
	seen := make(map[*PhyOperator]struct{})
	var countScopes func([]PhyScope) (int, int)
	var countScope func(PhyScope) (int, int)
	var countOperator func(*PhyOperator) (int, int)
	countOperator = func(operator *PhyOperator) (int, int) {
		if operator == nil {
			return 0, 0
		}
		if _, ok := seen[operator]; ok {
			return 0, 0
		}
		seen[operator] = struct{}{}
		nodes, children := 1, len(operator.Children)
		for _, child := range operator.Children {
			childNodes, childChildren := countOperator(child)
			nodes += childNodes
			children += childChildren
		}
		return nodes, children
	}
	countScope = func(scope PhyScope) (int, int) {
		nodes, children := countOperator(scope.RootOperator)
		for _, preScope := range scope.PreScopes {
			preNodes, preChildren := countScope(preScope)
			nodes += preNodes
			children += preChildren
		}
		return nodes, children
	}
	countScopes = func(scopes []PhyScope) (int, int) {
		var nodes, children int
		for _, scope := range scopes {
			scopeNodes, scopeChildren := countScope(scope)
			nodes += scopeNodes
			children += scopeChildren
		}
		return nodes, children
	}
	localNodes, localChildren := countScopes(p.LocalScope)
	remoteNodes, remoteChildren := countScopes(p.RemoteScope)
	return localNodes + remoteNodes, localChildren + remoteChildren
}

func (c *phyPlanExportCloner) cloneScopes(scopes []PhyScope) []PhyScope {
	if scopes == nil {
		return nil
	}
	clones := make([]PhyScope, len(scopes))
	for i, scope := range scopes {
		clones[i] = c.cloneScope(scope)
	}
	return clones
}

func (c *phyPlanExportCloner) cloneScope(scope PhyScope) PhyScope {
	clone := scope
	clone.Receiver = clonePhyReceivers(scope.Receiver)
	clone.DataSource = clonePhySource(scope.DataSource)
	clone.PreScopes = c.cloneScopes(scope.PreScopes)
	clone.RootOperator = c.cloneOperator(scope.RootOperator)
	return clone
}

func clonePhyReceivers(receivers []PhyReceiver) []PhyReceiver {
	if receivers == nil {
		return nil
	}
	clone := make([]PhyReceiver, len(receivers))
	copy(clone, receivers)
	return clone
}

func clonePhySource(source *PhySource) *PhySource {
	if source == nil {
		return nil
	}
	clone := *source
	if source.Attributes != nil {
		clone.Attributes = make([]string, len(source.Attributes))
		copy(clone.Attributes, source.Attributes)
	}
	return &clone
}

func (c *phyPlanExportCloner) cloneOperator(operator *PhyOperator) *PhyOperator {
	if operator == nil {
		return nil
	}
	if clone, ok := c.operators[operator]; ok {
		return clone
	}

	clone := &c.nodes[c.nextNode]
	c.nextNode++
	*clone = *operator
	c.operators[operator] = clone
	clone.DestReceiver = clonePhyReceivers(operator.DestReceiver)
	clone.OpStats = c.cloneOperatorStats(operator.OpStats)
	if operator.Children != nil {
		start := c.nextChild
		c.nextChild += len(operator.Children)
		clone.Children = c.children[start:c.nextChild]
		for i, child := range operator.Children {
			clone.Children[i] = c.cloneOperator(child)
		}
	}
	return clone
}

func (c *phyPlanExportCloner) cloneOperatorStats(stats *process.OperatorStats) *process.OperatorStats {
	if stats == nil {
		return nil
	}
	if clone, ok := c.stats[stats]; ok {
		return clone
	}
	clone := stats.CloneForExport()
	c.stats[stats] = clone
	return clone
}

func PhyPlanToJSON(p *PhyPlan) (string, error) {
	jsonData, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

func JSONToPhyPlan(jsonStr string) (PhyPlan, error) {
	var p PhyPlan
	err := json.Unmarshal([]byte(jsonStr), &p)
	if err != nil {
		return PhyPlan{}, err
	}
	return p, nil
}
