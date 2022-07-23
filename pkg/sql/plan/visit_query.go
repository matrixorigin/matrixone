// Copyright 2022 Matrix Origin
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

package plan

type VisitRule interface {
	Match(*Node) bool
	Apply(*Node, *Query) error
}

type VisitQuery struct {
	qry   *Query
	rules []VisitRule
}

func NewVisitQuery(qry *Query, rules []VisitRule) *VisitQuery {
	return &VisitQuery{
		qry:   qry,
		rules: rules,
	}
}

func (vq *VisitQuery) exploreNode(node *Node) error {
	for i := range node.Children {
		if err := vq.exploreNode(vq.qry.Nodes[node.Children[i]]); err != nil {
			return err
		}
	}

	for _, rule := range vq.rules {
		if rule.Match(node) {
			err := rule.Apply(node, vq.qry)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (vq *VisitQuery) Visit() error {
	if len(vq.qry.Steps) == 0 {
		return nil
	}
	for _, step := range vq.qry.Steps {
		err := vq.exploreNode(vq.qry.Nodes[step])
		if err != nil {
			return err
		}
	}
	return nil
}
