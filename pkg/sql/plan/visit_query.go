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

type VisitQuery struct {
	qry  *Query
	rule Rule
}

func NewVisitQuery(qry *Query, rule Rule) *VisitQuery {
	return &VisitQuery{
		qry:  qry,
		rule: rule,
	}
}

func (vq *VisitQuery) exploreNode(node *Node) {
	for i := range node.Children {
		vq.exploreNode(vq.qry.Nodes[node.Children[i]])
	}

	if vq.rule.Match(node) {
		vq.rule.Apply(node, vq.qry)
	}
}

func (vq *VisitQuery) Visit() (*Query, error) {
	if len(vq.qry.Steps) == 0 {
		return vq.qry, nil
	}
	for _, step := range vq.qry.Steps {
		vq.exploreNode(vq.qry.Nodes[step])
	}
	return vq.qry, nil
}
