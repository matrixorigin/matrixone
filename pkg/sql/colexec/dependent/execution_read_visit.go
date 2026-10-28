// Copyright 2026 Matrix Origin
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

package dependent

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

// VisitSourceNodes runs visit exactly once over every compiled source node
// of every region, in template order. Locking current-read preparation
// outside the dependent program (pkg/sql/compile) uses it to join this
// statement's sources to the per-attempt execution-read lifecycle BEFORE
// any pipeline runs: region demand opens sources lazily mid-stream, so the
// statement-wide admission decision, carrier acquisition and relation
// binding must already exist by then instead of being re-derived at each
// open (pre-PR review P2).
func (t *Template) VisitSourceNodes(visit func(node *plan.Node) error) error {
	if t == nil {
		return nil
	}
	for _, rg := range t.regions {
		for _, nt := range rg.nodes {
			if nt == nil || nt.source == nil || nt.node == nil {
				continue
			}
			if err := visit(nt.node); err != nil {
				return err
			}
		}
	}
	return nil
}
