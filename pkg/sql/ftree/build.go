// Copyright 2021 Matrix Origin
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

package ftree

import (
	"matrixone/pkg/sql/plan"
)

func New() *build {
	return &build{}
}

func (b *build) Build(qry *plan.Query) (*FTree, error) {
	if len(qry.Rels) == 1 {
		return &FTree{
			Qry:      qry,
			FreeVars: qry.FreeAttrs,
			Roots:    buildPath(qry.Rels[0], qry, nil),
		}, nil
	}
	rn, err := selectRoot(qry)
	if err != nil {
		return nil, err
	}
	roots, err := buildNodes(qry.RelsMap[rn], qry)
	if err != nil {
		return nil, err
	}
	f := &FTree{
		Qry:      qry,
		Roots:    roots,
		FreeVars: qry.FreeAttrs,
	}
	return f, f.check(qry)
}
