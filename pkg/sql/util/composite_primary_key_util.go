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

package util

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// this func can't judge index table col is compound or not
func JudgeIsCompositePrimaryKeyColumn(s string) bool {
	return s == catalog.CPkeyColName
}

func BuildCompositePrimaryKeyColumnName() (string, error) {
	return catalog.CPkeyColName, nil
}

// Build composite primary key batch
func FillCompositePKeyBatch(bat *batch.Batch, p *plan.PrimaryKeyDef, proc *process.Process) error {
	names := p.Names
	cPkeyVecMap := make(map[string]*vector.Vector)
	for num, attrName := range bat.Attrs {
		for _, name := range names {
			if attrName == name {
				cPkeyVecMap[name] = bat.Vecs[num]
			}
		}
	}
	vs := make([]*vector.Vector, 0)
	for _, name := range names {
		v := cPkeyVecMap[name]
		vs = append(vs, v)
	}
	for _, v := range vs {
		if nulls.Any(v.Nsp) {
			return moerr.NewConstraintViolation(proc.Ctx, "composite pkey don't support null value")
		}
	}
	vec, _ := serialWithCompacted(vs, proc)
	bat.Attrs = append(bat.Attrs, p.PkeyColName)
	bat.Vecs = append(bat.Vecs, vec)
	return nil
}
