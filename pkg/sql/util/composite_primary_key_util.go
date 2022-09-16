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
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/multi"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strconv"
)

var prefixPriColName string = "__mo_cpkey_"

func ExtractCompositePrimaryKeyColumnFromColDefs(colDefs []*plan.ColDef) ([]*plan.ColDef, *plan.ColDef) {
	for num := range colDefs {
		if colDefs[num].IsCPkey {
			cPKC := colDefs[num]
			colDefs = append(colDefs[:num], colDefs[num+1:]...)
			return colDefs, cPKC
		}
	}
	return colDefs, nil
}

func BuildCompositePrimaryKeyColumnName(s []string) string {
	var name string
	name = prefixPriColName
	for _, single := range s {
		lenNum := format.Int64ToString(int64(len(single)))
		for num := 0; num < 3-len(lenNum); num++ {
			name += string('0')
		}
		name += lenNum
		name += single
	}
	return name
}

func SplitCompositePrimaryKeyColumnName(s string) []string {
	var names []string
	for next := len(prefixPriColName); next < len(s); {
		strLen, _ := strconv.Atoi(s[next : next+3])
		names = append(names, s[next+3:next+3+strLen])
		next += strLen + 3
	}

	return names
}

func FillCompositePKeyBatch(bat *batch.Batch, p *plan.ColDef, proc *process.Process) error {
	names := SplitCompositePrimaryKeyColumnName(p.Name)
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
	vec, err := multi.Serial(vs, proc)
	if err != nil {
		return err
	}
	bat.Attrs = append(bat.Attrs, p.Name)
	bat.Vecs = append(bat.Vecs, vec)
	return nil
}
