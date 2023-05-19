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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"strconv"

	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func JudgeIsCompositeClusterByColumn(s string) bool {
	if len(s) < len(catalog.PrefixCBColName) {
		return false
	}
	return s[0:len(catalog.PrefixCBColName)] == catalog.PrefixCBColName
}

func BuildCompositeClusterByColumnName(s []string) string {
	var name string
	name = catalog.PrefixCBColName
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

func SplitCompositeClusterByColumnName(s string) []string {
	var names []string
	for next := len(catalog.PrefixCBColName); next < len(s); {
		strLen, _ := strconv.Atoi(s[next : next+3])
		names = append(names, s[next+3:next+3+strLen])
		next += strLen + 3
	}

	return names
}

func GetClusterByColumnOrder(cbName, colName string) int {
	if len(cbName) == 0 || len(colName) == 0 {
		return -1
	}
	if cbName == colName {
		return 0
	}
	if !JudgeIsCompositeClusterByColumn(cbName) {
		return -1
	}
	idx := 0
	for next := len(catalog.PrefixCBColName); next < len(cbName); {
		strLen, _ := strconv.Atoi(cbName[next : next+3])
		if cbName[next+3:next+3+strLen] == colName {
			return idx
		}
		next += strLen + 3
		idx++
	}
	return -1
}

// build the clusterBy key's vector of the cluster table according to the composite column name, and append the result vector to batch
// cbName: column name of composite column
func FillCompositeClusterByBatch(bat *batch.Batch, cbName string, proc *process.Process) error {
	names := SplitCompositeClusterByColumnName(cbName)
	return FillCompositeKeyBatch(bat, cbName, names, proc)
}

// build the vector of the composite key, and append the result vector to batch
// ckeyName: column name of composite column
// keyParts: parts of the composite column
func FillCompositeKeyBatch(bat *batch.Batch, ckeyName string, keyParts []string, proc *process.Process) error {
	cCBVectorMap := make(map[string]*vector.Vector)
	for num, attrName := range bat.Attrs {
		for _, elem := range keyParts {
			if attrName == elem {
				cCBVectorMap[elem] = bat.Vecs[num]
			}
		}
	}
	vs := make([]*vector.Vector, 0)
	for _, elem := range keyParts {
		v := cCBVectorMap[elem]
		vs = append(vs, v)
	}
	vec, err := function.RunFunctionDirectly(proc, function.SerialFunctionEncodeID, vs, bat.Length())
	if err != nil {
		return err
	}
	bat.Attrs = append(bat.Attrs, ckeyName)
	bat.Vecs = append(bat.Vecs, vec)
	return nil
}
