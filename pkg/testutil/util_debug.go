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

package testutil

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

var _ = OperatorCatchBatch

// OperatorCatchBatch return a string with format
//
//	`insert operator catch a batch, batch length is 100, [vec 0 : len is 100]`
func OperatorCatchBatch(operatorName string, bat *batch.Batch) string {
	if bat == nil {
		return ""
	}
	str := fmt.Sprintf("`%s` operator catch a batch, batch length is %d\n", operatorName, bat.Length())
	for i, vec := range bat.Vecs {
		str += fmt.Sprintf("[vec %d[type is %d, %p] : len is %d]\n", i,
			vec.GetType().Oid, vec,
			vec.Length())
	}
	return str
}
