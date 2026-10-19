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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func timestampAsDatetime(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	reader, err := newTimestampPairDateReader(parameters[0], proc)
	if err != nil {
		return err
	}
	results := vector.MustFunctionResult[types.Datetime](result)
	scale := results.GetType().Scale
	for row := uint64(0); row < uint64(length); row++ {
		if functionRowSkipped(selectList, row) {
			if err := results.Append(0, true); err != nil {
				return err
			}
			continue
		}
		value, invalid := reader(row)
		if invalid || !validDatetimeResult(value) {
			if err := results.Append(0, true); err != nil {
				return err
			}
			continue
		}
		if err := results.Append(value.TruncateToScale(scale), false); err != nil {
			return err
		}
	}
	return nil
}
