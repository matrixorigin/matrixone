// Copyright 2021 - 2022 Matrix Origin
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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// a general round method is needed for timestamp fsp
func CurrentTimestamp(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	scale := int32(6)
	if len(ivecs) == 1 {
		scale = int32(vector.MustFixedCol[int64](ivecs[0])[0])
	}
	rtyp := types.New(types.T_timestamp, 0, scale)
	resultValue := types.UnixNanoToTimestamp(proc.UnixTime)
	return vector.NewConstFixed(rtyp, resultValue, 1, proc.Mp()), nil
}
