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
	"github.com/matrixorigin/matrixone/pkg/vectorize/get_timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func UTCTimestamp(_ []*vector.Vector, _ *process.Process) (*vector.Vector, error) {
	resultType := types.Type{Oid: types.T_datetime, Size: 8}
	resultVector := vector.NewConst(resultType, 1)
	result := make([]types.Datetime, 1)
	result[0] = get_timestamp.GetUTCTimestamp()
	vector.SetCol(resultVector, result)
	return resultVector, nil
}
