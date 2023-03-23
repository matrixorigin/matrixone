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

package seq

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// TODO: Require usage or select privilege on the last used sequence.

// Lastval is initialized by call of nextval.
// Will be set to the most recently returned nextval of any sequence in current session.
// or is will be set to setval value when the third arg of it is true.
func Lastval(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	// Get last value
	lastv := proc.SessionInfo.SeqLastValue[0]
	if lastv == "" {
		return nil, moerr.NewInternalError(proc.Ctx, "Last value of current session is not initialized.")
	}
	res := vector.NewVec(types.T_varchar.ToType())
	if err := vector.AppendAny(res, []byte(lastv), false, proc.Mp()); err != nil {
		return nil, err
	}
	return res, nil
}
