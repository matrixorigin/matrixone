// Copyright 2023 Matrix Origin
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

package lockop

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
)

func TestLockWithUniqueID(t *testing.T) {
	runLockOpTest(
		t,
		func(proc *process.Process) {
			pkType := types.New(types.T_int32, 0, 0)
			assert.NoError(t, LockTableWithUniqueID(proc.Ctx, sid, "u1", 1, proc.Base.TxnClient, pkType, nil, proc.Mp(), lock.LockMode_Exclusive))
			assert.Equal(t, 1, len(internalProcesses))

			assert.NoError(t, UnlockWithUniqueID(proc.Ctx, "u1"))
			assert.Equal(t, 0, len(internalProcesses))
		},
	)
}

func TestLockWithUniqueIDAndShared(t *testing.T) {
	runLockOpTest(
		t,
		func(proc *process.Process) {
			pkType := types.New(types.T_int32, 0, 0)
			assert.NoError(t, LockTableWithUniqueID(proc.Ctx, sid, "u1", 1, proc.Base.TxnClient, pkType, nil, proc.Mp(), lock.LockMode_Shared))
			assert.Equal(t, 1, len(internalProcesses))

			assert.NoError(t, LockTableWithUniqueID(proc.Ctx, sid, "u2", 1, proc.Base.TxnClient, pkType, nil, proc.Mp(), lock.LockMode_Shared))
			assert.Equal(t, 2, len(internalProcesses))

			assert.NoError(t, UnlockWithUniqueID(proc.Ctx, "u1"))
			assert.Equal(t, 1, len(internalProcesses))

			assert.NoError(t, UnlockWithUniqueID(proc.Ctx, "u2"))
			assert.Equal(t, 0, len(internalProcesses))
		},
	)
}
