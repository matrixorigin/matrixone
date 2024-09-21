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

package dispatch

import (
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPrepareRemote(t *testing.T) {
	_ = colexec.NewServer(nil)

	proc := testutil.NewProcess()

	uid, err := uuid.NewV7()
	require.NoError(t, err)

	d := Dispatch{
		FuncId: SendToAllFunc,
		ctr:    &container{},
		RemoteRegs: []colexec.ReceiveInfo{
			{Uuid: uid},
		},
	}

	// uuid map should have this pipeline information after prepare remote.
	require.NoError(t, d.prepareRemote(proc))

	p, c, b := colexec.Get().GetProcByUuid(uid, false)
	require.True(t, b)
	require.Equal(t, proc, p)
	require.Equal(t, d.ctr.remoteInfo, c)
}
