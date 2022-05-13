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

package process

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/require"
)

func TestProcess(t *testing.T) {
	proc := New(mheap.New(guest.New(1<<30, host.New(1<<30))))
	vec, err := Get(proc, 10, types.Type{Oid: types.T_int8})
	require.NoError(t, err)
	Put(proc, vec)
	vec, err = Get(proc, 10, types.Type{Oid: types.T_int8})
	require.NoError(t, err)
	Put(proc, vec)
	FreeRegisters(proc)
}
