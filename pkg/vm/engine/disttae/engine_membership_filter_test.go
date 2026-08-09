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

package disttae

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

type remoteMembershipFilterAdmission struct {
	acquired atomic.Int64
	released atomic.Int64
}

func (a *remoteMembershipFilterAdmission) Acquire(bytes int64) (int64, bool) {
	a.acquired.Add(bytes)
	return 0, true
}

func (a *remoteMembershipFilterAdmission) Release(bytes int64) int64 {
	a.released.Add(bytes)
	return 0
}

func TestBuildBlockReadersDecodesRemoteMembershipFilter(t *testing.T) {
	proc := testutil.NewProcess(t)
	e := &Engine{fs: proc.GetFileService()}
	tableDef := &plan.TableDef{
		Name: "remote_membership_filter",
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "pk"},
	}

	readers, err := e.BuildBlockReaders(
		context.Background(),
		proc,
		timestamp.Timestamp{},
		nil,
		tableDef,
		readutil.NewBlockListRelationData(1),
		1,
		engine.FilterHint{
			MembershipFilterBytes: []byte{docfilter.TagSorted64, 1},
		},
	)
	require.Error(t, err)
	require.Nil(t, readers)
}

func TestBuildBlockReadersSharesRemoteMembershipFilterLease(t *testing.T) {
	const service = "remote-membership-filter-test"
	admission := new(remoteMembershipFilterAdmission)
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, service, nil)
	moruntime.SetupServiceBasedRuntime(service, rt)
	rt.SetGlobalVariables(moruntime.CNMemoryThrottler, admission)

	proc := testutil.NewProcess(t)
	e := &Engine{service: service, fs: proc.GetFileService()}
	tableDef := &plan.TableDef{
		Name: "remote_membership_filter",
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "pk"},
	}
	payload := append([]byte{docfilter.TagSorted64}, make([]byte, 8)...)

	readers, err := e.BuildBlockReaders(
		context.Background(),
		proc,
		timestamp.Timestamp{},
		nil,
		tableDef,
		readutil.NewBlockListRelationData(2),
		2,
		engine.FilterHint{MembershipFilterBytes: payload},
	)
	require.NoError(t, err)
	require.Len(t, readers, 2)
	require.Equal(t, int64(8), admission.acquired.Load())
	require.Zero(t, admission.released.Load())

	require.NoError(t, readers[0].Close())
	require.Zero(t, admission.released.Load())
	require.NoError(t, readers[1].Close())
	require.Equal(t, int64(8), admission.released.Load())
}
