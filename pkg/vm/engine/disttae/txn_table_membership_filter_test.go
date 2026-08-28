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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestTxnTableBuildReadersUsesMembershipFilterAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)

	tbl := newTxnTableForTest()
	tbl.proc.Store(proc)
	tbl.getTxn().engine.fs = proc.GetFileService()
	tbl.tableDef = &plan.TableDef{
		Name: "membership_filter_admission",
		Cols: []*plan.ColDef{{
			Name:   "pk",
			Typ:    plan.Type{Id: int32(types.T_int64)},
			Seqnum: 0,
		}},
		Name2ColIndex: map[string]int32{"pk": 0},
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "pk"},
	}

	rt := moruntime.ServiceRuntime(proc.GetService())
	require.NotNil(t, rt)
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.CNMemoryThrottler)
	admission := new(recordingFilterAdmission)
	rt.SetGlobalVariables(moruntime.CNMemoryThrottler, admission)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.CNMemoryThrottler, previous)
		} else {
			rt.SetGlobalVariables(moruntime.CNMemoryThrottler, nil)
		}
	})

	state := logtailreplay.NewPartitionState("", true, 42, false)
	relData := readutil.NewBlockListRelationData(
		2,
		readutil.WithPartitionState(state),
	)
	payload := append([]byte{docfilter.TagSorted64}, make([]byte, 8)...)
	readers, err := tbl.BuildReaders(
		context.Background(),
		proc,
		nil,
		relData,
		2,
		0,
		false,
		engine.Policy_CheckAll,
		engine.FilterHint{MembershipFilterBytes: payload},
	)
	require.NoError(t, err)
	require.Len(t, readers, 2)
	require.Equal(t, 1, admission.acquireCalls)
	require.Equal(t, int64(8), admission.acquired)
	require.Zero(t, admission.releaseCalls)

	closeReaders(readers)
	require.Equal(t, 1, admission.releaseCalls)
	require.Equal(t, admission.acquired, admission.released)
}
