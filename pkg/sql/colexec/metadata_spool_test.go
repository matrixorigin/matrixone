// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec_test

import (
	"context"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetadataSpoolAdmissionAndClose(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	proc.Base.Lim.SpillSize = 5
	budget, err := proc.GetHashBuildBudget()
	require.NoError(t, err)
	first, err := colexec.NewMetadataSpool(proc)
	require.NoError(t, err)
	defer func() { require.NoError(t, first.Close()) }()
	second, err := colexec.NewMetadataSpool(proc)
	require.NoError(t, err)
	defer func() { require.NoError(t, second.Close()) }()
	require.NoError(t, first.Reserve(3))
	require.Error(t, second.Reserve(3), "different files must share the same statement cap")
	require.Equal(t, uint64(3), budget.SpillDiskUsed())
	require.Equal(t, uint64(2), budget.SpillFDUsed())
	require.NoError(t, first.Close())
	require.ErrorIs(t, first.Reserve(1), os.ErrClosed)
	require.NoError(t, second.Reserve(3), "physical close makes storage admission reusable")
	require.NoError(t, second.Close())
	require.NoError(t, second.Close())
	require.Zero(t, budget.SpillDiskUsed())
	require.Zero(t, budget.SpillFDUsed())
}

func TestMetadataSpoolPartialCreation(t *testing.T) {
	for _, mode := range []string{"file_service_error", "fd_limit", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProc(t)
			defer proc.Free()
			budget, err := proc.GetHashBuildBudget()
			require.NoError(t, err)
			switch mode {
			case "file_service_error":
				proc.Base.FileService = nil
			case "fd_limit":
				token, err := budget.ReserveSpillFD(budget.SpillFDCap())
				require.NoError(t, err)
				defer token.Release()
			case "cancel":
				ctx, cancel := context.WithCancel(proc.Ctx)
				cancel()
				proc.Ctx = ctx
			}
			before := budget.SpillFDUsed()
			file, err := colexec.NewMetadataSpool(proc)
			require.Error(t, err)
			require.Nil(t, file)
			require.Zero(t, budget.SpillDiskUsed())
			require.Equal(t, before, budget.SpillFDUsed(), "partial creation returns only its own reservations")
		})
	}
}
