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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJSONReaderMemoryAdmissionCapturesGeneration(t *testing.T) {
	proc := testutil.NewProcess(t)
	original := proc.Ctx
	ctx, cancel := context.WithCancel(original)
	proc.Ctx = ctx
	defer func() { cancel(); proc.Ctx = original }()
	admission, err := JSONReaderMemoryAdmission(proc)
	require.NoError(t, err)
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	before := generation.Snapshot().Used
	release, err := admission(123)
	require.NoError(t, err)
	require.Equal(t, before+123, generation.Snapshot().Used)
	cancel()
	_, err = admission(1)
	require.ErrorIs(t, err, context.Canceled)
	release()
	require.Equal(t, before, generation.Snapshot().Used)
}

func TestJSONReaderMemoryAdmissionRejectsClosedGeneration(t *testing.T) {
	proc := testutil.NewProcess(t)
	admission, err := JSONReaderMemoryAdmission(proc)
	require.NoError(t, err)
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	generation.Close()
	release, err := admission(1)
	require.Error(t, err)
	require.Nil(t, release)
}

func TestJSONReaderMemoryAdmissionUsesFoldingContext(t *testing.T) {
	proc := testutil.NewProcess(t)
	original := proc.Ctx
	ctx, cancel := context.WithCancel(original)
	cancel()
	proc.Ctx = ctx
	proc.SetBaseProcessRunningStatus(false)
	defer func() { proc.Ctx = original }()
	admission, err := JSONReaderMemoryAdmissionForEvaluation(proc)
	require.NoError(t, err)
	release, err := admission(1)
	require.NoError(t, err)
	release()
}
