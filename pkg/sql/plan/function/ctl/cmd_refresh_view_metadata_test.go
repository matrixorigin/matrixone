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

package ctl

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestHandleRefreshViewMetadataRegistrationAndErrors(t *testing.T) {
	proc := testutil.NewProcess(t)
	RegisterRefreshViewMetadataHandler("", nil)
	t.Cleanup(func() { RegisterRefreshViewMetadataHandler("", nil) })

	_, err := handleRefreshViewMetadata(proc, tn, "", nil)
	require.Error(t, err)
	_, err = handleRefreshViewMetadata(proc, cn, "", nil)
	require.Error(t, err)

	expected := errors.New("refresh failed")
	RegisterRefreshViewMetadataHandler("", func(_ *process.Process, parameter string) (int, error) {
		require.Equal(t, "worker-a", parameter)
		return 0, expected
	})
	_, err = handleRefreshViewMetadata(proc, cn, "worker-a", nil)
	require.ErrorIs(t, err, expected)

	RegisterRefreshViewMetadataHandler("", func(_ *process.Process, parameter string) (int, error) {
		require.Equal(t, "worker-b", parameter)
		return 7, nil
	})
	result, err := handleRefreshViewMetadata(proc, cn, "worker-b", nil)
	require.NoError(t, err)
	require.Equal(t, RefreshViewMetadata, result.Method)
	require.Equal(t, 7, result.Data)
}
