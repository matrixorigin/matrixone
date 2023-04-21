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

package ctl

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
)

func TestHandleGetSnapshotTS(t *testing.T) {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	result, err := handleGetSnapshotTS(nil, cn, "", nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, result.Data)
	ts, err := timestamp.ParseTimestamp(result.Data.(string))
	assert.NoError(t, err)
	assert.NotEqual(t, timestamp.Timestamp{}, ts)
}

func TestHandleUseSnapshotTS(t *testing.T) {
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	result, err := handleUseSnapshotTS(nil, cn, "1-2", nil)
	assert.NoError(t, err)
	assert.Equal(t, "OK", result.Data)

	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.TxnOptions)
	assert.True(t, ok)
	assert.NotEmpty(t, v)

	result, err = handleUseSnapshotTS(nil, cn, "", nil)
	assert.NoError(t, err)
	assert.Equal(t, "OK", result.Data)

	v, ok = runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.TxnOptions)
	assert.True(t, ok)
	assert.Empty(t, v)
}
