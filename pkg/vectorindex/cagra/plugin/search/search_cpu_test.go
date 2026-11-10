//go:build !gpu

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

package search

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewReaderRequiresGPUBuild(t *testing.T) {
	proc := testutil.NewProc(t)
	_, err := Hooks{}.NewReader(proc, cagraSpec(t, types.T_array_float32, `{"op_type":"vector_l2_ops"}`),
		cagraRequest(types.T_array_float32, 1))
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
	_, err = Hooks{}.NewReader(nil, cagraSpec(t, types.T_array_float32, `{"op_type":"vector_l2_ops"}`),
		cagraRequest(types.T_array_float32, 1))
	require.ErrorContains(t, err, "requires a process")
}
