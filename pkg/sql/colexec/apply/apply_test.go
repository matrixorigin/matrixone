// Copyright 2024 Matrix Origin
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

package apply

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	tests := []struct {
		name      string
		applyType int
		want      string
	}{
		{name: "cross", applyType: CROSS, want: "apply: cross apply "},
		{name: "outer", applyType: OUTER, want: "apply: outer apply "},
		{name: "unknown", applyType: -1, want: "apply"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			arg := NewArgument()
			arg.ApplyType = test.applyType
			buf := new(bytes.Buffer)
			arg.String(buf)
			require.Equal(t, test.want, buf.String())
		})
	}
}

func TestNilTableFunctionLifecycle(t *testing.T) {
	proc := testutil.NewProc(t)
	arg := NewArgument()

	err := arg.Prepare(proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	require.ErrorContains(t, err, "apply operator missing table function")

	_, err = arg.Call(proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	require.ErrorContains(t, err, "apply operator missing table function")

	require.NotPanics(t, func() {
		arg.Reset(proc, false, nil)
	})

	require.NotPanics(t, func() {
		arg.Free(proc, false, nil)
	})
}
