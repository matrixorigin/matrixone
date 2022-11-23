// Copyright 2022 Matrix Origin
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

package table_function

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestString(t *testing.T) {
	arg := Argument{Name: "unnest"}
	String(&arg, bytes.NewBuffer(nil))
}

func TestCall(t *testing.T) {
	arg := Argument{Name: "unnest"}
	end, err := Call(0, testutil.NewProc(), &arg)
	require.NoError(t, err)
	require.True(t, end)
	arg.Name = "generate_series"
	end, err = Call(0, testutil.NewProc(), &arg)
	require.NoError(t, err)
	require.True(t, end)
	arg.Name = "not_exist"
	end, err = Call(0, testutil.NewProc(), &arg)
	require.Error(t, err)
	require.True(t, end)
}

func TestPrepare(t *testing.T) {
	arg := Argument{Name: "unnest"}
	err := Prepare(testutil.NewProc(), &arg)
	require.Error(t, err)
	arg.Name = "generate_series"
	err = Prepare(testutil.NewProc(), &arg)
	require.NoError(t, err)
	arg.Name = "not_exist"
	err = Prepare(testutil.NewProc(), &arg)
	require.Error(t, err)
}
