// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package unary

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJsonQuote(t *testing.T) {
	proc := testutil.NewProc()
	vec := testutil.MakeScalarVarchar("hello", 1)
	_, err := JsonQuote([]*vector.Vector{vec}, proc)
	require.NoError(t, err)
	vec = testutil.MakeScalarNull(types.T_varchar, 1)
	_, err = JsonQuote([]*vector.Vector{vec}, proc)
	require.NoError(t, err)
	vec = testutil.MakeVarcharVector([]string{"hello", "world", "matrix"}, []uint64{0, 0, 1})
	_, err = JsonQuote([]*vector.Vector{vec}, proc)
}
