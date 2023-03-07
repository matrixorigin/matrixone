// Copyright 2021 - 2022 Matrix Origin
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

package multi

import (
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCurrentTimestamp(t *testing.T) {
	myProc := testutil.NewProc()
	resultVector, err := CurrentTimestamp(nil, myProc)
	require.NoError(t, err)
	resultValues := vector.MustTCols[types.Timestamp](resultVector)
	resultStr := resultValues[0].String2(time.Local, 6)
	fmt.Println(resultStr)

	inputVector0 := testutil.MakeScalarInt64(3, 4)
	inputVectors := []*vector.Vector{inputVector0}
	resultVector, err = CurrentTimestamp(inputVectors, myProc)
	require.NoError(t, err)
	resultValues = vector.MustTCols[types.Timestamp](resultVector)
	resultStr = resultValues[0].String2(time.Local, resultVector.Typ.Scale)
	fmt.Println(resultStr)
}
