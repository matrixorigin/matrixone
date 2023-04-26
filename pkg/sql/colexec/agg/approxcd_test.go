// Copyright 2022 Matrix Origin
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

package agg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApproxCountDisticMarshalAndUnmarshal(t *testing.T) {
	approx := NewApproxc[uint64]()
	approx.Grows(5)
	for i := 0; i < 5; i++ {
		approx.Fill(int64(i), uint64(i), uint64(i), int64(i), true, false)
	}

	data, err := approx.MarshalBinary()
	require.NoError(t, err)

	ret := NewApproxc[uint64]()
	err = ret.UnmarshalBinary(data)
	require.NoError(t, err)

	require.Equal(t, approx, ret)
}
