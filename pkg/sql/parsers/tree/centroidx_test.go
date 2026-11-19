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

package tree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCentroidxOption(t *testing.T) {
	o := CentroidxOption{OpType: "vector_l2_ops"}
	require.Equal(t, "vector_l2_ops", o.String(), "a metric alone stays the plain option")
	got, err := ParseCentroidxOption(o.String())
	require.NoError(t, err)
	require.Equal(t, o, got)

	o = CentroidxOption{OpType: "vector_l2_ops", Params: `{"quantization":"int8"}`, Config: `{"db":"D"}`}
	got, err = ParseCentroidxOption(o.String())
	require.NoError(t, err)
	require.Equal(t, o, got)

	_, err = ParseCentroidxOption("{")
	require.Error(t, err)
}
