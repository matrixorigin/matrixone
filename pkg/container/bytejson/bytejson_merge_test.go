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

package bytejson

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestByteJsonMergePatch(t *testing.T) {
	target, err := ParseFromString(`{"a":1,"nested":{"keep":1,"remove":2}}`)
	require.NoError(t, err)
	patch, err := ParseFromString(`{"b":2,"nested":{"remove":null,"add":3}}`)
	require.NoError(t, err)

	merged, err := target.MergePatch(patch)
	require.NoError(t, err)
	require.JSONEq(t, `{"a":1,"b":2,"nested":{"add":3,"keep":1}}`, merged.String())
}

func TestByteJsonMergePreserve(t *testing.T) {
	left, err := ParseFromString(`{"a":{"x":1},"array":[1]}`)
	require.NoError(t, err)
	right, err := ParseFromString(`{"a":{"y":2},"array":[2],"value":null}`)
	require.NoError(t, err)

	merged, err := left.MergePreserve(right)
	require.NoError(t, err)
	require.JSONEq(t, `{"a":{"x":1,"y":2},"array":[1,2],"value":null}`, merged.String())
}

func TestByteJsonMergePreserveAutowrapsScalars(t *testing.T) {
	left, err := ParseFromString(`1`)
	require.NoError(t, err)
	right, err := ParseFromString(`null`)
	require.NoError(t, err)

	merged, err := left.MergePreserve(right)
	require.NoError(t, err)
	require.JSONEq(t, `[1,null]`, merged.String())
}
