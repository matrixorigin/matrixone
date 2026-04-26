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

package types

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeSetValues(t *testing.T) {
	values, err := NormalizeSetValues([]string{"red", "blue   ", "GREEN"})
	require.NoError(t, err)
	require.Equal(t, []string{"red", "blue", "GREEN"}, values)

	_, err = NormalizeSetValues([]string{"red", "Red"})
	require.Error(t, err)

	_, err = NormalizeSetValues([]string{"red,blue"})
	require.Error(t, err)
}

func TestParseSet(t *testing.T) {
	setDef := "read,write,execute"

	bits, err := ParseSet(setDef, "read,execute")
	require.NoError(t, err)
	require.Equal(t, uint64(5), bits)

	bits, err = ParseSet(setDef, "3")
	require.NoError(t, err)
	require.Equal(t, uint64(3), bits)

	value, err := ParseSetIndex(setDef, 5)
	require.NoError(t, err)
	require.Equal(t, "read,execute", value)

	_, err = ParseSet(setDef, "delete")
	require.Error(t, err)

	_, err = ParseSetValue(setDef, 8)
	require.Error(t, err)
}

func TestNormalizeSetValuesMaxMembers(t *testing.T) {
	values := make([]string, MaxSetMembers+1)
	for i := range values {
		values[i] = fmt.Sprintf("v%d", i)
	}
	_, err := NormalizeSetValues(values)
	require.Error(t, err)
}
