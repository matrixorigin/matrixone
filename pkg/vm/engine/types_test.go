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

package engine

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConstraintDefMarshalBinary(t *testing.T) {
	a := &ConstraintDef{}
	a.Cts = append(a.Cts, &UniqueIndexDef{UniqueIndex: "hello"})
	a.Cts = append(a.Cts, &SecondaryIndexDef{SecondaryIndex: "world"})
	data, err := a.MarshalBinary()
	require.NoError(t, err)
	b := &ConstraintDef{}
	err = b.UnmarshalBinary(data)
	require.NoError(t, err)
	require.Equal(t, "hello", b.Cts[0].(*UniqueIndexDef).UniqueIndex)
	require.Equal(t, "world", b.Cts[1].(*SecondaryIndexDef).SecondaryIndex)
}
