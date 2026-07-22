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

package plan

import (
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestGetWriteFilePattern(t *testing.T) {
	// nil param
	_, ok := GetWriteFilePattern(nil)
	require.False(t, ok)

	// no option => read-only
	_, ok = GetWriteFilePattern(&tree.ExternParam{})
	require.False(t, ok)

	// present, case-insensitive key
	p := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"FORMAT", "csv", "Write_File_Pattern", "stage://s/p-%U.csv"},
	}}
	pat, ok := GetWriteFilePattern(p)
	require.True(t, ok)
	require.Equal(t, "stage://s/p-%U.csv", pat)

	// odd-length option list must not panic and not match
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{Option: []string{"write_file_pattern"}}}
	_, ok = GetWriteFilePattern(p)
	require.False(t, ok)
}

func TestGetExternParamFromTableDef(t *testing.T) {
	// nil tableDef => empty param
	require.NotNil(t, getExternParamFromTableDef(nil))

	// empty Createsql => empty param, no pattern
	td := &TableDef{}
	_, ok := GetWriteFilePattern(getExternParamFromTableDef(td))
	require.False(t, ok)

	// well-formed Createsql carrying a write pattern round-trips
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"write_file_pattern", "stage://s/o-%6N.jl"},
	}}
	raw, err := json.Marshal(param)
	require.NoError(t, err)
	td = &TableDef{Createsql: string(raw)}
	pat, ok := GetWriteFilePattern(getExternParamFromTableDef(td))
	require.True(t, ok)
	require.Equal(t, "stage://s/o-%6N.jl", pat)

	// malformed Createsql must not panic, yields no pattern
	td = &TableDef{Createsql: "{not json"}
	_, ok = GetWriteFilePattern(getExternParamFromTableDef(td))
	require.False(t, ok)
}
