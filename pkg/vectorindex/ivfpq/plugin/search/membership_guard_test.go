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

package search

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// A required membership filter that is missing is an error; one that is present is
// applied by the search, so the request reaches the search setup.
func TestNewReaderRequiresPresentMembershipFilter(t *testing.T) {
	proc := testutil.NewProcess(t)
	r, err := Hooks{}.NewReader(proc, nil, searchplugin.Request{MembershipFilterRequired: true})
	require.Nil(t, r)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "got %v", err)
	require.ErrorContains(t, err, "requires a membership filter that is missing")

	r, err = Hooks{}.NewReader(proc, nil, searchplugin.Request{
		HasMembershipFilter: true, MembershipFilterRequired: true, MembershipFilter: []byte{1},
	})
	require.Nil(t, r)
	require.ErrorContains(t, err, "ivfpq index search is missing source or index metadata")
	require.True(t, Hooks{}.SupportsMembership())
}
