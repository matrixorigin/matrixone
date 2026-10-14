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

// A required membership filter is refused: the search would return candidates
// the filter excludes.
func TestNewReaderRefusesRequiredMembershipFilter(t *testing.T) {
	proc := testutil.NewProcess(t)
	r, err := Hooks{}.NewReader(proc, nil, searchplugin.Request{
		HasMembershipFilter: true, MembershipFilterRequired: true, MembershipFilter: []byte{1},
	})
	require.Nil(t, r)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "got %v", err)
}
