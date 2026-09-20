// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package udferr

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticIdentity(t *testing.T) {
	first, second := New("TYPE_CONTRACT: invalid result"), New("TYPE_CONTRACT: invalid result")
	wrapped := errutil.Wrapf(first, "gateway")
	require.ErrorIs(t, wrapped, first)
	require.NotErrorIs(t, wrapped, second)
	var diagnostic *Error
	require.True(t, errors.As(wrapped, &diagnostic))
	require.Equal(t, first, diagnostic)
	require.EqualError(t, wrapped, "gateway: TYPE_CONTRACT: invalid result")
	require.EqualError(t, Newf("invalid dimension %d", 3), "invalid dimension 3")
}
