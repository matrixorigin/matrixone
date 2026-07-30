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

package fileservice

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

type testCloser struct {
	calls int
	err   error
}

func (c *testCloser) Close() error {
	c.calls++
	return c.err
}

func TestCloseOnError(t *testing.T) {
	primary := moerr.NewEmptyRangeNoCtx("object")
	closeErr := errors.New("close failed")

	tests := []struct {
		name       string
		initialErr error
		closeErr   error
		wantCalls  int
	}{
		{name: "ownership transferred", wantCalls: 0},
		{name: "close succeeds", initialErr: primary, wantCalls: 1},
		{name: "both operations fail", initialErr: primary, closeErr: closeErr, wantCalls: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			closer := &testCloser{err: tc.closeErr}
			err := tc.initialErr

			closeOnError(&err, closer)

			require.Equal(t, tc.wantCalls, closer.calls)
			switch {
			case tc.initialErr == nil:
				require.NoError(t, err)
			case tc.closeErr == nil:
				require.Same(t, tc.initialErr, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrEmptyRange))
			default:
				require.ErrorIs(t, err, tc.initialErr)
				require.ErrorIs(t, err, tc.closeErr)
			}
		})
	}
}
