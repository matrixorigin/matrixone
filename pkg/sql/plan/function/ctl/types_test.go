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

package ctl

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestGetFirstTNResponse(t *testing.T) {
	ctx := context.Background()

	t.Run("first response", func(t *testing.T) {
		response, err := GetFirstTNResponse(ctx, Result{Data: []any{"first", "second"}})
		require.NoError(t, err)
		require.Equal(t, "first", response)
	})

	t.Run("no TN response", func(t *testing.T) {
		response, err := GetFirstTNResponse(ctx, Result{Data: []any{}})
		require.Nil(t, response)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNoAvailableBackend), err)
	})

	t.Run("invalid response data", func(t *testing.T) {
		response, err := GetFirstTNResponse(ctx, Result{Data: "invalid"})
		require.Nil(t, response)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), err)
	})
}
