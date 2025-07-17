// Copyright 2025 Matrix Origin
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

package agg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAggAvgContext_Size(t *testing.T) {
	ctx := aggAvgContext(0)
	require.Equal(t, int64(8), ctx.Size())
}

func TestAggAvgDecimalCommonCtx_Size(t *testing.T) {
	ctx := aggAvgDecimalCommonCtx(0)
	require.Equal(t, int64(4), ctx.Size())
}

func TestAvgTwCacheContext_Size(t *testing.T) {
	ctx := AvgTwCacheContext{}
	require.Equal(t, int64(16), ctx.Size())
}

func TestAvgTwCacheDecimalContext_Size(t *testing.T) {
	ctx := AvgTwCacheDecimalContext{}
	require.Equal(t, int64(32), ctx.Size())
}

func TestAvgTwResultContext_Size(t *testing.T) {
	ctx := AvgTwResultContext{}
	require.Equal(t, int64(16), ctx.Size())
}

func TestAvgTwResultDecimalContext_Size(t *testing.T) {
	ctx := AvgTwResultDecimalContext{}
	require.Equal(t, int64(32), ctx.Size())
}

func TestAggVarPopGroupContext_Size(t *testing.T) {
	ctx := aggVarPopGroupContext{}
	require.Equal(t, int64(16), ctx.Size())
}

func TestAggVarPopOfDecimalGroupContext_Size(t *testing.T) {
	ctx := aggVarPopOfDecimalGroupContext{}
	require.Equal(t, int64(32), ctx.Size())
}

func TestAggVarPopOfDecimalCommonContext_Size(t *testing.T) {
	ctx := aggVarPopOfDecimalCommonContext{}
	require.Equal(t, int64(8), ctx.Size())
}

func TestAggSumDecimal_Size(t *testing.T) {
	ctx := aggSumDecimal{}
	require.Equal(t, int64(4), ctx.Size())
}
