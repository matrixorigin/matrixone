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

package plan

import (
	"testing"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestBuildFullTextScanParamsDoesNotForceNativeOnly(t *testing.T) {
	builder := &QueryBuilder{}
	idx := &pbplan.IndexDef{
		Parts:           []string{"body"},
		IndexAlgoParams: `{"parser":"ngram"}`,
	}

	params, err := builder.buildFullTextScanParams(nil, idx)
	require.NoError(t, err)

	var param fulltext.FullTextParserParam
	require.NoError(t, sonic.Unmarshal([]byte(params), &param))
	require.Equal(t, fulltext.FullTextImplementationNative, param.Implementation)
	require.False(t, param.NativeOnlyMode)
	require.Equal(t, "ngram", param.Parser)
	require.Equal(t, []string{"body"}, param.Parts)
}

func TestBuildFullTextScanParamsPreservesExplicitNativeOnly(t *testing.T) {
	builder := &QueryBuilder{}
	idx := &pbplan.IndexDef{
		Parts:           []string{"body"},
		IndexAlgoParams: `{"parser":"ngram","native_only":true}`,
	}

	params, err := builder.buildFullTextScanParams(nil, idx)
	require.NoError(t, err)

	var param fulltext.FullTextParserParam
	require.NoError(t, sonic.Unmarshal([]byte(params), &param))
	require.Equal(t, fulltext.FullTextImplementationNative, param.Implementation)
	require.True(t, param.NativeOnlyMode)
	require.Equal(t, "ngram", param.Parser)
	require.Equal(t, []string{"body"}, param.Parts)
}
