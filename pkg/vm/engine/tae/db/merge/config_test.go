// Copyright 2024 Matrix Origin
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

package merge

import (
	"testing"

	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/require"
)

func newTestSchema(colCnt, pkIdx int, config *BasicPolicyConfig) *catalog.Schema {
	schema := catalog.MockSchema(colCnt, pkIdx)
	schema.Extra = &apipb.SchemaExtra{
		MinOsizeQuailifed: config.ObjectMinOsize,
		MaxObjOnerun:      uint32(config.MergeMaxOneRun),
		MaxOsizeMergedObj: config.MaxOsizeMergedObj,
		Hints:             config.MergeHints,
		MinCnMergeSize:    config.MinCNMergeSize,
	}
	return schema
}

func TestString(t *testing.T) {
	require.Equal(t, "minOsizeObj:110.00MB, maxOneRun:16, maxOsizeMergedObj: 128.00MB, offloadToCNSize:78.12GB, hints: []", defaultBasicConfig.String())
}

func TestConfigForTable(t *testing.T) {
	tbl := catalog.MockStaloneTableEntry(0, newTestSchema(1, 0, defaultBasicConfig))
	configProvider := newCustomConfigProvider()
	defaultConfig := configProvider.getConfig(tbl)
	require.Equal(t, defaultBasicConfig.MinCNMergeSize, defaultConfig.MinCNMergeSize)
	require.Equal(t, defaultBasicConfig.MaxOsizeMergedObj, defaultConfig.MaxOsizeMergedObj)
	require.Equal(t, defaultBasicConfig.MergeMaxOneRun, defaultConfig.MergeMaxOneRun)
	require.Equal(t, defaultBasicConfig.ObjectMinOsize, defaultConfig.ObjectMinOsize)
	require.Equal(t, defaultBasicConfig.MergeHints, defaultConfig.MergeHints)

	tbl2 := catalog.MockStaloneTableEntry(0, newTestSchema(1, 0, &BasicPolicyConfig{}))
	configProvider = newCustomConfigProvider()
	config := configProvider.getConfig(tbl2)
	require.Equal(t, defaultBasicConfig.MinCNMergeSize, config.MinCNMergeSize)
	require.Equal(t, defaultBasicConfig.MaxOsizeMergedObj, config.MaxOsizeMergedObj)
	require.Equal(t, defaultBasicConfig.MergeMaxOneRun, config.MergeMaxOneRun)
	require.Equal(t, defaultBasicConfig.ObjectMinOsize, config.ObjectMinOsize)
	require.Equal(t, defaultBasicConfig.MergeHints, config.MergeHints)
}
