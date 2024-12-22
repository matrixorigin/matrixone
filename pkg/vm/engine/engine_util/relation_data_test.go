// Copyright 2021-2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine_util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Add for just pass UT coverage check
func TestEmptyRelationData(t *testing.T) {
	relData := BuildEmptyRelData()
	relData.String()
	require.Panics(t, func() {
		relData.GetShardIDList()
	})
	require.Panics(t, func() {
		relData.GetShardID(0)
	})
	require.Panics(t, func() {
		relData.SetShardID(0, 0)
	})
	require.Panics(t, func() {
		relData.AppendShardID(0)
	})
	require.Panics(t, func() {
		relData.GetBlockInfoSlice()
	})
	require.Panics(t, func() {
		relData.GetBlockInfo(0)
	})
	require.Panics(t, func() {
		relData.SetBlockInfo(0, nil)
	})
	require.Panics(t, func() {
		relData.AppendBlockInfo(nil)
	})
	require.Panics(t, func() {
		relData.AppendBlockInfoSlice(nil)
	})
	require.Panics(t, func() {
		relData.DataSlice(0, 0)
	})
	require.Panics(t, func() {
		relData.GroupByPartitionNum()
	})
	require.Equal(t, 0, relData.DataCnt())

}
