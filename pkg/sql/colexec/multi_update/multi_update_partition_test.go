// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionMultiUpdateString(t *testing.T) {
	op := &PartitionMultiUpdate{}
	buf := new(bytes.Buffer)
	op.String(buf)
	require.Equal(t, "MultiUpdate: partition_multi_update", buf.String())
}

func TestNewPartitionMultiUpdateFrom(t *testing.T) {
	ps := &PartitionMultiUpdate{
		raw:     &MultiUpdate{},
		tableID: 1,
	}
	op := NewPartitionMultiUpdateFrom(ps)
	require.Equal(t, ps.raw.MultiUpdateCtx, op.(*PartitionMultiUpdate).raw.MultiUpdateCtx)
	require.Equal(t, ps.raw.Action, op.(*PartitionMultiUpdate).raw.Action)
	require.Equal(t, ps.raw.IsOnduplicateKeyUpdate, op.(*PartitionMultiUpdate).raw.IsOnduplicateKeyUpdate)
	require.Equal(t, ps.raw.Engine, op.(*PartitionMultiUpdate).raw.Engine)
	require.Equal(t, ps.tableID, op.(*PartitionMultiUpdate).tableID)
}
