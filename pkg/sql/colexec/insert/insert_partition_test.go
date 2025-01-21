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

package insert

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionInsertString(t *testing.T) {
	op := &PartitionInsert{}
	buf := new(bytes.Buffer)
	op.String(buf)
	require.Equal(t, "insert: partition_insert", buf.String())
}

func TestNewPartitionInsertFrom(t *testing.T) {
	ps := &PartitionInsert{
		raw:     &Insert{},
		tableID: 1,
	}
	op := NewPartitionInsertFrom(ps)
	require.Equal(t, ps.raw.InsertCtx, op.(*PartitionInsert).raw.InsertCtx)
	require.Equal(t, ps.raw.ToWriteS3, op.(*PartitionInsert).raw.ToWriteS3)
	require.Equal(t, ps.tableID, op.(*PartitionInsert).tableID)
}
