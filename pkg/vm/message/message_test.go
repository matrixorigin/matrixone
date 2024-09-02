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

package message

import (
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestMessage(t *testing.T) {
	mc := &MessageCenter{
		StmtIDToBoard: make(map[uuid.UUID]*MessageBoard, 64),
		RwMutex:       &sync.Mutex{},
	}
	mb := NewMessageBoard()
	mb.BeforeRunonce()
	SendMessage(JoinMapMsg{JoinMapPtr: nil, Tag: 1}, mb)
	SendMessage(JoinMapMsg{JoinMapPtr: nil, IsShuffle: true, ShuffleIdx: 1, Tag: 2}, mb)
	SendMessage(TopValueMessage{TopValueZM: []byte{}, Tag: 3}, mb)
	SendRuntimeFilter(RuntimeFilterMessage{Typ: RuntimeFilter_PASS}, &plan.RuntimeFilterSpec{}, mb)
	mb.DebugString()
	mb.Reset()
	id, err := uuid.NewUUID()
	require.NoError(t, err)
	mb.SetMultiCN(mc, id)
	mb.DebugString()
	mb.Reset()
	mb.DebugString()
}
