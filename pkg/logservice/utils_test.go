// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testType struct {
	shardID   uint64
	replicaID uint64
	History   map[uint64]uint64
}

func TestGobMarshalUnmarshal(t *testing.T) {
	td := testType{
		shardID:   100,
		replicaID: 200,
		History:   make(map[uint64]uint64),
	}
	td.History[200] = 640
	td.History[350] = 12345

	ss := bytes.NewBuffer(nil)
	assert.NoError(t, gobMarshalTo(ss, &td))

	td2 := testType{
		shardID:   2345,
		replicaID: 4567,
	}
	assert.NoError(t, gobUnmarshalFrom(ss, &td2))

	assert.Equal(t, td.History, td2.History)
	assert.Equal(t, uint64(2345), td2.shardID)
	assert.Equal(t, uint64(4567), td2.replicaID)
}
