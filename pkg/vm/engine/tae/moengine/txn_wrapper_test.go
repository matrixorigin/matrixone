// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package moengine

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func TestTxnCleintNewWithSnapshot(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	e := NewEngine(tae)
	txnClient := EngineToTxnClient(e)

	txn, err := txnClient.New(context.Background(), timestamp.Timestamp{})
	assert.Nil(t, err)
	id := txn.Txn().ID

	snapshot, err := txn.Snapshot()
	assert.Nil(t, err)

	txn2, err := txnClient.NewWithSnapshot(snapshot)
	assert.Nil(t, err)
	id2 := txn2.Txn().ID
	assert.Equal(t, id2, id)
}
