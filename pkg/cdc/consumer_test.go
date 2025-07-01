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

package cdc

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqlexecstub := gostub.Stub(&sqlExecutorFactory, mockSqlExecutorFactory)
	defer sqlexecstub.Reset()

	r := NewTxnRetriever(nil)

	tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 4)
	cnUUID := "a-b-c-d"
	info := &ConsumerInfo{DbName: "db", TableName: "tbl", IndexName: "hnsw_idx"}

	consumer, err := NewIndexConsumer(cnUUID, tblDef, info)
	require.NoError(t, err)
	err = consumer.Consume(ctx, r)
	require.NoError(t, err)
}
