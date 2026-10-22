//go:build mo_m0

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/stretchr/testify/require"
	"testing"
)

type birthLogtailTxn struct{ txnif.AsyncTxn }

func (birthLogtailTxn) GetPrepareTS() types.TS  { return types.BuildTS(50, 0) }
func (birthLogtailTxn) GetMemo() *txnif.TxnMemo { return &txnif.TxnMemo{} }

func birthLogtailBatch(version uint32, next uint16) *containers.BatchWithVersion {
	b := &containers.BatchWithVersion{Batch: containers.NewBatchWithCapacity(int(next) + 2), Version: version, NextSeqnum: next}
	for i := uint16(0); i < next; i++ {
		v := containers.MakeVector(types.T_int64.ToType(), common.DefaultAllocator)
		v.Append(int64(i+1), false)
		name := "id"
		if i > 0 {
			name = "flag"
		}
		b.AddVector(name, v)
		b.Seqnums = append(b.Seqnums, i)
	}
	row := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	row.Append(types.BuildTestRowid(1, 1), false)
	b.AddVector(catalog.PhyAddrColumnName, row)
	b.Seqnums = append(b.Seqnums, objectio.SEQNUM_ROWID)
	ts := containers.MakeVector(types.T_TS.ToType(), common.DefaultAllocator)
	ts.Append(types.BuildTS(5, 0), false)
	b.AddVector(objectio.DefaultCommitTS_Attr, ts)
	b.Seqnums = append(b.Seqnums, objectio.SEQNUM_COMMITTS)
	return b
}

func TestM0BirthLogtailPullCapturedSchema(t *testing.T) {
	src := birthLogtailBatch(0, 1)
	b := &TableLogtailRespBuilder{ctx: context.Background(), tid: 42, tname: "customer", dataInsBatches: map[uint32]*containers.BatchWithVersion{0: src}}
	defer b.Close()
	response, err := b.BuildResp()
	require.NoError(t, err)
	require.Len(t, response.Commands, 1)
	got := response.Commands[0].Bat.LogtailWriteSchema
	require.Equal(t, &api.LogtailWriteSchema{Version: 0, NextColSeqnum: 1}, got)
	wire, err := response.Commands[0].Bat.Marshal()
	require.NoError(t, err)
	var roundtrip api.Batch
	require.NoError(t, roundtrip.Unmarshal(wire))
	require.Equal(t, got, roundtrip.LogtailWriteSchema)
}

func TestM0BirthLogtailPushSeparatesCapturedVersions(t *testing.T) {
	rt := dbutils.NewRuntime()
	defer rt.VectorPool.Small.Destory()
	defer rt.VectorPool.Transient.Destory()
	b := NewTxnLogtailRespBuilder(rt)
	defer b.Close()
	b.txn = birthLogtailTxn{}
	b.rotateTable(0, "db", "customer", 1, 42, 0)
	a, c := birthLogtailBatch(0, 1), birthLogtailBatch(1, 2)
	defer a.Close()
	defer c.Close()
	b.visitAppendData(a)
	b.visitAppendData(c)
	b.BuildResp()
	require.Len(t, *b.logtails, 1)
	commands := (*b.logtails)[0].Commands
	require.Len(t, commands, 2)
	for i, e := range commands {
		require.Equal(t, uint64(42), e.TableId)
		require.Equal(t, uint32(i), e.Bat.LogtailWriteSchema.Version)
		require.Equal(t, uint32(i+1), e.Bat.LogtailWriteSchema.NextColSeqnum)
		require.Len(t, e.Bat.Attrs, i+3)
	}
}
