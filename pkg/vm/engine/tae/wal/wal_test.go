// Copyright 2021 Matrix Origin
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

package wal

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "TAEWAL"
)

func TestCheckpoint1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	cfg := &store.StoreCfg{
		RotateChecker: store.NewMaxSizeRotateChecker(int(common.K) * 2),
	}
	driver := NewDriver(dir, "store", cfg)
	defer driver.Close()

	var bs bytes.Buffer
	for i := 0; i < 300; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()

	e := entry.GetBase()
	e.SetType(entry.ETCustomizedStart)
	buf2 := make([]byte, common.K)
	copy(buf2, buf)
	err := e.Unmarshal(buf2)
	assert.Nil(t, err)
	lsn, err := driver.AppendEntry(GroupC, e)
	assert.Nil(t, err)
	err = e.WaitDone()
	assert.Nil(t, err)
	_, err = driver.LoadEntry(GroupC, lsn)
	assert.Nil(t, err)
	assert.Equal(t, lsn, driver.GetCurrSeqNum())
	testutils.WaitExpect(400, func() bool {
		return driver.GetPenddingCnt() == 1
	})
	assert.Equal(t, uint64(1), driver.GetPenddingCnt())

	flush := entry.GetBase()
	flush.SetType(entry.ETCustomizedStart)
	buf3 := make([]byte, common.K*3/2)
	copy(buf3, buf)
	err = flush.Unmarshal(buf3)
	assert.Nil(t, err)
	l, err := driver.AppendEntry(GroupC+1, flush)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), l)
	assert.Nil(t, err)

	index := []*Index{{
		LSN:  lsn,
		CSN:  0,
		Size: 2,
	}}
	_, err = driver.Checkpoint(index)
	assert.Nil(t, err)

	flush2 := entry.GetBase()
	flush2.SetType(entry.ETCustomizedStart)
	buf4 := make([]byte, common.K*3/2)
	copy(buf4, buf)
	err = flush2.Unmarshal(buf4)
	assert.Nil(t, err)
	_, err = driver.AppendEntry(GroupC+1, flush2)
	assert.Nil(t, err)
	err = flush2.WaitDone()
	assert.Nil(t, err)

	err = driver.Compact()
	assert.Nil(t, err)
	_, err = driver.LoadEntry(GroupC, lsn)
	assert.Nil(t, err)

	index = []*Index{{
		LSN:  lsn,
		CSN:  1,
		Size: 2,
	}}
	_, err = driver.Checkpoint(index)
	assert.Nil(t, err)
	testutils.WaitExpect(400, func() bool {
		return lsn == driver.GetCheckpointed()
	})
	assert.Equal(t, lsn, driver.GetCheckpointed())
	assert.Equal(t, lsn, driver.GetCurrSeqNum())
	testutils.WaitExpect(400, func() bool {
		return driver.GetPenddingCnt() == 0
	})
	assert.Equal(t, uint64(0), driver.GetPenddingCnt())

	flush3 := entry.GetBase()
	flush3.SetType(entry.ETCustomizedStart)
	buf5 := make([]byte, common.K*3/2)
	copy(buf5, buf)
	err = flush3.Unmarshal(buf5)
	assert.Nil(t, err)
	_, err = driver.AppendEntry(GroupC+1, flush3)
	assert.Nil(t, err)
	err = flush3.WaitDone()
	assert.Nil(t, err)

	// err = driver.Compact()
	// assert.Nil(t, err)
	// _, err = driver.LoadEntry(GroupC, lsn)
	// assert.NotNil(t, err)
}

func TestCheckpoint2(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	cfg := &store.StoreCfg{
		RotateChecker: store.NewMaxSizeRotateChecker(int(common.K) * 2),
	}
	driver := NewDriver(dir, "store", cfg)
	defer driver.Close()

	var bs bytes.Buffer
	for i := 0; i < 300; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()

	uncommit := entry.GetBase()
	uncommit.SetType(entry.ETCustomizedStart)
	info := &entry.Info{
		Group: entry.GTUncommit,
		Uncommits: []entry.Tid{{
			Group: GroupC,
			Tid:   1,
		}},
	}
	uncommit.SetInfo(info)
	buf1 := make([]byte, common.K)
	copy(buf1, buf)
	err := uncommit.Unmarshal(buf1)
	assert.Nil(t, err)
	lsn, err := driver.AppendEntry(entry.GTUncommit, uncommit)
	assert.Nil(t, err)
	err = uncommit.WaitDone()
	assert.Nil(t, err)
	_, err = driver.LoadEntry(entry.GTUncommit, lsn)
	assert.Nil(t, err)

	commit := entry.GetBase()
	commit.SetType(entry.ETCustomizedStart)
	buf2 := make([]byte, common.K)
	copy(buf2, buf)
	err = commit.Unmarshal(buf2)
	assert.Nil(t, err)
	commitInfo := &entry.Info{
		Group: GroupC,
		TxnId: 1,
	}
	commit.SetInfo(commitInfo)
	lsn, err = driver.AppendEntry(GroupC, commit)
	assert.Nil(t, err)
	err = commit.WaitDone()
	assert.Nil(t, err)
	_, err = driver.LoadEntry(GroupC, lsn)
	assert.Nil(t, err)
	assert.Equal(t, lsn, driver.GetCurrSeqNum())
	testutils.WaitExpect(400, func() bool {
		return driver.GetPenddingCnt() == 1
	})
	assert.Equal(t, uint64(1), driver.GetPenddingCnt())

	flush := entry.GetBase()
	flush.SetType(entry.ETCustomizedStart)
	buf3 := make([]byte, common.K*3/2)
	copy(buf3, buf)
	err = flush.Unmarshal(buf3)
	assert.Nil(t, err)
	l, err := driver.AppendEntry(GroupC+1, flush)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), l)
	assert.Nil(t, err)

	index := []*Index{{
		LSN:  lsn,
		CSN:  0,
		Size: 1,
	}}
	_, err = driver.Checkpoint(index)
	assert.Nil(t, err)
	testutils.WaitExpect(400, func() bool {
		return lsn == driver.GetCheckpointed()
	})
	assert.Equal(t, lsn, driver.GetCheckpointed())
	assert.Equal(t, lsn, driver.GetCurrSeqNum())
	testutils.WaitExpect(400, func() bool {
		return driver.GetPenddingCnt() == 0
	})
	assert.Equal(t, uint64(0), driver.GetPenddingCnt())

	flush2 := entry.GetBase()
	flush2.SetType(entry.ETCustomizedStart)
	buf4 := make([]byte, common.K*3/2)
	copy(buf4, buf)
	err = flush2.Unmarshal(buf4)
	assert.Nil(t, err)
	_, err = driver.AppendEntry(GroupC+1, flush2)
	assert.Nil(t, err)
	err = flush2.WaitDone()
	assert.Nil(t, err)

	err = driver.Compact()
	assert.Nil(t, err)
	_, err = driver.LoadEntry(GroupC, lsn)
	assert.NotNil(t, err)
}
