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

package main

import (
	"bytes"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
)

var sampleDir = "/tmp/logstoreSample/replay"
var name = "replay"

func init() {
	os.RemoveAll(sampleDir)
}

func main() {
	s := store.NewStoreWithBatchStoreDriver(sampleDir, name, nil)
	var bs bytes.Buffer
	for i := 0; i < 3000; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()

	for i := 0; i < 5000; i++ {
		e1 := entry.GetBase()
		uncommitInfo := &entry.Info{
			Group: 10,
		}
		e1.SetType(entry.IOET_WALEntry_Uncommitted)
		e1.SetInfo(uncommitInfo)
		n := make([]byte, common.K*100)
		copy(n, buf)
		err := e1.UnmarshalFromNode(n, true)
		if err != nil {
			panic(err)
		}
		_, err = s.Append(10, e1)
		if err != nil {
			panic(err)
		}

		txnInfo := &entry.Info{
			Group: 11,
		}
		e2 := entry.GetBase()
		e2.SetType(entry.IOET_WALEntry_Txn)
		e2.SetInfo(txnInfo)
		n = make([]byte, common.K*100)
		copy(n, buf)
		err = e2.UnmarshalFromNode(n, true)
		if err != nil {
			panic(err)
		}
		cmtLsn, err := s.Append(11, e2)
		if err != nil {
			panic(err)
		}

		cmd := entry.CommandInfo{
			Size:       2,
			CommandIds: []uint32{0, 1},
		}
		cmds := make(map[uint64]entry.CommandInfo)
		cmds[cmtLsn] = cmd
		info := &entry.Info{
			Group: entry.GTCKp,
			Checkpoints: []*entry.CkpRanges{{
				Group:   11,
				Command: cmds,
			}},
		}
		e3 := entry.GetBase()
		e3.SetType(entry.IOET_WALEntry_Checkpoint)
		e3.SetInfo(info)
		_, err = s.Append(entry.GTCKp, e3)
		if err != nil {
			panic(err)
		}
		err = e3.WaitDone()
		if err != nil {
			panic(err)
		}
		err = e1.WaitDone()
		if err != nil {
			panic(err)
		}
		e1.Free()
		e2.Free()
		e3.Free()
	}

	err := s.Close()
	if err != nil {
		panic(err)
	}

	t0 := time.Now()

	s = store.NewStoreWithBatchStoreDriver(sampleDir, name, nil)
	a := func(group uint32, commitId uint64, payload []byte, typ uint16, info any) {
		// fmt.Printf("%s", payload)
	}
	err = s.Replay(a, nil)
	if err != nil {
		panic(err)
	}

	logutil.Infof("Open and replay takes %v", time.Since(t0))
}
