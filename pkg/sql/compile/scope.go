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

package compile

import (
	"bytes"
	"errors"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/errno"
	"matrixone/pkg/rpcserver"
	"matrixone/pkg/rpcserver/message"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/createDatabase"
	"matrixone/pkg/sql/op/createTable"
	"matrixone/pkg/sql/op/dropDatabase"
	"matrixone/pkg/sql/op/dropTable"
	"matrixone/pkg/sql/op/insert"
	"matrixone/pkg/sql/op/showDatabases"
	"matrixone/pkg/sql/op/showTables"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/sqlerror"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/pipeline"
	"net"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
)

func (s *Scope) Run(e engine.Engine) error {
	segs := make([]engine.Segment, len(s.Data.Segs))
	cs := make([]uint64, 0, len(s.Data.Refs))
	attrs := make([]string, 0, len(s.Data.Refs))
	{
		for k, v := range s.Data.Refs {
			cs = append(cs, v)
			attrs = append(attrs, k)
		}
	}
	p := pipeline.New(cs, attrs, s.Ins)
	{
		db, err := e.Database(s.Data.DB)
		if err != nil {
			return err
		}
		r, err := db.Relation(s.Data.ID)
		if err != nil {
			return err
		}
		defer r.Close()
		for i, seg := range s.Data.Segs {
			segs[i] = r.Segment(engine.SegmentInfo{
				Id:       seg.Id,
				GroupId:  seg.GroupId,
				TabletId: seg.TabletId,
				Node:     seg.Node,
				Version:  seg.Version,
			}, s.Proc)
		}
	}
	if _, err := p.Run(segs, s.Proc); err != nil {
		return sqlerror.New(errno.SyntaxErrororAccessRuleViolation, err.Error())
	}
	return nil
}

func (s *Scope) MergeRun(e engine.Engine) error {
	var err error
	var wg sync.WaitGroup

	for i := range s.Ss {
		switch s.Ss[i].Magic {
		case Normal:
			wg.Add(1)
			go func(s *Scope) {
				if rerr := s.Run(e); rerr != nil {
					err = rerr
				}
				wg.Done()
			}(s.Ss[i])
		case Merge:
			wg.Add(1)
			go func(s *Scope) {
				if rerr := s.MergeRun(e); rerr != nil {
					err = rerr
				}
				wg.Done()
			}(s.Ss[i])
		case Remote:
			wg.Add(1)
			go func(s *Scope) {
				if rerr := s.RemoteRun(e); rerr != nil {
					err = rerr
				}
				wg.Done()
			}(s.Ss[i])
		}
	}
	p := pipeline.NewMerge(s.Ins)
	if _, rerr := p.RunMerge(s.Proc); rerr != nil {
		err = rerr
	}
	wg.Wait()
	if err != nil {
		return sqlerror.New(errno.SyntaxErrororAccessRuleViolation, err.Error())
	}
	return nil
}

func (s *Scope) RemoteRun(e engine.Engine) error {
	var buf bytes.Buffer

	arg := s.Ins[len(s.Ins)-1].Arg.(*transfer.Argument)
	defer func() {
		arg.Reg.Wg.Add(1)
		arg.Reg.Ch <- nil
		arg.Reg.Wg.Wait()
	}()
	encoder, decoder := rpcserver.NewCodec(1 << 30)
	conn := goetty.NewIOSession(goetty.WithCodec(encoder, decoder))
	defer conn.Close()
	addr, _ := net.ResolveTCPAddr("tcp", s.N.Addr)
	if _, err := conn.Connect(fmt.Sprintf("%v:%v", addr.IP, addr.Port+100), time.Second*3); err != nil {
		return err
	}
	if err := protocol.EncodeScope(Transfer(s), &buf); err != nil {
		return err
	}
	if err := conn.WriteAndFlush(&message.Message{Data: buf.Bytes()}); err != nil {
		return err
	}
	for {
		val, err := conn.Read()
		if err != nil {
			return err
		}
		msg := val.(*message.Message)
		if len(msg.Code) > 0 {
			return errors.New(string(msg.Code))
		}
		if msg.Sid == 1 {
			break
		}
		bat, _, err := protocol.DecodeBatch(val.(*message.Message).Data)
		if err != nil {
			return err
		}
		arg.Reg.Wg.Add(1)
		arg.Reg.Ch <- bat
		arg.Reg.Wg.Wait()
	}
	return nil
}

func (s *Scope) Insert(ts uint64) error {
	o, _ := s.O.(*insert.Insert)
	defer o.R.Close()
	return o.R.Write(ts, o.Bat)
}

func (s *Scope) Explain(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	bat := batch.New(true, []string{"Pipeline"})
	{
		vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
		if err := vec.Append([][]byte{[]byte(s.O.String())}); err != nil {
			return err
		}
		bat.Vecs[0] = vec
	}
	return fill(u, bat)
}

func (s *Scope) CreateTable(ts uint64) error {
	o, _ := s.O.(*createTable.CreateTable)
	if r, err := o.Db.Relation(o.Id); err == nil {
		r.Close()
		if o.Flg {
			return nil
		}
		return sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("table '%v' already exists", o.Id))
	}
	return o.Db.Create(ts, o.Id, o.Defs, o.Pdef, nil, "")
}

func (s *Scope) CreateDatabase(ts uint64) error {
	o, _ := s.O.(*createDatabase.CreateDatabase)
	if _, err := o.E.Database(o.Id); err == nil {
		if o.Flg {
			return nil
		}
		return sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("database '%v' already exists", o.Id))
	}
	return o.E.Create(ts, o.Id, 0)
}

func (s *Scope) DropTable(ts uint64) error {
	o, _ := s.O.(*dropTable.DropTable)
	for i := range o.Dbs {
		db, err := o.E.Database(o.Dbs[i])
		if err != nil {
			if o.Flg {
				continue
			}
			return err
		}
		if r, err := db.Relation(o.Ids[i]); err != nil {
			if o.Flg {
				continue
			}
			return err
		} else {
			r.Close()
		}
		if err := db.Delete(ts, o.Ids[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scope) DropDatabase(ts uint64) error {
	o, _ := s.O.(*dropDatabase.DropDatabase)
	if _, err := o.E.Database(o.Id); err != nil {
		if o.Flg {
			return nil
		}
		return err
	}
	return o.E.Delete(ts, o.Id)
}

func (s *Scope) ShowTables(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	o, _ := s.O.(*showTables.ShowTables)
	bat := batch.New(true, []string{"Table"})
	{
		rs := o.Db.Relations()
		vs := make([][]byte, len(rs))
		for i, r := range rs {
			vs[i] = []byte(r)
		}
		vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
		if err := vec.Append(vs); err != nil {
			return err
		}
		bat.Vecs[0] = vec
	}
	return fill(u, bat)
}

func (s *Scope) ShowDatabases(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	o, _ := s.O.(*showDatabases.ShowDatabases)
	bat := batch.New(true, []string{"Database"})
	{
		rs := o.E.Databases()
		vs := make([][]byte, len(rs))
		for i, r := range rs {
			vs[i] = []byte(r)
		}
		vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
		if err := vec.Append(vs); err != nil {
			return err
		}
		bat.Vecs[0] = vec
	}
	return fill(u, bat)
}
