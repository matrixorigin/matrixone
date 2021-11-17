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
	"net"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/rpcserver"
	"github.com/matrixorigin/matrixone/pkg/rpcserver/message"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/transfer"
	"github.com/matrixorigin/matrixone/pkg/sql/op/createDatabase"
	"github.com/matrixorigin/matrixone/pkg/sql/op/createIndex"
	"github.com/matrixorigin/matrixone/pkg/sql/op/createTable"
	"github.com/matrixorigin/matrixone/pkg/sql/op/dropDatabase"
	"github.com/matrixorigin/matrixone/pkg/sql/op/dropIndex"
	"github.com/matrixorigin/matrixone/pkg/sql/op/dropTable"
	"github.com/matrixorigin/matrixone/pkg/sql/op/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/op/showColumns"
	"github.com/matrixorigin/matrixone/pkg/sql/op/showDatabases"
	"github.com/matrixorigin/matrixone/pkg/sql/op/showTables"
	"github.com/matrixorigin/matrixone/pkg/sql/protocol"
	"github.com/matrixorigin/matrixone/pkg/sqlerror"
	"github.com/matrixorigin/matrixone/pkg/vectorize/like"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"

	"github.com/fagongzi/goetty"
)

func (s *Scope) Run(e engine.Engine) error {
	segs := make([]engine.Segment, len(s.DataSource.Segments))
	cs := make([]uint64, 0, len(s.DataSource.RefCount))
	attrs := make([]string, 0, len(s.DataSource.RefCount))
	{
		for k, v := range s.DataSource.RefCount {
			cs = append(cs, v)
			attrs = append(attrs, k)
		}
	}
	p := pipeline.New(cs, attrs, s.Instructions)
	{
		db, err := e.Database(s.DataSource.DBName)
		if err != nil {
			return err
		}
		r, err := db.Relation(s.DataSource.RelationName)
		if err != nil {
			return err
		}
		defer r.Close()
		for i, seg := range s.DataSource.Segments {
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

	for i := range s.PreScopes {
		switch s.PreScopes[i].Magic {
		case Normal:
			wg.Add(1)
			go func(s *Scope) {
				if rerr := s.Run(e); rerr != nil {
					err = rerr
				}
				wg.Done()
			}(s.PreScopes[i])
		case Merge:
			wg.Add(1)
			go func(s *Scope) {
				if rerr := s.MergeRun(e); rerr != nil {
					err = rerr
				}
				wg.Done()
			}(s.PreScopes[i])
		case Remote:
			wg.Add(1)
			go func(s *Scope) {
				if rerr := s.RemoteRun(e); rerr != nil {
					err = rerr
				}
				wg.Done()
			}(s.PreScopes[i])
		}
	}
	p := pipeline.NewMerge(s.Instructions)
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

	arg := s.Instructions[len(s.Instructions)-1].Arg.(*transfer.Argument)
	defer func() {
		if arg.Reg.Ch != nil {
			arg.Reg.Wg.Add(1)
			arg.Reg.Ch <- nil
			arg.Reg.Wg.Wait()
		}
	}()
	encoder, decoder := rpcserver.NewCodec(1 << 30)
	conn := goetty.NewIOSession(goetty.WithCodec(encoder, decoder))
	defer conn.Close()
	addr, _ := net.ResolveTCPAddr("tcp", s.NodeInfo.Addr)
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
		if arg.Reg.Ch == nil {
			if bat != nil {
				bat.Clean(s.Proc)
			}
			continue
		}
		arg.Reg.Wg.Add(1)
		arg.Reg.Ch <- bat
		arg.Reg.Wg.Wait()
	}
	return nil
}

func (s *Scope) Insert(ts uint64) (uint64, error) {
	o, _ := s.Operator.(*insert.Insert)
	defer o.R.Close()
	return uint64(o.Bat.Vecs[0].Length()), o.R.Write(ts, o.Bat)
}

func (s *Scope) Explain(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	bat := batch.New(true, []string{"Pipeline"})
	{
		vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
		if err := vec.Append([][]byte{[]byte(s.Operator.String())}); err != nil {
			return err
		}
		bat.Vecs[0] = vec
	}
	return fill(u, bat)
}

func (s *Scope) CreateTable(ts uint64) error {
	o, _ := s.Operator.(*createTable.CreateTable)
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
	o, _ := s.Operator.(*createDatabase.CreateDatabase)
	if _, err := o.E.Database(o.Id); err == nil {
		if o.Flg {
			return nil
		}
		return sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("database '%v' already exists", o.Id))
	}
	return o.E.Create(ts, o.Id, 0)
}

func (s *Scope) DropTable(ts uint64) error {
	o, _ := s.Operator.(*dropTable.DropTable)
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
	o, _ := s.Operator.(*dropDatabase.DropDatabase)
	if _, err := o.E.Database(o.Id); err != nil {
		if o.Flg {
			return nil
		}
		return err
	}
	return o.E.Delete(ts, o.Id)
}

func (s *Scope) ShowTables(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	o, _ := s.Operator.(*showTables.ShowTables)
	bat := batch.New(true, []string{"Table"})
	{
		rs := o.Db.Relations()
		vs := make([][]byte, len(rs))

		// like
		count := 0
		if o.Like == nil {
			for _, r := range rs {
				vs[count] = []byte(r)
				count++
			}
		} else {
			tempSlice := make([]int64, 1)
			for _, r := range rs {
				str := []byte(r)
				if k, _ := like.PureLikePure(str, o.Like, tempSlice); k != nil {
					vs[count] = str
					count++
				}
			}
		}
		vs = vs[:count]

		vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
		if err := vec.Append(vs); err != nil {
			return err
		}
		bat.Vecs[0] = vec
	}
	return fill(u, bat)
}

func (s *Scope) ShowDatabases(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	o, _ := s.Operator.(*showDatabases.ShowDatabases)
	bat := batch.New(true, []string{"Database"})
	{
		rs := o.E.Databases()
		vs := make([][]byte, len(rs))

		// like
		count := 0
		if o.Like == nil {
			for _, r := range rs {
				vs[count] = []byte(r)
				count++
			}
		} else {
			tempSlice := make([]int64, 1)
			for _, r := range rs {
				str := []byte(r)
				if k, _ := like.PureLikePure(str, o.Like, tempSlice); k != nil {
					vs[count] = str
					count++
				}
			}
		}
		vs = vs[:count]

		vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
		if err := vec.Append(vs); err != nil {
			return err
		}
		bat.Vecs[0] = vec
	}
	return fill(u, bat)
}

func (s *Scope) ShowColumns(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	o, _ := s.Operator.(*showColumns.ShowColumns)
	cs := o.R.Attribute()
	var idxs []int
	count := 0
	bat := batch.New(true, []string{"Filed", "Type", "Null", "Key", "Default", "Extra"})
	{
		vs := make([][]byte, len(cs))
		if o.Like == nil {
			for _, c := range cs {
				vs[count] = []byte(c.Name)
				idxs = append(idxs, count)
				count++
			}
		} else {
			tmpSlice := make([]int64, 1)
			for i, c := range cs {
				colName := []byte(c.Name)
				if k, _ := like.PureLikePure(colName, o.Like, tmpSlice); k != nil {
					vs[count] = colName
					idxs = append(idxs, i)
					count++
				}
			}
		}
		vs = vs[:count]

		vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
		if err := vec.Append(vs); err != nil {
			return err
		}
		bat.Vecs[0] = vec
	}
	{
		vs := make([][]byte, count)
		for i, idx := range idxs {
			var str string
			if cs[idx].Type.Width > 0 {
				str = fmt.Sprintf("%s(%v)", strings.ToLower(cs[idx].Type.String()), cs[idx].Type.Width)
			} else {
				str = strings.ToLower(cs[idx].Type.String())
			}
			vs[i] = []byte(str)
		}
		vec := vector.New(types.Type {Oid: types.T_varchar, Size: 24})
		if err := vec.Append(vs); err != nil {
			return err
		}
		bat.Vecs[1] = vec
	}
	{
		vs := make([][]byte, count)
		for i, idx := range idxs {
			if cs[idx].Nullability {
				vs[i] = []byte("Yes")
			} else {
				vs[i] = []byte("No")
			}
		}
		vec := vector.New(types.Type {Oid: types.T_varchar, Size: 24})
		if err := vec.Append(vs); err != nil {
			return err
		}
		bat.Vecs[2] = vec
	}
	{
		vs := make([][]byte, count)
		for i, idx := range idxs {
			if cs[idx].PrimaryKey {
				vs[i] = []byte("Pri")
			} else {
				vs[i] = []byte("")
			}
		}
		vec := vector.New(types.Type {Oid: types.T_varchar, Size: 24})
		if err := vec.Append(vs); err != nil {
			return err
		}
		bat.Vecs[3] = vec
	}
	{
		vs := make([][]byte, count)
		for i, idx := range idxs {
			if cs[idx].HasDefaultExpr() {
				str := fmt.Sprintf("%v", cs[idx].Default.Value)
				vs[i] = []byte(str)
			} else {
				vs[i] = []byte("")
			}
		}
		vec := vector.New(types.Type {Oid: types.T_varchar, Size: 24})
		if err := vec.Append(vs); err != nil {
			return err
		}
		bat.Vecs[4] = vec
	}
	{
		vs := make([][]byte, count)
		for i, _ := range idxs {
			// TODO: Show extra attribute
			vs[i] = []byte("")
		}
		vec := vector.New(types.Type {Oid: types.T_varchar, Size: 24})
		if err := vec.Append(vs); err != nil {
			return err
		}
		bat.Vecs[5] = vec
	}
	return fill(u, bat)
}

func (s *Scope) CreateIndex(ts uint64) error {
	o, _ := s.Operator.(*createIndex.CreateIndex)
	defer o.R.Close()
	err := o.R.CreateIndex(ts, o.Defs)
	if o.IfNotExists && err == errors.New("index already exist") {
		return nil
	}
	return err
}

func (s *Scope) DropIndex(ts uint64) error {
	o, _ := s.Operator.(*dropIndex.DropIndex)
	defer o.R.Close()
	err := o.R.DropIndex(ts, o.IndexName)
	if o.IfNotExists && err == errors.New("index not exist") {
		return nil
	}
	return err
}
