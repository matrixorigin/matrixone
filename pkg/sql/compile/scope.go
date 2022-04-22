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
	"context"
	"fmt"
	"math"
	"net"
	"runtime"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/updateTag"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deleteTag"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergededup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"

	"github.com/fagongzi/goetty"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/rpcserver"
	"github.com/matrixorigin/matrixone/pkg/rpcserver/message"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/protocol"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/plus"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/times"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transform"
	"github.com/matrixorigin/matrixone/pkg/vectorize/like"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	nullString = "NULL"
)

// CreateDatabase do create database work according to create database plan.
func (s *Scope) CreateDatabase(ts uint64) error {
	p, _ := s.Plan.(*plan.CreateDatabase)
	if _, err := p.E.Database(p.Id); err == nil {
		if p.IfNotExistFlag {
			return nil
		}
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("database %s already exists", p.Id))
	}
	return p.E.Create(ts, p.Id, 0)
}

// CreateTable do create table work according to create table plan.
func (s *Scope) CreateTable(ts uint64) error {
	p, _ := s.Plan.(*plan.CreateTable)
	if r, err := p.Db.Relation(p.Id); err == nil {
		r.Close()
		if p.IfNotExistFlag {
			return nil
		}
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("table '%s' already exists", p.Id))
	}
	return p.Db.Create(ts, p.Id, p.Defs)
}

// CreateIndex do create index work according to create index plan
func (s *Scope) CreateIndex(ts uint64) error {
	o, _ := s.Plan.(*plan.CreateIndex)
	if o.HasExist && o.IfNotExistFlag {
		return nil
	}

	defer o.Relation.Close()
	for _, def := range o.Defs {
		if err := o.Relation.AddTableDef(ts, def); err != nil {
			return err
		}
	}
	return nil
}

// DropDatabase do drop database work according to drop index plan
func (s *Scope) DropDatabase(ts uint64) error {
	p, _ := s.Plan.(*plan.DropDatabase)
	if _, err := p.E.Database(p.Id); err != nil {
		if p.IfExistFlag {
			return nil
		}
		return err
	}
	return p.E.Delete(ts, p.Id)
}

// DropTable do drop table work according to drop table plan
func (s *Scope) DropTable(ts uint64) error {
	p, _ := s.Plan.(*plan.DropTable)
	for i := range p.Dbs {
		db, err := p.E.Database(p.Dbs[i])
		if err != nil {
			if p.IfExistFlag {
				continue
			}
			return err
		}
		if r, err := db.Relation(p.Ids[i]); err != nil {
			if p.IfExistFlag {
				continue
			}
			return err
		} else {
			r.Close()
		}
		if err := db.Delete(ts, p.Ids[i]); err != nil {
			return err
		}
	}
	return nil
}

// DropIndex do drop index work according to drop index plan
func (s *Scope) DropIndex(ts uint64) error {
	p, _ := s.Plan.(*plan.DropIndex)
	if p.NotExisted && p.IfExistFlag {
		return nil
	}

	defer p.Relation.Close()
	//return p.Relation.DropIndex(ts, p.Id)
	return nil
}

// ShowDatabases fill batch with all database names
func (s *Scope) ShowDatabases(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	p, _ := s.Plan.(*plan.ShowDatabases)
	attrs := p.ResultColumns()
	bat := batch.New(true, []string{attrs[0].Name})
	// Column 1
	{
		rs := p.E.Databases()
		vs := make([][]byte, len(rs))

		// like
		count := 0
		if p.Like == nil {
			for _, r := range rs {
				vs[count] = []byte(r)
				count++
			}
		} else {
			tempSlice := make([]int64, 1)
			for _, r := range rs {
				str := []byte(r)
				if k, _ := like.BtConstAndConst(str, p.Like, tempSlice); k != nil {
					vs[count] = str
					count++
				}
			}
		}
		vs = vs[:count]

		vec := vector.New(attrs[0].Type)
		if err := vector.Append(vec, vs); err != nil {
			return err
		}
		bat.Vecs[0] = vec
		bat.InitZsOne(count)
	}
	return fill(u, bat)
}

// ShowTables fill batch with all table names in a database
func (s *Scope) ShowTables(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	p, _ := s.Plan.(*plan.ShowTables)
	attrs := p.ResultColumns()
	bat := batch.New(true, []string{attrs[0].Name})
	// Column 1
	{
		rs := p.Db.Relations()
		vs := make([][]byte, len(rs))

		// like
		count := 0
		if p.Like == nil {
			for _, r := range rs {
				vs[count] = []byte(r)
				count++
			}
		} else {
			tempSlice := make([]int64, 1)
			for _, r := range rs {
				str := []byte(r)
				if k, _ := like.BtConstAndConst(str, p.Like, tempSlice); k != nil {
					vs[count] = str
					count++
				}
			}
		}
		vs = vs[:count]

		vec := vector.New(attrs[0].Type)
		if err := vector.Append(vec, vs); err != nil {
			return err
		}
		bat.Vecs[0] = vec
		bat.InitZsOne(count)
	}
	return fill(u, bat)
}

type columnInfo struct {
	name string
	typ  types.Type
	dft  string // default value
	pri  bool   // primary key
}

// ShowColumns fill batch with column information of a table
func (s *Scope) ShowColumns(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	p, _ := s.Plan.(*plan.ShowColumns)
	results := p.ResultColumns() // field, type, null, key, default, extra
	defs := p.Relation.TableDefs()
	attrs := make([]columnInfo, len(defs))

	names := make([]string, 0)
	for _, resultColumn := range results {
		names = append(names, resultColumn.Name)
	}

	count := 0
	tmpSlice := make([]int64, 1)
	for _, def := range defs {
		switch tableOption := def.(type) {
		case *engine.AttributeDef:
			if p.Like != nil { // deal with like rule
				if k, _ := like.BtConstAndConst([]byte(tableOption.Attr.Name), p.Like, tmpSlice); k == nil {
					continue
				}
			}
			attrs[count] = columnInfo{
				name: tableOption.Attr.Name,
				typ:  tableOption.Attr.Type,
				pri:  tableOption.Attr.Primary,
			}
			if tableOption.Attr.HasDefaultExpr() {
				if tableOption.Attr.Default.IsNull {
					attrs[count].dft = nullString
				} else {
					switch tableOption.Attr.Type.Oid {
					case types.T_date, types.T_datetime:
						attrs[count].dft = fmt.Sprintf("%s", tableOption.Attr.Default.Value)
					default:
						attrs[count].dft = fmt.Sprintf("%v", tableOption.Attr.Default.Value)
					}
				}
			}
			count++
		}
	}
	attrs = attrs[:count]

	bat := batch.New(true, names)
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.New(results[i].Type)
	}

	nameVector := make([][]byte, len(attrs))
	typeVector := make([][]byte, len(attrs))
	defaultValueVector := make([][]byte, len(attrs))
	keyVector := make([][]byte, len(attrs))
	emptyVector := make([][]byte, len(attrs))

	for i, attr := range attrs {
		var typ, pri string

		if attr.typ.Width > 0 {
			typ = fmt.Sprintf("%s(%v)", strings.ToLower(attr.typ.String()), attr.typ.Width)
		} else {
			typ = strings.ToLower(attr.typ.String())
		}

		if attr.pri {
			pri = "PRI"
		} else {
			pri = ""
		}

		nameVector[i] = []byte(attr.name)
		typeVector[i] = []byte(typ)
		defaultValueVector[i] = []byte(attr.dft)
		keyVector[i] = []byte(pri)
		emptyVector[i] = []byte("")
	}

	vector.Append(bat.Vecs[0], nameVector)         // field
	vector.Append(bat.Vecs[1], typeVector)         // type
	vector.Append(bat.Vecs[2], emptyVector)        // null todo: not implement
	vector.Append(bat.Vecs[3], keyVector)          // key
	vector.Append(bat.Vecs[4], defaultValueVector) // default
	vector.Append(bat.Vecs[5], emptyVector)        // extra todo: not implement

	bat.InitZsOne(count)
	return fill(u, bat)
}

// ShowCreateTable fill batch with definition of a table
func (s *Scope) ShowCreateTable(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	p, _ := s.Plan.(*plan.ShowCreateTable)
	results := p.ResultColumns()
	tn := p.Relation.ID()
	defs := p.Relation.TableDefs()

	names := make([]string, 0)
	for _, r := range results {
		names = append(names, r.Name)
	}

	bat := batch.New(true, names)
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.New(results[i].Type)
	}

	var buf bytes.Buffer
	buf.WriteString("CREATE TABLE `")
	buf.WriteString(tn)
	buf.WriteString("` (\n")
	var attributeDefs []*engine.AttributeDef
	var indexTableDefs []*engine.IndexTableDef
	primaryIndexDef := new(engine.PrimaryIndexDef)
	for _, d := range defs {
		switch v := d.(type) {
		case *engine.AttributeDef:
			attributeDefs = append(attributeDefs, v)
		case *engine.IndexTableDef:
			indexTableDefs = append(indexTableDefs, v)
		case *engine.PrimaryIndexDef:
			*primaryIndexDef = *v
		}
	}
	prefix := " "
	for _, a := range attributeDefs {
		buf.WriteString(prefix)
		a.Format(&buf)
		prefix = ",\n "
	}

	if len(primaryIndexDef.Names) > 0 {
		buf.WriteString(prefix)
		primaryIndexDef.Format(&buf)
		prefix = ",\n "
	}
	for _, idx := range indexTableDefs {
		buf.WriteString(prefix)
		idx.Format(&buf)
		prefix = ",\n "
	}
	buf.WriteString("\n)")

	tableName := make([][]byte, 1)
	createTable := make([][]byte, 1)
	tableName[0] = []byte(tn)
	createTable[0] = buf.Bytes()

	vector.Append(bat.Vecs[0], tableName)
	vector.Append(bat.Vecs[1], createTable)

	bat.InitZsOne(1)
	return fill(u, bat)
}

// ShowCreateDatabase fill batch with definition of a database
func (s *Scope) ShowCreateDatabase(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	p, _ := s.Plan.(*plan.ShowCreateDatabase)
	if _, err := p.E.Database(p.Id); err != nil {
		if p.IfNotExistFlag {
			return nil
		}
		return err
	}

	results := p.ResultColumns()
	names := make([]string, 0)
	for _, r := range results {
		names = append(names, r.Name)
	}

	bat := batch.New(true, names)
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.New(results[i].Type)
	}

	var buf bytes.Buffer
	buf.WriteString("CREATE DATABASE `")
	buf.WriteString(p.Id)
	buf.WriteString("`")

	dbName := make([][]byte, 1)
	createDatabase := make([][]byte, 1)
	dbName[0] = []byte(p.Id)
	createDatabase[0] = buf.Bytes()

	vector.Append(bat.Vecs[0], dbName)
	vector.Append(bat.Vecs[1], createDatabase)

	bat.InitZsOne(1)
	return fill(u, bat)
}

// Insert will insert a batch into relation and return numbers of affectedRow
func (s *Scope) Insert(ts uint64) (uint64, error) {
	p, _ := s.Plan.(*plan.Insert)
	defer p.Relation.Close()
	return uint64(vector.Length(p.Bat.Vecs[0])), p.Relation.Write(ts, p.Bat)
}

// Delete will delete rows from a single of table
func (s *Scope) Delete(ts uint64, e engine.Engine) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*deleteTag.Argument)
	arg.Ts = ts
	defer arg.Relation.Close()
	if err := s.MergeRun(e); err != nil {
		return 0, err
	}
	return arg.AffectedRows, nil
}

// Update will update rows from a single of table
func (s *Scope) Update(ts uint64, e engine.Engine) (uint64, error) {
	s.Magic = Merge
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*updateTag.Argument)
	arg.Ts = ts
	defer arg.Relation.Close()
	if err := s.MergeRun(e); err != nil {
		return 0, err
	}
	return arg.AffectedRows, nil
}

// Run read data from storage engine and run the instructions of scope.
func (s *Scope) Run(e engine.Engine) (err error) {
	p := pipeline.New(s.DataSource.RefCounts, s.DataSource.Attributes, s.Instructions)
	if _, err = p.Run(s.DataSource.R, s.Proc); err != nil {
		return err
	}
	return nil
}

// MergeRun range and run the scope's pre-scopes by go-routine, and finally run itself to do merge work.
func (s *Scope) MergeRun(e engine.Engine) error {
	var err error

	for i := range s.PreScopes {
		switch s.PreScopes[i].Magic {
		case Normal:
			go func(cs *Scope) {
				if rerr := cs.Run(e); rerr != nil {
					err = rerr
				}
			}(s.PreScopes[i])
		case Merge:
			go func(cs *Scope) {
				if rerr := cs.MergeRun(e); rerr != nil {
					err = rerr
				}
			}(s.PreScopes[i])
		case Remote:
			go func(cs *Scope) {
				if rerr := cs.RemoteRun(e); rerr != nil {
					err = rerr
				}
			}(s.PreScopes[i])
		case Parallel:
			go func(cs *Scope) {
				if rerr := cs.ParallelRun(e); rerr != nil {
					err = rerr
				}
			}(s.PreScopes[i])
		}
	}
	p := pipeline.NewMerge(s.Instructions)
	if _, rerr := p.RunMerge(s.Proc); rerr != nil {
		err = rerr
	}
	return err
}

// RemoteRun send the scope to a remote node (if target node is itself, it is same to function ParallelRun) and run it.
func (s *Scope) RemoteRun(e engine.Engine) error {
	var buf bytes.Buffer

	if Address == s.NodeInfo.Addr {
		return s.ParallelRun(e)
	}
	ps := Transfer(s)
	err := protocol.EncodeScope(ps, &buf)
	if err != nil {
		return err
	}
	arg := s.Instructions[len(s.Instructions)-1].Arg.(*connector.Argument)
	encoder, decoder := rpcserver.NewCodec(1 << 30)
	conn := goetty.NewIOSession(goetty.WithCodec(encoder, decoder))
	defer conn.Close()
	addr, _ := net.ResolveTCPAddr("tcp", s.NodeInfo.Addr)
	if _, err := conn.Connect(fmt.Sprintf("%v:%v", addr.IP, addr.Port+100), time.Second*3); err != nil {
		select {
		case <-arg.Reg.Ctx.Done():
		case arg.Reg.Ch <- nil:
		}
		return err
	}
	if err := conn.WriteAndFlush(&message.Message{Data: buf.Bytes()}); err != nil {
		select {
		case <-arg.Reg.Ctx.Done():
		case arg.Reg.Ch <- nil:
		}
		return err
	}
	for {
		val, err := conn.Read()
		if err != nil {
			select {
			case <-arg.Reg.Ctx.Done():
			case arg.Reg.Ch <- nil:
			}
			return err
		}
		msg := val.(*message.Message)
		if len(msg.Code) > 0 {
			select {
			case <-arg.Reg.Ctx.Done():
			case arg.Reg.Ch <- nil:
			}
			return errors.New(errno.SystemError, string(msg.Code))
		}
		if msg.Sid == 1 {
			select {
			case <-arg.Reg.Ctx.Done():
			case arg.Reg.Ch <- nil:
			}
			break
		}
		bat, _, err := protocol.DecodeBatchWithProcess(val.(*message.Message).Data, s.Proc)
		if err != nil {
			select {
			case <-arg.Reg.Ctx.Done():
			case arg.Reg.Ch <- nil:
			}
			return err
		}
		if arg.Reg.Ch == nil {
			if bat != nil {
				batch.Clean(bat, s.Proc.Mp)
			}
			continue
		}
		select {
		case <-arg.Reg.Ctx.Done():
		case arg.Reg.Ch <- bat:
		}
	}
	return nil
}

// ParallelRun try to execute the scope in parallel way.
func (s *Scope) ParallelRun(e engine.Engine) error {
	var jop *join.Argument
	var top *times.Argument

	{
		for _, in := range s.Instructions {
			if in.Op == vm.Join {
				jop = in.Arg.(*join.Argument)
			}
			if in.Op == vm.Times {
				top = in.Arg.(*times.Argument)
			}
		}
	}
	if jop != nil {
		if s.DataSource == nil {
			return s.RunCQWithSubquery(e, jop)
		}
		return s.RunCQ(e, jop)
	}
	if top != nil {
		if s.DataSource == nil {
			return s.RunCAQWithSubquery(e, top)
		}
		return s.RunCAQ(e, top)
	}
	switch t := s.Instructions[0].Arg.(type) {
	case *transform.Argument:
		if t.Typ == transform.Bare {
			return s.RunQ(e)
		}
		return s.RunAQ(e)
	}
	return nil
}

// RunQ run the scope which sql is a query for single table and without any aggregate functions
// it will build a multi-layer merging structure according to the scope, and finally run it.
// For an example, if the input scope is
//		[transform -> order -> push]
// and we assume that there are 4 Cores at the node, we will convert it to be
//		s1: [transform -> order -> push]  - - > m1
//		s2: [transform -> order -> push]  - - > m1
//		s3: [transform -> order -> push]  - - > m2
//		s4: [transform -> order -> push]  - - > m2
//		m1 : [mergeOrder -> push] - -  > m3
// 		m2 : [mergeOrder -> push] - -  > m3
//		m3 : [mergeOrder -> push] - - > top scope
func (s *Scope) RunQ(e engine.Engine) error {
	var rds []engine.Reader
	var cond extend.Extend

	{
		for _, in := range s.Instructions {
			if in.Op == vm.Transform {
				arg := in.Arg.(*transform.Argument)
				if arg.Restrict != nil {
					cond = arg.Restrict.E
				}
			}
		}
	}
	mcpu := runtime.NumCPU()
	{
		db, err := e.Database(s.DataSource.SchemaName)
		if err != nil {
			return err
		}
		rel, err := db.Relation(s.DataSource.RelationName)
		if err != nil {
			return err
		}
		defer rel.Close()
		rds = rel.NewReader(mcpu, cond, s.NodeInfo.Data)
	}
	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = &Scope{
			Magic: Normal,
			DataSource: &Source{
				R:            rds[i],
				IsMerge:      s.DataSource.IsMerge,
				SchemaName:   s.DataSource.SchemaName,
				RelationName: s.DataSource.RelationName,
				RefCounts:    s.DataSource.RefCounts,
				Attributes:   s.DataSource.Attributes,
			},
		}
		ss[i].Proc = process.New(mheap.New(guest.New(s.Proc.Mp.Gm.Limit, s.Proc.Mp.Gm.Mmu)))
		ss[i].Proc.Id = s.Proc.Id
		ss[i].Proc.Lim = s.Proc.Lim
	}
	{
		var flg bool

		for i, in := range s.Instructions {
			if flg {
				break
			}
			switch in.Op {
			case vm.Top:
				flg = true
				arg := in.Arg.(*top.Argument)
				s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
				s.Instructions[0] = vm.Instruction{
					Op: vm.MergeTop,
					Arg: &mergetop.Argument{
						Fields: arg.Fs,
						Limit:  arg.Limit,
					},
				}
				for i := range ss {
					ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
						Op: vm.Top,
						Arg: &top.Argument{
							Fs:    arg.Fs,
							Limit: arg.Limit,
						},
					})
				}
				for len(ss) > 3 {
					ss = newMergeTopScope(ss, &mergetop.Argument{Fields: arg.Fs, Limit: arg.Limit}, s.Proc)
				}
			case vm.Order:
				flg = true
				arg := in.Arg.(*order.Argument)
				s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
				s.Instructions[0] = vm.Instruction{
					Op: vm.MergeOrder,
					Arg: &mergeorder.Argument{
						Fields: arg.Fs,
					},
				}
				for i := range ss {
					ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
						Op: vm.Order,
						Arg: &order.Argument{
							Fs: arg.Fs,
						},
					})
				}
				for len(ss) > 3 {
					ss = newMergeOrderScope(ss, &mergeorder.Argument{Fields: arg.Fs}, s.Proc)
				}
			case vm.Dedup:
				flg = true
				s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
				s.Instructions[0] = vm.Instruction{
					Op:  vm.MergeDedup,
					Arg: &mergededup.Argument{},
				}
				for i := range ss {
					ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
						Op:  vm.Dedup,
						Arg: &dedup.Argument{},
					})
				}
				for len(ss) > 3 {
					ss = newMergeDedupScope(ss, s.Proc)
				}
			case vm.Limit:
				flg = true
				arg := in.Arg.(*limit.Argument)
				s.Instructions = append(s.Instructions[:1], s.Instructions[i+1:]...)
				s.Instructions[0] = vm.Instruction{
					Op: vm.MergeLimit,
					Arg: &mergelimit.Argument{
						Limit: arg.Limit,
					},
				}
				for i := range ss {
					ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
						Op: vm.Limit,
						Arg: &limit.Argument{
							Limit: arg.Limit,
						},
					})
				}
				for len(ss) > 3 {
					ss = newMergeLimitScope(ss, arg.Limit, s.Proc)
				}
			default:
				for i := range ss {
					ss[i].Instructions = append(ss[i].Instructions, dupInstruction(in))
				}
			}
		}
		if !flg {
			for i := range ss {
				ss[i].Instructions = ss[i].Instructions[:len(ss[i].Instructions)-1]
			}
			s.Instructions[0] = vm.Instruction{
				Op:  vm.Merge,
				Arg: &merge.Argument{},
			}
			s.Instructions[1] = s.Instructions[len(s.Instructions)-1]
			s.Instructions = s.Instructions[:2]
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.Magic = Merge
	s.PreScopes = ss
	s.Proc.Cancel = cancel
	s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ss))
	{
		for i := 0; i < len(ss); i++ {
			s.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Ctx: ctx,
				Ch:  make(chan *batch.Batch, 1),
			}
		}
	}
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: s.Proc.Mp.Gm,
				Reg: s.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return s.MergeRun(e)
}

// RunAQ run the scope which sql is a query for single table with aggregate functions
func (s *Scope) RunAQ(e engine.Engine) error {
	var rds []engine.Reader

	mcpu := runtime.NumCPU()
	{
		db, err := e.Database(s.DataSource.SchemaName)
		if err != nil {
			return err
		}
		rel, err := db.Relation(s.DataSource.RelationName)
		if err != nil {
			return err
		}
		defer rel.Close()
		rds = rel.NewReader(mcpu, nil, s.NodeInfo.Data)
	}
	ss := make([]*Scope, mcpu)
	arg := s.Instructions[0].Arg.(*transform.Argument)
	for i := 0; i < mcpu; i++ {
		ss[i] = &Scope{
			Magic: Normal,
			DataSource: &Source{
				R:            rds[i],
				IsMerge:      s.DataSource.IsMerge,
				SchemaName:   s.DataSource.SchemaName,
				RelationName: s.DataSource.RelationName,
				RefCounts:    s.DataSource.RefCounts,
				Attributes:   s.DataSource.Attributes,
			},
		}
		ss[i].Instructions = append(ss[i].Instructions, dupInstruction(s.Instructions[0]))
		ss[i].Proc = process.New(mheap.New(guest.New(s.Proc.Mp.Gm.Limit, s.Proc.Mp.Gm.Mmu)))
		ss[i].Proc.Id = s.Proc.Id
		ss[i].Proc.Lim = s.Proc.Lim
	}
	if len(ss) > 3 {
		ss = newMergeScope(ss, arg.Typ, s.Proc)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.Magic = Merge
	s.PreScopes = ss
	s.Instructions[0] = vm.Instruction{
		Op:  vm.Plus,
		Arg: &plus.Argument{Typ: arg.Typ},
	}
	s.Proc.Cancel = cancel
	s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ss))
	{
		for i := 0; i < len(ss); i++ {
			s.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Ctx: ctx,
				Ch:  make(chan *batch.Batch, 1),
			}
		}
	}
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: s.Proc.Mp.Gm,
				Reg: s.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return s.MergeRun(e)
}

// RunCQ run the scope which sql is a query for conjunctive query
func (s *Scope) RunCQ(e engine.Engine, op *join.Argument) error {
	var err error
	var bats []*batch.Batch
	var rds []engine.Reader

	{ // fill batchs
		bats = make([]*batch.Batch, len(op.Vars))
		ctx, cancel := context.WithCancel(context.Background())
		s.Proc.Cancel = cancel
		s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(s.PreScopes))
		{
			for i := 0; i < len(s.PreScopes); i++ {
				s.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
					Ctx: ctx,
					Ch:  make(chan *batch.Batch, 1),
				}
			}
		}
		for i := range s.PreScopes {
			flg := true
			for j, in := range s.PreScopes[i].Instructions {
				if in.Op == vm.Connector {
					flg = false
					s.PreScopes[i].Instructions[j].Arg = &connector.Argument{
						Mmu: s.Proc.Mp.Gm,
						Reg: s.Proc.Reg.MergeReceivers[i],
					}
				}
			}
			if flg {
				s.PreScopes[i].Instructions = append(s.PreScopes[i].Instructions, vm.Instruction{
					Op: vm.Connector,
					Arg: &connector.Argument{
						Mmu: s.Proc.Mp.Gm,
						Reg: s.Proc.Reg.MergeReceivers[i],
					},
				})
			}
		}
		for i := range s.PreScopes {
			switch s.PreScopes[i].Magic {
			case Normal:
				go func(s *Scope) {
					if rerr := s.Run(e); rerr != nil {
						err = rerr
					}
				}(s.PreScopes[i])
			case Merge:
				go func(s *Scope) {
					if rerr := s.MergeRun(e); rerr != nil {
						err = rerr
					}
				}(s.PreScopes[i])
			case Remote:
				go func(s *Scope) {
					if rerr := s.RemoteRun(e); rerr != nil {
						err = rerr
					}
				}(s.PreScopes[i])
			case Parallel:
				go func(s *Scope) {
					if rerr := s.ParallelRun(e); rerr != nil {
						err = rerr
					}
				}(s.PreScopes[i])
			}
		}
		if err != nil {
			for i, in := range s.Instructions {
				if in.Op == vm.Connector {
					arg := s.Instructions[i].Arg.(*connector.Argument)
					select {
					case <-arg.Reg.Ctx.Done():
					case arg.Reg.Ch <- nil:
					}
					break
				}
			}
			return err
		}
		flg := false // check for empty tables
		for i := 0; i < len(s.Proc.Reg.MergeReceivers); i++ {
			reg := s.Proc.Reg.MergeReceivers[i]
			for {
				bat := <-reg.Ch
				if bat == nil {
					break
				}
				if len(bat.Zs) == 0 {
					continue
				}
				if bats[i] == nil {
					bats[i] = bat
				} else {
					if bats[i], err = bats[i].Append(s.Proc.Mp, bat); err != nil {
						for i := range bats {
							if bats[i] != nil {
								batch.Clean(bats[i], s.Proc.Mp)
							}
						}
						return err
					}
				}
			}
			if bats[i] == nil {
				flg = true
			}
		}
		if flg {
			for i, in := range s.Instructions {
				if in.Op == vm.Connector {
					arg := s.Instructions[i].Arg.(*connector.Argument)
					select {
					case <-arg.Reg.Ctx.Done():
					case arg.Reg.Ch <- nil:
					}
					break
				}
			}
			for i := range bats {
				if bats[i] != nil {
					batch.Clean(bats[i], s.Proc.Mp)
				}
			}
			return nil
		}
		constructViews(bats, op.Vars)
		op.Bats = bats
		defer func() {
			for i := range bats {
				if bats[i] != nil {
					batch.Clean(bats[i], s.Proc.Mp)
				}
			}
		}()
	}
	mcpu := runtime.NumCPU()
	{
		db, err := e.Database(s.DataSource.SchemaName)
		if err != nil {
			return err
		}
		rel, err := db.Relation(s.DataSource.RelationName)
		if err != nil {
			return err
		}
		defer rel.Close()
		rds = rel.NewReader(mcpu, nil, s.NodeInfo.Data)
	}
	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = &Scope{
			Magic: Normal,
			DataSource: &Source{
				R:            rds[i],
				IsMerge:      s.DataSource.IsMerge,
				SchemaName:   s.DataSource.SchemaName,
				RelationName: s.DataSource.RelationName,
				RefCounts:    s.DataSource.RefCounts,
				Attributes:   s.DataSource.Attributes,
			},
		}
		ss[i].Proc = process.New(mheap.New(guest.New(s.Proc.Mp.Gm.Limit, s.Proc.Mp.Gm.Mmu)))
		ss[i].Proc.Id = s.Proc.Id
		ss[i].Proc.Lim = s.Proc.Lim
		{
			for _, in := range s.Instructions {
				ss[i].Instructions = append(ss[i].Instructions, dupInstruction(in))
				if in.Op == vm.Join {
					break
				}
			}
		}
	}
	{
		j := 0
		for k, in := range s.Instructions {
			j = k
			if in.Op == vm.Join {
				break
			}
		}
		s.Instructions = s.Instructions[j+1:]
	}
	rs := &Scope{Magic: Merge}
	rs.PreScopes = ss
	rs.Instructions = append(rs.Instructions, vm.Instruction{
		Op:  vm.Merge,
		Arg: &merge.Argument{},
	})
	rs.Instructions = append(rs.Instructions, s.Instructions...)
	ctx, cancel := context.WithCancel(context.Background())
	rs.Proc = process.New(mheap.New(guest.New(s.Proc.Mp.Gm.Limit, s.Proc.Mp.Gm.Mmu)))
	rs.Proc.Cancel = cancel
	rs.Proc.Id = s.Proc.Id
	rs.Proc.Lim = s.Proc.Lim
	rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ss))
	{
		for i := 0; i < len(ss); i++ {
			rs.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Ctx: ctx,
				Ch:  make(chan *batch.Batch, 1),
			}
		}
	}
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return rs.MergeRun(e)
}

// RunCQWithSubquery run the scope which sql is a query for conjunctive query and fact table is subquery
func (s *Scope) RunCQWithSubquery(e engine.Engine, op *join.Argument) error {
	var err error
	var bats []*batch.Batch

	{ // fill batchs
		rs := new(Scope)
		bats = make([]*batch.Batch, len(op.Vars))
		rs.Proc = process.New(mheap.New(guest.New(s.Proc.Mp.Gm.Limit, s.Proc.Mp.Gm.Mmu)))
		rs.PreScopes = s.PreScopes[1:]
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc.Cancel = cancel
		rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(rs.PreScopes))
		{
			for i := 0; i < len(rs.PreScopes); i++ {
				rs.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
					Ctx: ctx,
					Ch:  make(chan *batch.Batch, 1),
				}
			}
		}
		for i := range rs.PreScopes {
			flg := true
			for j, in := range rs.PreScopes[i].Instructions {
				if in.Op == vm.Connector {
					flg = false
					rs.PreScopes[i].Instructions[j].Arg = &connector.Argument{
						Mmu: rs.Proc.Mp.Gm,
						Reg: rs.Proc.Reg.MergeReceivers[i],
					}
				}
			}
			if flg {
				rs.PreScopes[i].Instructions = append(rs.PreScopes[i].Instructions, vm.Instruction{
					Op: vm.Connector,
					Arg: &connector.Argument{
						Mmu: rs.Proc.Mp.Gm,
						Reg: rs.Proc.Reg.MergeReceivers[i],
					},
				})
			}
		}
		for i := range rs.PreScopes {
			switch rs.PreScopes[i].Magic {
			case Normal:
				go func(s *Scope) {
					if rerr := s.Run(e); rerr != nil {
						err = rerr
					}
				}(rs.PreScopes[i])
			case Merge:
				go func(s *Scope) {
					if rerr := s.MergeRun(e); rerr != nil {
						err = rerr
					}
				}(rs.PreScopes[i])
			case Remote:
				go func(s *Scope) {
					if rerr := s.RemoteRun(e); rerr != nil {
						err = rerr
					}
				}(rs.PreScopes[i])
			case Parallel:
				go func(s *Scope) {
					if rerr := s.ParallelRun(e); rerr != nil {
						err = rerr
					}
				}(rs.PreScopes[i])
			}
		}
		if err != nil {
			for i, in := range s.Instructions {
				if in.Op == vm.Connector {
					arg := s.Instructions[i].Arg.(*connector.Argument)
					select {
					case <-arg.Reg.Ctx.Done():
					case arg.Reg.Ch <- nil:
					}
					break
				}
			}
			return err
		}
		flg := false // check for empty tables
		for i := 0; i < len(rs.Proc.Reg.MergeReceivers); i++ {
			reg := rs.Proc.Reg.MergeReceivers[i]
			for {
				bat := <-reg.Ch
				if bat == nil {
					break
				}
				if len(bat.Zs) == 0 {
					continue
				}
				if bats[i] == nil {
					bats[i] = bat
				} else {
					if bats[i], err = bats[i].Append(rs.Proc.Mp, bat); err != nil {
						for i := range bats {
							if bats[i] != nil {
								batch.Clean(bats[i], rs.Proc.Mp)
							}
						}
						return err
					}
				}
			}
			if bats[i] == nil {
				flg = true
			}
		}
		if flg {
			for i, in := range s.Instructions {
				if in.Op == vm.Connector {
					arg := s.Instructions[i].Arg.(*connector.Argument)
					select {
					case <-arg.Reg.Ctx.Done():
					case arg.Reg.Ch <- nil:
					}
					break
				}
			}
			for i := range bats {
				if bats[i] != nil {
					batch.Clean(bats[i], s.Proc.Mp)
				}
			}
			return nil
		}
		constructViews(bats, op.Vars)
		op.Bats = bats
		defer func() {
			for i := range bats {
				if bats[i] != nil {
					batch.Clean(bats[i], s.Proc.Mp)
				}
			}
		}()
	}
	s.Magic = Merge
	s.PreScopes = s.PreScopes[:1]
	return s.MergeRun(e)
}

// RunCAQ run the scope which sql is a query for conjunctive aggregation query
func (s *Scope) RunCAQ(e engine.Engine, op *times.Argument) error {
	var err error
	var bats []*batch.Batch
	var rds []engine.Reader

	{ // fill batchs
		bats = make([]*batch.Batch, len(op.Vars))
		ctx, cancel := context.WithCancel(context.Background())
		s.Proc.Cancel = cancel
		s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(s.PreScopes))
		{
			for i := 0; i < len(s.PreScopes); i++ {
				s.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
					Ctx: ctx,
					Ch:  make(chan *batch.Batch, 1),
				}
			}
		}
		for i := range s.PreScopes {
			flg := true
			for j, in := range s.PreScopes[i].Instructions {
				if in.Op == vm.Connector {
					flg = false
					s.PreScopes[i].Instructions[j].Arg = &connector.Argument{
						Mmu: s.Proc.Mp.Gm,
						Reg: s.Proc.Reg.MergeReceivers[i],
					}
				}
			}
			if flg {
				s.PreScopes[i].Instructions = append(s.PreScopes[i].Instructions, vm.Instruction{
					Op: vm.Connector,
					Arg: &connector.Argument{
						Mmu: s.Proc.Mp.Gm,
						Reg: s.Proc.Reg.MergeReceivers[i],
					},
				})

			}
		}
		for i := range s.PreScopes {
			switch s.PreScopes[i].Magic {
			case Normal:
				go func(s *Scope) {
					if rerr := s.Run(e); rerr != nil {
						err = rerr
					}
				}(s.PreScopes[i])
			case Merge:
				go func(s *Scope) {
					if rerr := s.MergeRun(e); rerr != nil {
						err = rerr
					}
				}(s.PreScopes[i])
			case Remote:
				go func(s *Scope) {
					if rerr := s.RemoteRun(e); rerr != nil {
						err = rerr
					}
				}(s.PreScopes[i])
			case Parallel:
				go func(s *Scope) {
					if rerr := s.ParallelRun(e); rerr != nil {
						err = rerr
					}
				}(s.PreScopes[i])
			}
		}
		if err != nil {
			for i, in := range s.Instructions {
				if in.Op == vm.Connector {
					arg := s.Instructions[i].Arg.(*connector.Argument)
					select {
					case <-arg.Reg.Ctx.Done():
					case arg.Reg.Ch <- nil:
					}
					break
				}
			}
			return err
		}
		flg := false // check for empty tables
		for i := 0; i < len(s.Proc.Reg.MergeReceivers); i++ {
			reg := s.Proc.Reg.MergeReceivers[i]
			for {
				bat := <-reg.Ch
				if bat == nil {
					break
				}
				if len(bat.Zs) == 0 {
					continue
				}
				if bats[i] == nil {
					bats[i] = bat
				} else {
					if bats[i], err = bats[i].Append(s.Proc.Mp, bat); err != nil {
						for i := range bats {
							if bats[i] != nil {
								batch.Clean(bats[i], s.Proc.Mp)
							}
						}
						return err
					}
				}
			}
			if bats[i] == nil {
				flg = true
			}
		}
		if flg {
			for i, in := range s.Instructions {
				if in.Op == vm.Connector {
					arg := s.Instructions[i].Arg.(*connector.Argument)
					select {
					case <-arg.Reg.Ctx.Done():
					case arg.Reg.Ch <- nil:
					}
					break
				}
			}
			for i := range bats {
				if bats[i] != nil {
					batch.Clean(bats[i], s.Proc.Mp)
				}
			}
			return nil
		}
		constructViews(bats, op.Vars)
		op.Bats = bats
		defer func() {
			for i := range bats {
				if bats[i] != nil {
					batch.Clean(bats[i], s.Proc.Mp)
				}
			}
		}()
	}
	mcpu := runtime.NumCPU()
	{
		db, err := e.Database(s.DataSource.SchemaName)
		if err != nil {
			return err
		}
		rel, err := db.Relation(s.DataSource.RelationName)
		if err != nil {
			return err
		}
		defer rel.Close()
		rds = rel.NewReader(mcpu, nil, s.NodeInfo.Data)
	}
	ss := make([]*Scope, mcpu)
	for i := 0; i < mcpu; i++ {
		ss[i] = &Scope{
			Magic: Normal,
			DataSource: &Source{
				R:            rds[i],
				IsMerge:      s.DataSource.IsMerge,
				SchemaName:   s.DataSource.SchemaName,
				RelationName: s.DataSource.RelationName,
				RefCounts:    s.DataSource.RefCounts,
				Attributes:   s.DataSource.Attributes,
			},
		}
		ss[i].Proc = process.New(mheap.New(guest.New(s.Proc.Mp.Gm.Limit, s.Proc.Mp.Gm.Mmu)))
		ss[i].Proc.Id = s.Proc.Id
		ss[i].Proc.Lim = s.Proc.Lim
		{
			for _, in := range s.Instructions {
				ss[i].Instructions = append(ss[i].Instructions, dupInstruction(in))
				if in.Op == vm.Times {
					break
				}
			}
		}
	}
	if len(ss) > 3 {
		if len(op.Result) == 0 {
			ss = newMergeScope(ss, plus.BoundVars, s.Proc)
		} else {
			ss = newMergeScope(ss, plus.FreeVarsAndBoundVars, s.Proc)
		}
	}
	{
		j := 0
		for k, in := range s.Instructions {
			j = k
			if in.Op == vm.Times {
				break
			}
		}
		s.Instructions = s.Instructions[j+1:]
	}
	rs := &Scope{Magic: Merge}
	rs.PreScopes = ss

	if len(op.Result) == 0 {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Plus,
			Arg: &plus.Argument{
				Typ: plus.BoundVars,
			},
		})
	} else {
		rs.Instructions = append(rs.Instructions, vm.Instruction{
			Op: vm.Plus,
			Arg: &plus.Argument{
				Typ: plus.FreeVarsAndBoundVars,
			},
		})
	}
	rs.Instructions = append(rs.Instructions, s.Instructions...)
	ctx, cancel := context.WithCancel(context.Background())
	rs.Proc = process.New(mheap.New(guest.New(s.Proc.Mp.Gm.Limit, s.Proc.Mp.Gm.Mmu)))
	rs.Proc.Cancel = cancel
	rs.Proc.Id = s.Proc.Id
	rs.Proc.Lim = s.Proc.Lim
	rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ss))
	{
		for i := 0; i < len(ss); i++ {
			rs.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Ctx: ctx,
				Ch:  make(chan *batch.Batch, 1),
			}
		}
	}
	for i := range ss {
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: rs.Proc.Mp.Gm,
				Reg: rs.Proc.Reg.MergeReceivers[i],
			},
		})
	}
	return rs.MergeRun(e)
}

// RunCAQWithSubquery run the scope which sql is a query for conjunctive aggregation query
func (s *Scope) RunCAQWithSubquery(e engine.Engine, op *times.Argument) error {
	var err error
	var bats []*batch.Batch

	{ // fill batchs
		rs := new(Scope)
		bats = make([]*batch.Batch, len(op.Vars))
		rs.Proc = process.New(mheap.New(guest.New(s.Proc.Mp.Gm.Limit, s.Proc.Mp.Gm.Mmu)))
		rs.PreScopes = s.PreScopes[1:]
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc.Cancel = cancel
		rs.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(rs.PreScopes))
		{
			for i := 0; i < len(rs.PreScopes); i++ {
				rs.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
					Ctx: ctx,
					Ch:  make(chan *batch.Batch, 1),
				}
			}
		}
		for i := range rs.PreScopes {
			flg := true
			for j, in := range rs.PreScopes[i].Instructions {
				if in.Op == vm.Connector {
					flg = false
					rs.PreScopes[i].Instructions[j].Arg = &connector.Argument{
						Mmu: rs.Proc.Mp.Gm,
						Reg: rs.Proc.Reg.MergeReceivers[i],
					}
				}
			}
			if flg {
				rs.PreScopes[i].Instructions = append(rs.PreScopes[i].Instructions, vm.Instruction{
					Op: vm.Connector,
					Arg: &connector.Argument{
						Mmu: rs.Proc.Mp.Gm,
						Reg: rs.Proc.Reg.MergeReceivers[i],
					},
				})
			}
		}
		for i := range rs.PreScopes {
			switch rs.PreScopes[i].Magic {
			case Normal:
				go func(s *Scope) {
					if rerr := s.Run(e); rerr != nil {
						err = rerr
					}
				}(rs.PreScopes[i])
			case Merge:
				go func(s *Scope) {
					if rerr := s.MergeRun(e); rerr != nil {
						err = rerr
					}
				}(rs.PreScopes[i])
			case Remote:
				go func(s *Scope) {
					if rerr := s.RemoteRun(e); rerr != nil {
						err = rerr
					}
				}(rs.PreScopes[i])
			case Parallel:
				go func(s *Scope) {
					if rerr := s.ParallelRun(e); rerr != nil {
						err = rerr
					}
				}(rs.PreScopes[i])
			}
		}
		if err != nil {
			for i, in := range s.Instructions {
				if in.Op == vm.Connector {
					arg := s.Instructions[i].Arg.(*connector.Argument)
					select {
					case <-arg.Reg.Ctx.Done():
					case arg.Reg.Ch <- nil:
					}
					break
				}
			}
			return err
		}
		flg := false // check for empty tables
		for i := 0; i < len(rs.Proc.Reg.MergeReceivers); i++ {
			reg := rs.Proc.Reg.MergeReceivers[i]
			for {
				bat := <-reg.Ch
				if bat == nil {
					break
				}
				if len(bat.Zs) == 0 {
					continue
				}
				if bats[i] == nil {
					bats[i] = bat
				} else {
					if bats[i], err = bats[i].Append(rs.Proc.Mp, bat); err != nil {
						for i := range bats {
							if bats[i] != nil {
								batch.Clean(bats[i], rs.Proc.Mp)
							}
						}
						return err
					}
				}
			}
			if bats[i] == nil {
				flg = true
			}
		}
		if flg {
			for i, in := range s.Instructions {
				if in.Op == vm.Connector {
					arg := s.Instructions[i].Arg.(*connector.Argument)
					select {
					case <-arg.Reg.Ctx.Done():
					case arg.Reg.Ch <- nil:
					}
					break
				}
			}
			for i := range bats {
				if bats[i] != nil {
					batch.Clean(bats[i], s.Proc.Mp)
				}
			}
			return nil
		}
		constructViews(bats, op.Vars)
		op.Bats = bats
		defer func() {
			for i := range bats {
				if bats[i] != nil {
					batch.Clean(bats[i], s.Proc.Mp)
				}
			}
		}()
	}
	s.Magic = Merge
	s.PreScopes = s.PreScopes[:1]
	return s.MergeRun(e)
}

// newMergeScope make a multi-layer merge structure, and return its top scope
// the top scope will do merge work
func newMergeScope(ss []*Scope, typ int, proc *process.Process) []*Scope {
	step := int(math.Log2(float64(len(ss))))
	n := len(ss) / step
	rs := make([]*Scope, n)
	for i := 0; i < n; i++ {
		if i == n-1 {
			rs[i] = &Scope{
				PreScopes: ss[i*step:],
				Magic:     Merge,
			}
		} else {
			rs[i] = &Scope{
				PreScopes: ss[i*step : (i+1)*step],
				Magic:     Merge,
			}
		}
		rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
			Op:  vm.Plus,
			Arg: &plus.Argument{Typ: typ},
		})
		{
			m := len(rs[i].PreScopes)
			ctx, cancel := context.WithCancel(context.Background())
			rs[i].Proc = process.New(mheap.New(guest.New(proc.Mp.Gm.Limit, proc.Mp.Gm.Mmu)))
			rs[i].Proc.Cancel = cancel
			rs[i].Proc.Cancel = cancel
			rs[i].Proc.Id = proc.Id
			rs[i].Proc.Lim = proc.Lim
			rs[i].Proc.Reg.MergeReceivers = make([]*process.WaitRegister, m)
			{
				for j := 0; j < m; j++ {
					rs[i].Proc.Reg.MergeReceivers[j] = &process.WaitRegister{
						Ctx: ctx,
						Ch:  make(chan *batch.Batch, 1),
					}
				}
			}
			for j := 0; j < m; j++ {
				ss[i*step+j].Instructions = append(ss[i*step+j].Instructions, vm.Instruction{
					Op: vm.Connector,
					Arg: &connector.Argument{
						Mmu: rs[i].Proc.Mp.Gm,
						Reg: rs[i].Proc.Reg.MergeReceivers[j],
					},
				})
			}
		}
	}
	return rs
}

// newMergeOrderScope make a multi-layer merge structure, and return its top scope
// the top scope will do mergeOrder work
func newMergeOrderScope(ss []*Scope, arg *mergeorder.Argument, proc *process.Process) []*Scope {
	step := int(math.Log2(float64(len(ss))))
	n := len(ss) / step
	rs := make([]*Scope, n)

	for i := 0; i < n; i++ {
		if i == n-1 {
			rs[i] = &Scope{
				PreScopes: ss[i*step:],
				Magic:     Merge,
			}
		} else {
			rs[i] = &Scope{
				PreScopes: ss[i*step : (i+1)*step],
				Magic:     Merge,
			}
		}
		rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
			Op:  vm.MergeOrder,
			Arg: &mergeorder.Argument{Fields: arg.Fields},
		})
		{
			m := len(rs[i].PreScopes)
			ctx, cancel := context.WithCancel(context.Background())
			rs[i].Proc = process.New(mheap.New(guest.New(proc.Mp.Gm.Limit, proc.Mp.Gm.Mmu)))
			rs[i].Proc.Cancel = cancel
			rs[i].Proc.Cancel = cancel
			rs[i].Proc.Id = proc.Id
			rs[i].Proc.Lim = proc.Lim
			rs[i].Proc.Reg.MergeReceivers = make([]*process.WaitRegister, m)
			{
				for j := 0; j < m; j++ {
					rs[i].Proc.Reg.MergeReceivers[j] = &process.WaitRegister{
						Ctx: ctx,
						Ch:  make(chan *batch.Batch, 1),
					}
				}
			}
			for j := 0; j < m; j++ {
				ss[i*step+j].Instructions = append(ss[i*step+j].Instructions, vm.Instruction{
					Op: vm.Connector,
					Arg: &connector.Argument{
						Mmu: rs[i].Proc.Mp.Gm,
						Reg: rs[i].Proc.Reg.MergeReceivers[j],
					},
				})
			}
		}
	}
	return rs
}

// newMergeLimitScope make a multi-layer merge structure, and return its top scope
// the top scope will do mergeLimit work to get first n from its pre-scopes
func newMergeLimitScope(ss []*Scope, limit uint64, proc *process.Process) []*Scope {
	step := int(math.Log2(float64(len(ss))))
	n := len(ss) / step
	rs := make([]*Scope, n)

	for i := 0; i < n; i++ {
		if i == n-1 {
			rs[i] = &Scope{
				PreScopes: ss[i*step:],
				Magic:     Merge,
			}
		} else {
			rs[i] = &Scope{
				PreScopes: ss[i*step : (i+1)*step],
				Magic:     Merge,
			}
		}
		rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
			Op:  vm.MergeLimit,
			Arg: &mergelimit.Argument{Limit: limit},
		})
		{
			m := len(rs[i].PreScopes)
			ctx, cancel := context.WithCancel(context.Background())
			rs[i].Proc = process.New(mheap.New(guest.New(proc.Mp.Gm.Limit, proc.Mp.Gm.Mmu)))
			rs[i].Proc.Cancel = cancel
			rs[i].Proc.Cancel = cancel
			rs[i].Proc.Id = proc.Id
			rs[i].Proc.Lim = proc.Lim
			rs[i].Proc.Reg.MergeReceivers = make([]*process.WaitRegister, m)
			{
				for j := 0; j < m; j++ {
					rs[i].Proc.Reg.MergeReceivers[j] = &process.WaitRegister{
						Ctx: ctx,
						Ch:  make(chan *batch.Batch, 1),
					}
				}
			}
			for j := 0; j < m; j++ {
				ss[i*step+j].Instructions = append(ss[i*step+j].Instructions, vm.Instruction{
					Op: vm.Connector,
					Arg: &connector.Argument{
						Mmu: rs[i].Proc.Mp.Gm,
						Reg: rs[i].Proc.Reg.MergeReceivers[j],
					},
				})
			}
		}
	}
	return rs
}

// newMergeTopScope make a multi-layer merge structure, and return its top scope
// the top scope will do mergeTop work to get top n from its pre-scopes
func newMergeTopScope(ss []*Scope, arg *mergetop.Argument, proc *process.Process) []*Scope {
	step := int(math.Log2(float64(len(ss))))
	n := len(ss) / step
	rs := make([]*Scope, n)

	for i := 0; i < n; i++ {
		if i == n-1 {
			rs[i] = &Scope{
				PreScopes: ss[i*step:],
				Magic:     Merge,
			}
		} else {
			rs[i] = &Scope{
				PreScopes: ss[i*step : (i+1)*step],
				Magic:     Merge,
			}
		}
		rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
			Op: vm.MergeTop,
			Arg: &mergetop.Argument{
				Fields: arg.Fields,
				Limit:  arg.Limit,
			},
		})
		{
			m := len(rs[i].PreScopes)
			ctx, cancel := context.WithCancel(context.Background())
			rs[i].Proc = process.New(mheap.New(guest.New(proc.Mp.Gm.Limit, proc.Mp.Gm.Mmu)))
			rs[i].Proc.Cancel = cancel
			rs[i].Proc.Cancel = cancel
			rs[i].Proc.Id = proc.Id
			rs[i].Proc.Lim = proc.Lim
			rs[i].Proc.Reg.MergeReceivers = make([]*process.WaitRegister, m)
			{
				for j := 0; j < m; j++ {
					rs[i].Proc.Reg.MergeReceivers[j] = &process.WaitRegister{
						Ctx: ctx,
						Ch:  make(chan *batch.Batch, 1),
					}
				}
			}
			for j := 0; j < m; j++ {
				ss[i*step+j].Instructions = append(ss[i*step+j].Instructions, vm.Instruction{
					Op: vm.Connector,
					Arg: &connector.Argument{
						Mmu: rs[i].Proc.Mp.Gm,
						Reg: rs[i].Proc.Reg.MergeReceivers[j],
					},
				})
			}
		}
	}
	return rs
}

// newMergeDedupScope make a multi-layer merge structure, and return its top scope
// the top scope will do merge-deduplication work
func newMergeDedupScope(ss []*Scope, proc *process.Process) []*Scope {
	step := int(math.Log2(float64(len(ss))))
	n := len(ss) / step
	rs := make([]*Scope, n)

	for i := 0; i < n; i++ {
		if i == n-1 {
			rs[i] = &Scope{
				PreScopes: ss[i*step:],
				Magic:     Merge,
			}
		} else {
			rs[i] = &Scope{
				PreScopes: ss[i*step : (i+1)*step],
				Magic:     Merge,
			}
		}
		rs[i].Instructions = append(rs[i].Instructions, vm.Instruction{
			Op:  vm.MergeDedup,
			Arg: &mergededup.Argument{},
		})
		{
			m := len(rs[i].PreScopes)
			ctx, cancel := context.WithCancel(context.Background())
			rs[i].Proc = process.New(mheap.New(guest.New(proc.Mp.Gm.Limit, proc.Mp.Gm.Mmu)))
			rs[i].Proc.Cancel = cancel
			rs[i].Proc.Cancel = cancel
			rs[i].Proc.Id = proc.Id
			rs[i].Proc.Lim = proc.Lim
			rs[i].Proc.Reg.MergeReceivers = make([]*process.WaitRegister, m)
			{
				for j := 0; j < m; j++ {
					rs[i].Proc.Reg.MergeReceivers[j] = &process.WaitRegister{
						Ctx: ctx,
						Ch:  make(chan *batch.Batch, 1),
					}
				}
			}
			for j := 0; j < m; j++ {
				ss[i*step+j].Instructions = append(ss[i*step+j].Instructions, vm.Instruction{
					Op: vm.Connector,
					Arg: &connector.Argument{
						Mmu: rs[i].Proc.Mp.Gm,
						Reg: rs[i].Proc.Reg.MergeReceivers[j],
					},
				})
			}
		}
	}
	return rs
}
