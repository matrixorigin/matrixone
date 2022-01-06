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
	return o.Relation.CreateIndex(ts, o.Defs)
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

// DropIndex do drop index word according to drop index plan
func (s *Scope) DropIndex(ts uint64) error {
	p, _ := s.Plan.(*plan.DropIndex)
	if p.NotExisted && p.IfExistFlag {
		return nil
	}

	defer p.Relation.Close()
	return p.Relation.DropIndex(ts, p.Id)
}

// todo: show should get information from system table next day.

// ShowDatabases will show all database names
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
				if k, _ := like.PureLikePure(str, p.Like, tempSlice); k != nil {
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

// ShowTables will show all table names in a database
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
				if k, _ := like.PureLikePure(str, p.Like, tempSlice); k != nil {
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
}

// ShowColumns will show column information from a table
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
				if k, _ := like.PureLikePure([]byte(tableOption.Attr.Name), p.Like, tmpSlice); k == nil {
					continue
				}
			}
			attrs[count] = columnInfo{
				name: tableOption.Attr.Name,
				typ:  tableOption.Attr.Type,
			}
			if tableOption.Attr.HasDefaultExpr() {
				if tableOption.Attr.Default.IsNull {
					attrs[count].dft = nullString
				} else {
					switch tableOption.Attr.Type.Oid {
					case types.T_date:
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

	vnames := make([][]byte, len(attrs))
	vtyps := make([][]byte, len(attrs))
	vdfts := make([][]byte, len(attrs))
	undefine := make([][]byte, len(attrs))

	for i, attr := range attrs {
		var typ string

		if attr.typ.Width > 0 {
			typ = fmt.Sprintf("%s(%v)", strings.ToLower(attr.typ.String()), attr.typ.Width)
		} else {
			typ = strings.ToLower(attr.typ.String())
		}

		vnames[i] = []byte(attr.name)
		vtyps[i] = []byte(typ)
		vdfts[i] = []byte(attr.dft)
		undefine[i] = []byte("")
	}

	vector.Append(bat.Vecs[0], vnames)   // field
	vector.Append(bat.Vecs[1], vtyps)    // type
	vector.Append(bat.Vecs[2], undefine) // null todo: not implement
	vector.Append(bat.Vecs[3], undefine) // key todo: not implement
	vector.Append(bat.Vecs[4], vdfts)    // default
	vector.Append(bat.Vecs[5], undefine) // extra todo: not implement

	bat.InitZsOne(count)
	return fill(u, bat)
}

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
	if attributeDefs != nil {
		for _, a := range attributeDefs {
			buf.WriteString(prefix)
			a.Format(&buf)
			prefix = ",\n "
		}
	}
	if len(primaryIndexDef.Names) > 0 {
		buf.WriteString(prefix)
		primaryIndexDef.Format(&buf)
		prefix = ",\n "
	}
	if indexTableDefs != nil {
		for _, idx := range indexTableDefs {
			buf.WriteString(prefix)
			idx.Format(&buf)
			prefix = ",\n "
		}
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

// Insert will insert a batch into relation and return affectedRow
func (s *Scope) Insert(ts uint64) (uint64, error) {
	p, _ := s.Plan.(*plan.Insert)
	defer p.Relation.Close()
	return uint64(vector.Length(p.Bat.Vecs[0])), p.Relation.Write(ts, p.Bat)
}

func (s *Scope) Run(e engine.Engine) error {
	p := pipeline.New(s.DataSource.RefCounts, s.DataSource.Attributes, s.Instructions)
	if _, err := p.Run(s.DataSource.R, s.Proc); err != nil {
		return err
	}
	return nil
}

func (s *Scope) MergeRun(e engine.Engine) error {
	var err error

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
	p := pipeline.NewMerge(s.Instructions)
	if _, rerr := p.RunMerge(s.Proc); rerr != nil {
		err = rerr
	}
	return err
}

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

func (s *Scope) ParallelRun(e engine.Engine) error {
	switch t := s.Instructions[0].Arg.(type) {
	case *times.Argument:
		return s.RunCAQ(e)
	case *transform.Argument:
		if t.Typ == transform.Bare {
			return s.RunQ(e)
		}
		return s.RunAQ(e)
	}
	return nil
}

func (s *Scope) RunQ(e engine.Engine) error {
	var rd engine.Reader

	ss := make([]*Scope, 1)
	{ // get table reader
		db, err := e.Database(s.DataSource.SchemaName)
		if err != nil {
			return err
		}
		rel, err := db.Relation(s.DataSource.RelationName)
		if err != nil {
			return err
		}
		defer rel.Close()
		rd = rel.NewReader(1)[0]
	}
	arg := s.Instructions[0].Arg.(*transform.Argument)
	{
		ss[0] = &Scope{
			Magic: Normal,
			DataSource: &Source{
				R:            rd,
				IsMerge:      s.DataSource.IsMerge,
				SchemaName:   s.DataSource.SchemaName,
				RelationName: s.DataSource.RelationName,
				RefCounts:    s.DataSource.RefCounts,
				Attributes:   s.DataSource.Attributes,
			},
		}
		ss[0].Instructions = append(ss[0].Instructions, vm.Instruction{
			Op: vm.Transform,
			Arg: &transform.Argument{
				Typ:        arg.Typ,
				IsMerge:    arg.IsMerge,
				FreeVars:   arg.FreeVars,
				Restrict:   arg.Restrict,
				Projection: arg.Projection,
				BoundVars:  arg.BoundVars,
			},
		})

		ss[0].Proc = process.New(mheap.New(guest.New(s.Proc.Mp.Gm.Limit, s.Proc.Mp.Gm.Mmu)))
		ss[0].Proc.Id = s.Proc.Id
		ss[0].Proc.Lim = s.Proc.Lim
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.Magic = Merge
	s.PreScopes = ss
	s.Instructions[0] = vm.Instruction{
		Op: vm.Merge,
	}
	s.Proc.Cancel = cancel
	s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, 1)
	s.Proc.Reg.MergeReceivers[0] = &process.WaitRegister{
		Ctx: ctx,
		Ch:  make(chan *batch.Batch, 1),
	}

	ss[0].Instructions = append(ss[0].Instructions, vm.Instruction{
		Op: vm.Connector,
		Arg: &connector.Argument{
			Mmu: s.Proc.Mp.Gm,
			Reg: s.Proc.Reg.MergeReceivers[0],
		},
	})

	return s.MergeRun(e)
}

func (s *Scope) RunAQ(e engine.Engine) error {
	var rds []engine.Reader

	mcpu := runtime.NumCPU()
	ss := make([]*Scope, mcpu)
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
		rds = rel.NewReader(mcpu)
	}
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
		ss[i].Instructions = append(ss[i].Instructions, vm.Instruction{
			Op: vm.Transform,
			Arg: &transform.Argument{
				Typ:        arg.Typ,
				IsMerge:    arg.IsMerge,
				FreeVars:   arg.FreeVars,
				Restrict:   arg.Restrict,
				Projection: arg.Projection,
				BoundVars:  arg.BoundVars,
			},
		})
		ss[i].Proc = process.New(mheap.New(guest.New(s.Proc.Mp.Gm.Limit, s.Proc.Mp.Gm.Mmu)))
		ss[i].Proc.Id = s.Proc.Id
		ss[i].Proc.Lim = s.Proc.Lim
	}
	for len(ss) > 3 {
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

func (s *Scope) RunCAQ(e engine.Engine) error {
	var err error
	var rds []engine.Reader
	var arg *times.Argument

	mcpu := runtime.NumCPU()
	ss := make([]*Scope, mcpu)
	{
		s0 := s.PreScopes[0]
		db, err := e.Database(s0.DataSource.SchemaName)
		if err != nil {
			return err
		}
		rel, err := db.Relation(s0.DataSource.RelationName)
		if err != nil {
			return err
		}
		defer rel.Close()
		rds = rel.NewReader(mcpu)
		s.DataSource = s0.DataSource
		arg = s.Instructions[0].Arg.(*times.Argument)
		arg.Arg = s0.Instructions[0].Arg.(*transform.Argument)
	}
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
	s.PreScopes = s.PreScopes[1:]
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
		s.PreScopes[i].Instructions = append(s.PreScopes[i].Instructions, vm.Instruction{
			Op: vm.Connector,
			Arg: &connector.Argument{
				Mmu: s.Proc.Mp.Gm,
				Reg: s.Proc.Reg.MergeReceivers[i],
			},
		})
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
	for i := 0; i < len(s.Proc.Reg.MergeReceivers); i++ {
		reg := s.Proc.Reg.MergeReceivers[i]
		bat := <-reg.Ch
		if bat == nil {
			continue
		}
		if len(bat.Zs) == 0 {
			i--
			continue
		}
		arg.Bats = append(arg.Bats, bat)
	}
	if len(arg.Bats) != len(arg.Svars) {
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
		return nil
	}
	constructViews(arg.Bats, arg.Svars)
	for i := 0; i < mcpu; i++ {
		ss[i].Instructions = vm.Instructions{vm.Instruction{
			Op: vm.Times,
			Arg: &times.Argument{
				IsBare:   arg.IsBare,
				R:        arg.R,
				Rvars:    arg.Rvars,
				Ss:       arg.Ss,
				Svars:    arg.Svars,
				VarsMap:  arg.VarsMap,
				Bats:     arg.Bats,
				FreeVars: arg.FreeVars,
				Arg: &transform.Argument{
					Typ:        arg.Arg.Typ,
					IsMerge:    arg.Arg.IsMerge,
					FreeVars:   arg.Arg.FreeVars,
					Restrict:   arg.Arg.Restrict,
					Projection: arg.Arg.Projection,
					BoundVars:  arg.Arg.BoundVars,
				},
			},
		}}
	}
	for len(ss) > 3 {
		ss = newMergeScope(ss, arg.Arg.Typ, s.Proc)
	}
	rs := &Scope{
		PreScopes: ss,
		Magic:     Merge,
	}
	rs.Instructions = s.Instructions
	rs.Instructions[0] = vm.Instruction{
		Op:  vm.Plus,
		Arg: &plus.Argument{Typ: arg.Arg.Typ},
	}
	{
		ctx, cancel := context.WithCancel(context.Background())
		rs.Proc = process.New(mheap.New(guest.New(s.Proc.Mp.Gm.Limit, s.Proc.Mp.Gm.Mmu)))
		rs.Proc.Cancel = cancel
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
	}
	return rs.MergeRun(e)
}

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
