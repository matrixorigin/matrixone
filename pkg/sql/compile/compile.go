package compile

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/server"
	"matrixone/pkg/sql/build"
	"matrixone/pkg/sql/colexec/myoutput"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/dedup"
	"matrixone/pkg/sql/op/group"
	"matrixone/pkg/sql/op/innerJoin"
	"matrixone/pkg/sql/op/limit"
	"matrixone/pkg/sql/op/naturalJoin"
	"matrixone/pkg/sql/op/offset"
	"matrixone/pkg/sql/op/order"
	"matrixone/pkg/sql/op/product"
	"matrixone/pkg/sql/op/projection"
	"matrixone/pkg/sql/op/relation"
	"matrixone/pkg/sql/op/restrict"
	"matrixone/pkg/sql/op/summarize"
	"matrixone/pkg/sql/op/top"
	"matrixone/pkg/sql/opt"
	"matrixone/pkg/sql/result"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/pipeline"
	"matrixone/pkg/vm/process"
	"sync"
)

func New(db string, sql string, e engine.Engine, ns metadata.Nodes, proc *process.Process) *compile {
	return &compile{
		e:    e,
		db:   db,
		ns:   ns,
		sql:  sql,
		proc: proc,
	}
}

func (c *compile) Compile() ([]*Exec, error) {
	os, err := build.New(c.db, c.sql, c.e, c.proc).Build()
	if err != nil {
		return nil, err
	}
	for i, o := range os {
		os[i] = opt.Optimize(o)
	}
	es := make([]*Exec, len(os))
	for i, o := range os {
		ss, err := c.compile(o)
		if err != nil {
			return nil, err
		}
		mp := o.Attribute()
		cs := make([]*Col, 0, len(mp))
		attrs := make([]string, 0, len(mp))
		for k, v := range mp {
			attrs = append(attrs, k)
			cs = append(cs, &Col{v.Oid, k})
		}
		rs := make([]*result.Result, len(ss))
		for i, s := range ss {
			rs[i] = &result.Result{
				Attrs: attrs,
			}
			s.Ins = append(s.Ins, vm.Instruction{
				Op:  vm.MyOutput,
				Arg: &myoutput.Argument{rs[i]},
			})
		}
		es[i] = &Exec{
			cs: cs,
			ss: ss,
			rs: rs,
			e:  c.e,
		}
	}
	return es, nil
}

func (e *Exec) Run(mrs *server.MysqlResultSet) error {
	var wg sync.WaitGroup

	for _, s := range e.ss {
		switch s.Magic {
		case Normal:
			wg.Add(1)
			go func() {
				s.Run(e.e)
				wg.Done()
			}()
		case Merge:
		}
	}
	{
		mrs.Columns = make([]server.Column, len(e.cs))
		mrs.Name2Index = make(map[string]uint64)
		for i, c := range e.cs {
			mrs.Name2Index[c.Name] = uint64(i)
			col := new(server.MysqlColumn)
			col.SetName(c.Name)
			switch c.Typ {
			case types.T_int8:
				col.SetLength(1)
				col.SetColumnType(server.MYSQL_TYPE_TINY)
			case types.T_int16:
				col.SetLength(2)
				col.SetColumnType(server.MYSQL_TYPE_SHORT)
			case types.T_int32:
				col.SetLength(4)
				col.SetColumnType(server.MYSQL_TYPE_LONG)
			case types.T_int64:
				col.SetLength(8)
				col.SetColumnType(server.MYSQL_TYPE_LONGLONG)
			case types.T_float32:
				col.SetLength(4)
				col.SetColumnType(server.MYSQL_TYPE_FLOAT)
			case types.T_float64:
				col.SetLength(8)
				col.SetColumnType(server.MYSQL_TYPE_DOUBLE)
			case types.T_char:
			case types.T_varchar:
			}
			mrs.Columns[i] = col
		}
	}
	wg.Wait()
	for _, r := range e.rs {
		mrs.Data = append(mrs.Data, r.Rows...)
	}
	return nil
}

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
		for i, seg := range s.Data.Segs {
			segs[i] = r.Segment(engine.SegmentInfo{
				Id:       seg.Id,
				GroupId:  seg.GroupId,
				TabletId: seg.TabletId,
				Node:     seg.Node,
			}, s.Proc)
		}
	}
	if _, err := p.Run(segs, s.Proc); err != nil {
		return err
	}
	return nil
}

func (c *compile) compile(o op.OP) ([]*Scope, error) {
	switch n := o.(type) {
	case *top.Top:
	case *dedup.Dedup:
	case *group.Group:
	case *limit.Limit:
	case *order.Order:
	case *offset.Offset:
	case *product.Product:
	case *innerJoin.Join:
	case *naturalJoin.Join:
	case *relation.Relation:
		return c.compileRelation(n)
	case *restrict.Restrict:
		return c.compileRestrict(n)
	case *summarize.Summarize:
	case *projection.Projection:
		return c.compileProjection(n)
	}
	return nil, nil
}
