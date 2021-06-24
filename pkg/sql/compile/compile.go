package compile

import (
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/build"
	"matrixone/pkg/sql/colexec/myoutput"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/dedup"
	"matrixone/pkg/sql/op/group"
	"matrixone/pkg/sql/op/innerJoin"
	"matrixone/pkg/sql/op/insert"
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
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/pipeline"
	"matrixone/pkg/vm/process"
	"sync"
)

func New(db string, sql string, uid string,
	e engine.Engine, ns metadata.Nodes, proc *process.Process) *compile {
	return &compile{
		e:    e,
		db:   db,
		ns:   ns,
		uid:  uid,
		sql:  sql,
		proc: proc,
	}
}

func (c *compile) Compile() ([]*Exec, error) {
	stmts, err := tree.NewParser().Parse(c.sql)
	if err != nil {
		return nil, err
	}
	es := make([]*Exec, len(stmts))
	for i, stmt := range stmts {
		es[i] = &Exec{
			c:    c,
			stmt: stmt,
		}
	}
	return es, nil
}

func (e *Exec) Compile(u interface{}, fill func(interface{}, *batch.Batch)) error {
	o, err := build.New(e.c.db, e.c.sql, e.c.e, e.c.proc).BuildStatement(e.stmt)
	if err != nil {
		return err
	}
	o = opt.Optimize(o)
	ss, err := e.c.compile(o, make(map[string]uint64))
	if err != nil {
		return err
	}
	mp := o.Attribute()
	attrs := o.Columns()
	cs := make([]*Col, 0, len(mp))
	for _, attr := range attrs {
		cs = append(cs, &Col{mp[attr].Oid, attr})
	}
	for _, s := range ss {
		s.Ins = append(s.Ins, vm.Instruction{
			Op: vm.MyOutput,
			Arg: &myoutput.Argument{
				Data:  u,
				Func:  fill,
				Attrs: attrs,
			},
		})
	}
	e.cs = cs
	e.ss = ss
	e.e = e.c.e
	return nil
}

func (e *Exec) Statement() tree.Statement {
	return e.stmt
}

func (e *Exec) SetSchema(db string) error {
	e.c.db = db
	return nil
}

func (e *Exec) Columns() []*Col {
	return e.cs
}

func (e *Exec) Run() error {
	var wg sync.WaitGroup

	for i := range e.ss {
		switch e.ss[i].Magic {
		case Normal:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.Run(e.e); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.ss[i])
		case Merge:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.MergeRun(e.e, wg); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.ss[i])
		case Insert:
			wg.Add(1)
			go func(s *Scope) {
				if err := s.Insert(); err != nil {
					e.err = err
				}
				wg.Done()
			}(e.ss[i])
		}
	}
	wg.Wait()
	return e.err
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

func (s *Scope) MergeRun(e engine.Engine, wg sync.WaitGroup) error {
	var err error

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
				if rerr := s.MergeRun(e, wg); rerr != nil {
					err = rerr
				}
				wg.Done()
			}(s.Ss[i])
		}
	}
	p := pipeline.NewMerge(s.Ins)
	if _, err = p.RunMerge(s.Proc); err != nil {
		return err
	}
	return err
}

func (s *Scope) Insert() error {
	o, _ := s.O.(*insert.Insert)
	return o.R.Write(o.Bat)
}

func (c *compile) compile(o op.OP, mp map[string]uint64) ([]*Scope, error) {
	switch n := o.(type) {
	case *insert.Insert:
		return []*Scope{&Scope{Magic: Insert, O: o}}, nil
	case *top.Top:
		return c.compileTop(n, mp)
	case *dedup.Dedup:
		return c.compileDedup(n, mp)
	case *group.Group:
		return c.compileGroup(n, mp)
	case *limit.Limit:
		return c.compileLimit(n, mp)
	case *order.Order:
		return c.compileOrder(n, mp)
	case *offset.Offset:
		return c.compileOffset(n, mp)
	case *product.Product:
		return nil, fmt.Errorf("'%s' unsupprt now", o)
	case *innerJoin.Join:
		return c.compileInnerJoin(n, mp)
	case *naturalJoin.Join:
	case *relation.Relation:
		return c.compileRelation(n, mp)
	case *restrict.Restrict:
		return c.compileRestrict(n, mp)
	case *summarize.Summarize:
		return c.compileSummarize(n, mp)
	case *projection.Projection:
		return c.compileProjection(n, mp)
	}
	return nil, fmt.Errorf("'%s' unsupprt now", o)
}
