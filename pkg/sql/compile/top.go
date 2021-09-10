package compile

import (
	"matrixone/pkg/sql/colexec/mergetop"
	vtop "matrixone/pkg/sql/colexec/top"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/top"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileTopOutput(o *top.Top, mp map[string]uint64) ([]*Scope, error) {
	{
		for _, g := range o.Gs {
			mp[g.Name]++
			mp[g.Name]++
		}
	}
	ss, err := c.compile(o.Prev, mp)
	if err != nil {
		return nil, err
	}
	fs := make([]vtop.Field, len(o.Gs))
	{
		for i, g := range o.Gs {
			fs[i] = vtop.Field{
				Attr: g.Name,
				Type: vtop.Direction(g.Dirt),
			}
		}
	}
	rs := new(Scope)
	rs.Proc = process.New(guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu))
	rs.Proc.Lim = c.proc.Lim
	rs.Proc.Reg.Ws = make([]*process.WaitRegister, len(ss))
	{
		for i, j := 0, len(ss); i < j; i++ {
			rs.Proc.Reg.Ws[i] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}, 8),
			}
		}
	}
	if o.IsPD {
		arg := &vtop.Argument{Fs: fs, Limit: o.Limit}
		for i, s := range ss {
			ss[i] = pushTop(s, arg)
		}
	}
	for i, s := range ss {
		ss[i].Ins = append(s.Ins, vm.Instruction{
			Op: vm.Transfer,
			Arg: &transfer.Argument{
				Proc: rs.Proc,
				Reg:  rs.Proc.Reg.Ws[i],
			},
		})
	}
	rs.Ss = ss
	rs.Magic = Merge
	mfs := make([]mergetop.Field, len(o.Gs))
	{
		for i, g := range o.Gs {
			mfs[i] = mergetop.Field{
				Attr: g.Name,
				Type: mergetop.Direction(g.Dirt),
			}
		}
	}
	rs.Ins = append(rs.Ins, vm.Instruction{
		Op: vm.MergeTop,
		Arg: &mergetop.Argument{
			Fs:    mfs,
			Limit: o.Limit,
		},
	})
	return []*Scope{rs}, nil
}

func (c *compile) compileTop(o *top.Top, mp map[string]uint64) ([]*Scope, error) {
	{
		for _, g := range o.Gs {
			mp[g.Name]++
		}
	}
	ss, err := c.compile(o.Prev, mp)
	if err != nil {
		return nil, err
	}
	fs := make([]vtop.Field, len(o.Gs))
	{
		for i, g := range o.Gs {
			fs[i] = vtop.Field{
				Attr: g.Name,
				Type: vtop.Direction(g.Dirt),
			}
		}
	}
	rs := new(Scope)
	rs.Proc = process.New(guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu))
	rs.Proc.Lim = c.proc.Lim
	rs.Proc.Reg.Ws = make([]*process.WaitRegister, len(ss))
	{
		for i, j := 0, len(ss); i < j; i++ {
			rs.Proc.Reg.Ws[i] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}, 8),
			}
		}
	}
	if o.IsPD {
		arg := &vtop.Argument{Fs: fs, Limit: o.Limit}
		for i, s := range ss {
			ss[i] = pushTop(s, arg)
		}
	}
	for i, s := range ss {
		ss[i].Ins = append(s.Ins, vm.Instruction{
			Op: vm.Transfer,
			Arg: &transfer.Argument{
				Proc: rs.Proc,
				Reg:  rs.Proc.Reg.Ws[i],
			},
		})
	}
	rs.Ss = ss
	rs.Magic = Merge
	mfs := make([]mergetop.Field, len(o.Gs))
	{
		for i, g := range o.Gs {
			mfs[i] = mergetop.Field{
				Attr: g.Name,
				Type: mergetop.Direction(g.Dirt),
			}
		}
	}
	rs.Ins = append(rs.Ins, vm.Instruction{
		Op: vm.MergeTop,
		Arg: &mergetop.Argument{
			Fs:    mfs,
			Limit: o.Limit,
		},
	})
	return []*Scope{rs}, nil
}

func pushTop(s *Scope, arg *vtop.Argument) *Scope {
	if s.Magic == Merge || s.Magic == Remote {
		for i := range s.Ss {
			s.Ss[i] = pushTop(s.Ss[i], arg)
		}
		fs := make([]mergetop.Field, len(arg.Fs))
		{
			for i, f := range arg.Fs {
				fs[i] = mergetop.Field{
					Attr: f.Attr,
					Type: mergetop.Direction(f.Type),
				}
			}
		}
		s.Ins[len(s.Ins)-1] = vm.Instruction{
			Op: vm.MergeTop,
			Arg: &mergetop.Argument{
				Fs:    fs,
				Flg:   true,
				Limit: arg.Limit,
			},
		}
	} else {
		n := len(s.Ins) - 1
		s.Ins = append(s.Ins, vm.Instruction{
			Arg: arg,
			Op:  vm.Top,
		})
		s.Ins[n], s.Ins[n+1] = s.Ins[n+1], s.Ins[n]
	}
	return s
}
