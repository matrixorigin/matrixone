package compile

import (
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/colexec/mergesum"
	vsummarize "matrixone/pkg/sql/colexec/summarize"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/sql/op/summarize"
	"matrixone/pkg/vm"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
	"sync"
)

func (c *compile) compileSummarize(o *summarize.Summarize, mp map[string]uint64) ([]*Scope, error) {
	refer := make(map[string]uint64)
	{
		for _, attr := range o.As {
			if v, ok := mp[attr]; ok {
				refer[attr] = v + 1
				delete(mp, attr)
			} else {
				refer[attr]++
			}
		}
		for _, e := range o.Es {
			mp[e.Name]++
		}
	}
	ss, err := c.compile(o.Prev, mp)
	if err != nil {
		return nil, err
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
		for i, s := range ss {
			ss[i] = pushSummarize(s, refer, o)
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
	if o.IsPD {
		rs.Ins = append(rs.Ins, vm.Instruction{
			Op: vm.MergeSummarize,
			Arg: &mergesum.Argument{
				Refer: refer,
				Es:    mergeAggregates(o.Es),
			},
		})
	} else {
		rs.Ins = append(rs.Ins, vm.Instruction{
			Op: vm.MergeSummarize,
			Arg: &mergesum.Argument{
				Refer: refer,
				Es:    unitAggregates(o.Es),
			},
		})
	}
	return []*Scope{rs}, nil
}

func pushSummarize(s *Scope, refer map[string]uint64, o *summarize.Summarize) *Scope {
	if s.Magic == Merge || s.Magic == Remote {
		for i := range s.Ss {
			s.Ss[i] = pushSummarize(s.Ss[i], refer, o)
		}
		s.Ins[len(s.Ins)-1] = vm.Instruction{
			Op: vm.MergeSummarize,
			Arg: &mergesum.Argument{
				Refer: refer,
				Es:    remoteAggregates(o.Es),
			},
		}
	} else {
		n := len(s.Ins) - 1
		s.Ins = append(s.Ins, vm.Instruction{
			Op: vm.Summarize,
			Arg: &vsummarize.Argument{
				Refer: refer,
				Es:    unitAggregates(o.Es),
			},
		})
		s.Ins[n], s.Ins[n+1] = s.Ins[n+1], s.Ins[n]
	}
	return s
}

func unitAggregates(es []aggregation.Extend) []aggregation.Extend {
	rs := make([]aggregation.Extend, len(es))
	for i, e := range es {
		rs[i] = aggregation.Extend{
			Name:  e.Name,
			Alias: e.Alias,
			Op:    unitAggFuncs[e.Op],
		}
	}
	return rs
}

func mergeAggregates(es []aggregation.Extend) []aggregation.Extend {
	rs := make([]aggregation.Extend, len(es))
	for i, e := range es {
		rs[i] = aggregation.Extend{
			Name:  e.Alias,
			Alias: e.Alias,
			Op:    mergeAggFuncs[e.Op],
		}
	}
	return rs
}

func remoteAggregates(es []aggregation.Extend) []aggregation.Extend {
	rs := make([]aggregation.Extend, len(es))
	for i, e := range es {
		rs[i] = aggregation.Extend{
			Name:  e.Alias,
			Alias: e.Alias,
			Op:    remoteAggFuncs[e.Op],
		}
	}
	return rs
}

var unitAggFuncs map[int]int = map[int]int{
	aggregation.Avg:       aggregation.SumCount,
	aggregation.Max:       aggregation.Max,
	aggregation.Min:       aggregation.Min,
	aggregation.Sum:       aggregation.Sum,
	aggregation.Count:     aggregation.Count,
	aggregation.StarCount: aggregation.StarCount,
}

var mergeAggFuncs map[int]int = map[int]int{
	aggregation.Avg:       aggregation.Avg,
	aggregation.Max:       aggregation.Max,
	aggregation.Min:       aggregation.Min,
	aggregation.Sum:       aggregation.Sum,
	aggregation.Count:     aggregation.Sum,
	aggregation.StarCount: aggregation.Sum,
}

var remoteAggFuncs map[int]int = map[int]int{
	aggregation.Avg:       aggregation.SumCount,
	aggregation.Max:       aggregation.Max,
	aggregation.Min:       aggregation.Min,
	aggregation.Sum:       aggregation.Sum,
	aggregation.Count:     aggregation.Sum,
	aggregation.StarCount: aggregation.Sum,
}
