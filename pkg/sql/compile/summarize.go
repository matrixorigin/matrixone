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
	}
	ss, err := c.compile(o.Prev, mp)
	if err != nil {
		return nil, err
	}
	rs := new(Scope)
	gm := guest.New(c.proc.Gm.Limit, c.proc.Gm.Mmu)
	rs.Proc = process.New(gm, c.proc.Mp)
	rs.Proc.Lim = c.proc.Lim
	rs.Proc.Reg.Ws = make([]*process.WaitRegister, len(ss))
	{
		for i, j := 0, len(ss); i < j; i++ {
			rs.Proc.Reg.Ws[i] = &process.WaitRegister{
				Wg: new(sync.WaitGroup),
				Ch: make(chan interface{}),
			}
		}
	}
	for i, s := range ss {
		s.Ins = append(s.Ins, vm.Instruction{
			Op: vm.Summarize,
			Arg: &vsummarize.Argument{
				Refer: refer,
				Es:    unitAggregates(o.Es),
			},
		})
		s.Ins = append(s.Ins, vm.Instruction{
			Op: vm.Transfer,
			Arg: &transfer.Argument{
				Mmu: gm,
				Reg: rs.Proc.Reg.Ws[i],
			},
		})

	}
	rs.Ss = ss
	rs.Magic = Merge
	rs.Ins = append(rs.Ins, vm.Instruction{
		Op: vm.MergeSummarize,
		Arg: &mergesum.Argument{
			Refer: refer,
			Es:    mergeAggregates(o.Es),
		},
	})
	return []*Scope{rs}, nil
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
