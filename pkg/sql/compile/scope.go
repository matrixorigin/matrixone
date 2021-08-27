package compile

import (
	"matrixone/pkg/errno"
	"matrixone/pkg/sqlerror"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/pipeline"
	"sync"
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
		r, err := e.Relation(s.Data.ID)
		if err != nil {
			return err
		}
		for i, seg := range s.Data.Segs {
			segs[i] = r.Segment(seg, s.Proc)
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
		}
	}
	p := pipeline.NewMerge(s.Ins)
	if _, rerr := p.RunMerge(s.Proc); rerr != nil {
		err = rerr
	}
	if err != nil {
		wg.Wait()
		return sqlerror.New(errno.SyntaxErrororAccessRuleViolation, err.Error())
	}
	wg.Wait()
	return nil
}
