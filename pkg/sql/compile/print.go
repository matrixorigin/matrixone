package compile

import (
	"fmt"
	"matrixone/pkg/vm/pipeline"
)

func Print(prefix []byte, ss []*Scope) {
	for _, s := range ss {
		if s.Magic == Merge || s.Magic == Remote {
			Print(append(prefix, '\t'), s.Ss)
		}
		p := pipeline.NewMerge(s.Ins)
		fmt.Printf("%s:%v %v\n", prefix, s.Magic, p)
	}
}
