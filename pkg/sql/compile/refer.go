package compile

import "matrixone/pkg/sql/colexec/extend"

func IncRef(e extend.Extend, mp map[string]uint64) {
	switch v := e.(type) {
	case *extend.Attribute:
		mp[v.Name]++
	case *extend.UnaryExtend:
		IncRef(v.E, mp)
	case *extend.BinaryExtend:
		IncRef(v.Left, mp)
		IncRef(v.Right, mp)
	}
}
