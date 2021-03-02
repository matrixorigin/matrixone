package vm

import (
	"bytes"
)

func (ins Instructions) String() string {
	var buf bytes.Buffer

	for _, in := range ins {
		switch in.Op {
		case Nub:
		case Top:
		case Limit:
		case Group:
		case Order:
		case Transfer:
		case Restrict:
		case Summarize:
		case Projection:
		case SetUnion:
		case SetIntersect:
		case SetDifference:
		case MultisetUnion:
		case MultisetIntersect:
		case MultisetDifference:
		case EqJoin:
		case SemiJoin:
		case InnerJoin:
		case NaturalJoin:
		}
	}
	return buf.String()
}
