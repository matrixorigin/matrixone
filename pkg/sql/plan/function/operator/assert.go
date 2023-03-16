package operator

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func AssertTrue(vectors []*vector.Vector, _ *process.Process) (*vector.Vector, error) {
	input := vectors[0]
	if input.IsConst() {
		return nil, moerr.NewInternalErrorNoCtx("dup")
	}
	vals := vector.MustFixedCol[bool](input)
	for i := range vals {
		if vals[i] != true {
			return nil, moerr.NewInternalErrorNoCtx("dup")
		}
	}
	return input, nil
}
