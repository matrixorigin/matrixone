package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func NotCol(lv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := lv.Col.([]bool)
	n := len(lvs)
	vec, err := proc.AllocVector(lv.Typ, int64(n)*1)
	if err != nil {
		return nil, err
	}
	col := make([]bool, len(lvs))
	for i := 0; i < len(lvs); i++ {
		col[i] = !lvs[i]
	}
	nulls.Or(lv.Nsp, nil, vec.Nsp)
	vector.SetCol(vec, col)
	return vec, nil
}

func NotConst(lv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := lv.Col.([]bool)
	vec := proc.AllocScalarVector(lv.Typ)
	vector.SetCol(vec, []bool{!lvs[0]})
	return vec, nil
}

func NotNull(lv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(lv.Typ), nil
}

type NotFunc = func(lv *vector.Vector, proc *process.Process) (*vector.Vector, error)

var NotFuncMap = map[int]NotFunc{}

var NotFuncVec = []NotFunc{
	NotCol, NotConst, NotNull,
}

func InitNotFuncMap() {
	for i := 0; i < len(NotFuncVec); i++ {
		NotFuncMap[i] = NotFuncVec[i]
	}
}

func Not(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	lt := GetTypeID(lv)
	vec, err := NotFuncMap[lt](lv, proc)
	if err != nil {
		return nil, err
	}
	return vec, nil
}
