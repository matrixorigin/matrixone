package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func ColXorCol(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, rvs := lv.Col.([]bool), rv.Col.([]bool)
	n := len(lvs)
	vec, err := proc.AllocVector(lv.Typ, int64(n)*1)
	if err != nil {
		return nil, err
	}
	col := make([]bool, len(lvs))
	for i := 0; i < len(lvs); i++ {
		col[i] = (lvs[i] || rvs[i]) && !(lvs[i] && rvs[i])
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, col)
	return vec, nil
}

func ColXorConst(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, rvs := lv.Col.([]bool), rv.Col.([]bool)
	n := len(lvs)
	vec, err := proc.AllocVector(lv.Typ, int64(n)*1)
	if err != nil {
		return nil, err
	}
	rb := rvs[0]
	col := make([]bool, len(lvs))
	for i := 0; i < len(lvs); i++ {
		col[i] = (lvs[i] || rb) && !(lvs[i] && rb)
	}
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	vector.SetCol(vec, col)
	return vec, nil
}

func ColXorNull(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := lv.Col.([]bool)
	n := len(lvs)
	vec, err := proc.AllocVector(lv.Typ, int64(n)*1)
	if err != nil {
		return nil, err
	}
	col := make([]bool, len(lvs))
	for i := 0; i < len(lvs); i++ {
		nulls.Add(vec.Nsp, uint64(i))
	}
	vector.SetCol(vec, col)
	return vec, nil
}

func ConstXorCol(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return ColXorConst(rv, lv, proc)
}

func ConstXorConst(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, rvs := lv.Col.([]bool), rv.Col.([]bool)
	vec := proc.AllocScalarVector(lv.Typ)
	vector.SetCol(vec, []bool{(lvs[0] || rvs[0]) && !(lvs[0] && rvs[0])})
	return vec, nil
}

func ConstXorNull(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(lv.Typ), nil
}

func NullXorCol(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return ColXorNull(rv, lv, proc)
}

func NullXorConst(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return ConstXorNull(rv, lv, proc)
}

func NullXorNull(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(lv.Typ), nil
}

type XorFunc = func(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error)

var XorFuncMap = map[int]XorFunc{}

var XorFuncVec = []XorFunc{
	ColXorCol, ColXorConst, ColXorNull,
	ConstXorCol, ConstXorConst, ConstXorNull,
	NullXorCol, NullXorConst, NullXorNull,
}

func InitXorFuncMap() {
	for i := 0; i < len(XorFuncVec); i++ {
		XorFuncMap[i] = XorFuncVec[i]
	}
}

func Xor(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	rv := vectors[1]
	lt, rt := GetTypeID(lv), GetTypeID(rv)
	vec, err := XorFuncMap[lt*3+rt](lv, rv, proc)
	if err != nil {
		return nil, err
	}
	return vec, nil
}
