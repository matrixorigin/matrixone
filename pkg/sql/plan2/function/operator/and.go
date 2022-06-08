package operator

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func ColAndCol(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, ok := lv.Col.([]bool)
	if !ok {
		return nil, errors.New("the left vec col is not []bool type")
	}
	rvs, ok := rv.Col.([]bool)
	if !ok {
		return nil, errors.New("the right vec col is not []bool type")
	}
	n := len(lvs)
	vec, err := proc.AllocVector(lv.Typ, int64(n)*1)
	if err != nil {
		return nil, err
	}
	col := make([]bool, len(lvs))
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	for i := 0; i < len(lvs); i++ {
		col[i] = lvs[i] && rvs[i]
		ln, rn := nulls.Contains(lv.Nsp, uint64(i)), nulls.Contains(rv.Nsp, uint64(i))
		if (ln && !rn) || (!ln && rn) {
			if ln && !rn {
				if !rvs[i] {
					vec.Nsp.Np.Remove(uint64(i))
				}
			} else {
				if !lvs[i] {
					vec.Nsp.Np.Remove(uint64(i))
				}
			}
		}
	}
	vector.SetCol(vec, col)
	return vec, nil
}

func ColAndConst(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, ok := lv.Col.([]bool)
	if !ok {
		return nil, errors.New("the left vec col is not []bool type")
	}
	rvs, ok := rv.Col.([]bool)
	if !ok {
		return nil, errors.New("the right vec col is not []bool type")
	}
	n := len(lvs)
	vec, err := proc.AllocVector(lv.Typ, int64(n)*1)
	if err != nil {
		return nil, err
	}
	rb := rvs[0]
	col := make([]bool, len(lvs))
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	for i := 0; i < len(lvs); i++ {
		col[i] = lvs[i] && rb
		if nulls.Contains(lv.Nsp, uint64(i)) && !rb {
			vec.Nsp.Np.Remove(uint64(i))
		}
	}
	vector.SetCol(vec, col)
	return vec, nil
}

func ColAndNull(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, ok := lv.Col.([]bool)
	if !ok {
		return nil, errors.New("the left vec col is not []bool type")
	}
	n := len(lvs)
	vec, err := proc.AllocVector(lv.Typ, int64(n)*1)
	if err != nil {
		return nil, err
	}
	col := make([]bool, len(lvs))
	for i := 0; i < len(lvs); i++ {
		if nulls.Contains(lv.Nsp, uint64(i)) || lvs[i] {
			nulls.Add(vec.Nsp, uint64(i))
		}
	}
	vector.SetCol(vec, col)
	return vec, nil
}

func ConstAndCol(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return ColAndConst(rv, lv, proc)
}

func ConstAndConst(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, ok := lv.Col.([]bool)
	if !ok {
		return nil, errors.New("the left vec col is not []bool type")
	}
	rvs, ok := rv.Col.([]bool)
	if !ok {
		return nil, errors.New("the right vec col is not []bool type")
	}
	vec := proc.AllocScalarVector(lv.Typ)
	vector.SetCol(vec, []bool{lvs[0] && rvs[0]})
	return vec, nil
}

func ConstAndNull(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, ok := lv.Col.([]bool)
	if !ok {
		return nil, errors.New("the left vec col is not []bool type")
	}
	if lvs[0] {
		return proc.AllocScalarNullVector(lv.Typ), nil
	} else {
		vec := proc.AllocScalarVector(lv.Typ)
		vector.SetCol(vec, []bool{false})
		return vec, nil
	}
}

func NullAndCol(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return ColAndNull(rv, lv, proc)
}

func NullAndConst(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return ConstAndNull(rv, lv, proc)
}

func NullAndNull(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(lv.Typ), nil
}

func InitFuncMap() {
	InitAndFuncMap()
	InitOrFuncMap()
	InitXorFuncMap()
	InitNotFuncMap()
	InitEqFuncMap()
	InitGeFuncMap()
	InitGtFuncMap()
	InitLeFuncMap()
	InitLtFuncMap()
	InitNeFuncMap()
}

type AndFunc = func(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error)

var AndFuncMap = map[int]AndFunc{}

var AndFuncVec = []AndFunc{
	ColAndCol, ColAndConst, ColAndNull,
	ConstAndCol, ConstAndConst, ConstAndNull,
	NullAndCol, NullAndConst, NullAndNull,
}

func InitAndFuncMap() {
	for i := 0; i < len(AndFuncVec); i++ {
		AndFuncMap[i] = AndFuncVec[i]
	}
}

func GetTypeID(vs *vector.Vector) int {
	if vs.IsScalar() {
		if vs.IsScalarNull() {
			return 2
		}
		return 1
	}
	return 0
}

func And(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	rv := vectors[1]
	lt, rt := GetTypeID(lv), GetTypeID(rv)
	vec, err := AndFuncMap[lt*3+rt](lv, rv, proc)
	if err != nil {
		return nil, errors.New("And function: " + err.Error())
	}
	return vec, nil
}
