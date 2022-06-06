package operator

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

var (
	bt  = types.Type{Oid: types.T_bool}
	i64 = types.Type{Oid: types.T_int64}
)

type arg struct {
	info   string
	proc   *process.Process
	vs     []*vector.Vector
	match  bool // if false, the case shouldn't meet the function requirement
	err    bool
	expect *vector.Vector
}

func newProc() *process.Process {
	return process.New(mheap.New(guest.New(1<<10, host.New(1<<10))))
}

func makeBoolVector(values []bool) *vector.Vector {
	vec := vector.New(bt)
	vec.Col = values
	return vec
}

func makeScalarBool(v bool, length int) *vector.Vector {
	vec := newProc().AllocScalarVector(bt)
	vec.Length = length
	vec.Col = []bool{v}
	return vec
}

func makeInt64Vector(is []int64, nsp []uint64) *vector.Vector {
	vec := vector.New(i64)
	vec.Col = is
	for _, n := range nsp {
		nulls.Add(vec.Nsp, n)
	}
	return vec
}

func makeScalarInt64(i int64, length int) *vector.Vector {
	vec := newProc().AllocScalarVector(i64)
	vec.Length = length
	vec.Col = []int64{i}
	return vec
}

func makeScalarNull(length int) *vector.Vector {
	vec := newProc().AllocScalarNullVector(types.Type{Oid: types.T_any})
	vec.Length = length
	return vec
}

func TestCwFn1(t *testing.T) {
	testCases := []arg{
		{
			info: "when a = 1 then 1, when a = 2 then 2, else 3", proc: newProc(),
			vs: []*vector.Vector{
				makeBoolVector([]bool{true, true, false, false, false}), // a = 1
				makeScalarInt64(1, 5), // 1
				makeBoolVector([]bool{false, false, false, true, false}), // a = 2
				makeScalarInt64(2, 5), // 2
				makeScalarInt64(3, 5), // 3
			},
			match:  true,
			err:    false,
			expect: makeInt64Vector([]int64{1, 1, 3, 2, 3}, nil),
		},

		{
			info: "when a = 1 then 1, when a = 2 then 2, else null", proc: newProc(),
			vs: []*vector.Vector{
				makeBoolVector([]bool{false, false, false, false}), // a = 1
				makeScalarInt64(1, 4),                              // 1
				makeBoolVector([]bool{true, false, false, false}),  // a = 2
				makeScalarInt64(2, 4),                              // 2
				makeScalarNull(4),
			},
			match:  true,
			err:    false,
			expect: makeInt64Vector([]int64{2, 0, 0, 0}, []uint64{1, 2, 3}),
		},

		{
			info: "when a = 1 then 1, when a = 2 then 2", proc: newProc(),
			vs: []*vector.Vector{
				makeBoolVector([]bool{false, false, false, false}), // a = 1
				makeScalarInt64(1, 4),                              // 1
				makeBoolVector([]bool{true, false, false, false}),  // a = 2
				makeScalarInt64(2, 4),                              // 2
			},
			match:  true,
			err:    false,
			expect: makeInt64Vector([]int64{2, 0, 0, 0}, []uint64{1, 2, 3}),
		},

		{
			info: "when a = 1 then c1, when a = 2 then c2", proc: newProc(),
			vs: []*vector.Vector{
				makeBoolVector([]bool{true, true, false, false, false}),    // a = 1
				makeInt64Vector([]int64{1, 0, 3, 4, 5}, []uint64{1}),       // 1, null, 3, 4, 5
				makeBoolVector([]bool{false, false, true, true, false}),    // a = 2
				makeInt64Vector([]int64{0, 0, 0, 0, 0}, []uint64{1, 2, 3}), // 0, null, null, null, 0
			},
			match:  true,
			err:    false,
			expect: makeInt64Vector([]int64{1, 0, 0, 0, 0}, []uint64{1, 2, 3, 4}), // 1, null, null, null, null
		},

		{
			info: "when a = 1 then c1, when a = 2 then c2", proc: newProc(),
			vs: []*vector.Vector{
				makeBoolVector([]bool{true, true, false, false, false}),    // a = 1
				makeInt64Vector([]int64{1, 0, 3, 4, 5}, []uint64{1}),       // 1, null, 3, 4, 5
				makeBoolVector([]bool{false, false, true, true, false}),    // a = 2
				makeInt64Vector([]int64{0, 0, 0, 0, 0}, []uint64{1, 2, 3}), // 0, null, null, null, 0
			},
			match:  true,
			err:    false,
			expect: makeInt64Vector([]int64{1, 0, 0, 0, 0}, []uint64{1, 2, 3, 4}), // 1, null, null, null, null
		},

		{
			info: "when a = 1 then c1, when a = 2 then c2, else null", proc: newProc(),
			vs: []*vector.Vector{
				makeBoolVector([]bool{true, true, false, false, false}),    // a = 1
				makeInt64Vector([]int64{1, 0, 3, 4, 5}, []uint64{1}),       // 1, null, 3, 4, 5
				makeBoolVector([]bool{false, false, true, true, false}),    // a = 2
				makeInt64Vector([]int64{0, 0, 0, 0, 0}, []uint64{1, 2, 3}), // 0, null, null, null, 0
			},
			match:  true,
			err:    false,
			expect: makeInt64Vector([]int64{1, 0, 0, 0, 0}, []uint64{1, 2, 3, 4}), // 1, null, null, null, null
		},

		{
			info: "when a = 1 then c1, when a = 2 then c2, else c3", proc: newProc(),
			vs: []*vector.Vector{
				makeBoolVector([]bool{true, true, false, false, false}),      // a = 1
				makeInt64Vector([]int64{1, 0, 3, 4, 5}, []uint64{1}),         // 1, null, 3, 4, 5
				makeBoolVector([]bool{false, false, true, true, false}),      // a = 2
				makeInt64Vector([]int64{0, 0, 0, 0, 0}, []uint64{1, 2, 3}),   // 0, null, null, null, 0
				makeInt64Vector([]int64{100, 200, 300, 0, 500}, []uint64{3}), // 100, 200, 300, null, 500
			},
			match:  true,
			err:    false,
			expect: makeInt64Vector([]int64{1, 0, 0, 0, 500}, []uint64{1, 2, 3}), // 1, null, null, null, 500
		},

		{
			info: "when true then c1, when false then c2, else null", proc: newProc(),
			vs: []*vector.Vector{
				makeScalarBool(true, 4),
				makeInt64Vector([]int64{1, 2, 3, 4}, nil), // 1, 2, 3, 4
				makeScalarBool(false, 4),
				makeInt64Vector([]int64{4, 3, 2, 1}, nil),
				makeScalarNull(4),
			},
			match:  true,
			err:    false,
			expect: makeInt64Vector([]int64{1, 2, 3, 4}, nil), // 1, 2, 3, 4
		},

		{
			info: "when true then 1, when false then 2, else null", proc: newProc(),
			vs: []*vector.Vector{
				makeScalarBool(true, 4),
				makeScalarInt64(1, 4),
				makeScalarBool(false, 4),
				makeScalarInt64(2, 4),
				makeScalarNull(4),
			},
			match:  true,
			err:    false,
			expect: makeScalarInt64(1, 4),
		},

		{
			info: "when a = 1 then null, when a = 2 then null, else null", proc: newProc(),
			vs: []*vector.Vector{
				makeBoolVector([]bool{false, false, false, false}),
				makeScalarNull(4),
				makeBoolVector([]bool{false, false, false, false}),
				makeScalarNull(4),
				makeScalarNull(4),
			},
			match: false,
		},

		{
			// special case 1
			info: "when a = 1 then 1, when a = 1 then 2, else null", proc: newProc(),
			vs: []*vector.Vector{
				makeBoolVector([]bool{true, true, false, false}),
				makeScalarInt64(1, 4),
				makeBoolVector([]bool{false, true, true, false}),
				makeScalarInt64(2, 4),
				makeScalarNull(4),
			},
			match:  true,
			err:    false,
			expect: makeInt64Vector([]int64{1, 1, 2, 0}, []uint64{3}),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			{
				inputTypes := make([]types.T, len(tc.vs))
				for i := range inputTypes {
					inputTypes[i] = tc.vs[i].Typ.Oid
				}
				b := CwTypeCheckFn(inputTypes, nil, types.T_int64)
				if !tc.match {
					require.False(t, b, fmt.Sprintf("case '%s' shouldn't meet the function's requirement but it meets.", tc.info))
					return
				}
				require.True(t, b)
			}

			got, ergot := CwFn1[int64](tc.vs, tc.proc)
			if tc.err {
				require.Errorf(t, ergot, fmt.Sprintf("case '%d' expected error, but no error happens", i))
			} else {
				require.NoError(t, ergot)
				//println(fmt.Sprintf("e: %v", tc.expect.Col))
				//println(fmt.Sprintf("g: %v", got.Col))
				if tc.expect.IsScalar() {
					require.True(t, got.IsScalar())
					if tc.expect.IsScalarNull() {
						require.True(t, got.IsScalarNull())
					} else {
						require.Equal(t, tc.expect.Col.([]int64)[0], got.Col.([]int64)[0])
					}
				} else {
					require.False(t, got.IsScalar())

					expectL := vector.Length(tc.expect)
					gotL := vector.Length(got)
					require.Equal(t, expectL, gotL)
					require.Equal(t, nulls.Any(tc.expect.Nsp), nulls.Any(got.Nsp))

					for k := 0; k < gotL; k++ {
						c := nulls.Contains(tc.expect.Nsp, uint64(k))
						require.Equal(t, c, nulls.Contains(got.Nsp, uint64(k)))
						if !c {
							require.Equal(t, tc.expect.Col.([]int64)[k], got.Col.([]int64)[k])
						}
					}
				}
			}
		})
	}
}
