// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multi

import (
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestFromUnixtime(t *testing.T) {
	FromUnixtimeCase(t, types.T_int64, 1641046980, MustDatetimeMe("2022-01-01 22:23:00"))
	FromUnixtimeCase(t, types.T_int64, 1641046980, MustDatetimeMe("2022-01-01 22:23:00"))
	FromUnixtimeCase(t, types.T_int64, 1641046980, MustDatetimeMe("2022-01-01 22:23:00"))
}

//func FromUnixTime(lv []*vector.Vector, proc *process.Process) (*vector.Vector, error)
func FromUnixtimeCase(t *testing.T, typ types.T, src int64, res types.Datetime) {
	makeProcess := func() *process.Process {
		hm := host.New(1 << 40)
		gm := guest.New(1<<40, hm)
		return process.New(mheap.New(gm))
	}
	procs := makeProcess()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  interface{}
		wantScalar bool
	}{
		{
			name:       "TEST01",
			vecs:       makeVector(src, true, typ),
			proc:       procs,
			wantBytes:  []types.Datetime{res},
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := FromUnixTime(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			reflect.DeepEqual(c.wantBytes, plus.Col)
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

func makeVector(src int64, srcScalar bool, t types.T) []*vector.Vector {
	vectors := make([]*vector.Vector, 1)
	vectors[0] = &vector.Vector{
		Col:     []int64{src},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: t},
		IsConst: srcScalar,
		Length:  1,
	}
	return vectors
}
