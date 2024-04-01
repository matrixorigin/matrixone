// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"runtime/debug"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

func init() {
	reuse.CreatePool[Vector](
		func() *Vector {
			res := &Vector{}
			return res
		},
		func(v *Vector) {
			*v = Vector{
				AllocMsg: v.AllocMsg,
				FreeMsg:  v.FreeMsg,
				PutMsg:   v.PutMsg,
				GetMsg:   v.GetMsg,
				OnUsed:   v.OnUsed,
			}
		},
		reuse.DefaultOptions[Vector](),
	)
}

func (v Vector) TypeName() string {
	return "Vector"
}

func NewVecFromReuse() *Vector {
	v := reuse.Alloc[Vector](nil)
	v.nsp = nulls.New()
	if v.OnUsed {
		logutil.Errorf("AllocMsg=%s\n\nFreeMsg=%s\n\nGetMsg=%s\n\nPutMsg=%s\n\n",
			v.AllocMsg,
			v.FreeMsg,
			v.GetMsg,
			v.PutMsg,
		)
		panic("+++++++++++  vec on used")
	}
	v.AllocMsg = time.Now().String() + " : " + string(debug.Stack())
	v.OnUsed = true
	return v
}
