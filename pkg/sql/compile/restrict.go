// Copyright 2021 Matrix Origin
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

package compile

import (
	vrestrict "matrixone/pkg/sql/colexec/restrict"
	"matrixone/pkg/sql/op/restrict"
	"matrixone/pkg/vm"
)

func (c *compile) compileRestrict(o *restrict.Restrict, mp map[string]uint64) ([]*Scope, error) {
	{
		attrs := o.E.Attributes()
		for _, attr := range attrs {
			mp[attr]++
		}
	}
	ss, err := c.compile(o.Prev, mp)
	if err != nil {
		return nil, err
	}
	arg := &vrestrict.Argument{E: o.E}
	if o.IsPD {
		for i, s := range ss {
			ss[i] = pushRestrict(s, arg)
		}
	} else {
		for i, s := range ss {
			ss[i].Ins = append(s.Ins, vm.Instruction{
				Arg: arg,
				Op:  vm.Restrict,
			})
		}
	}
	return ss, nil
}

func pushRestrict(s *Scope, arg *vrestrict.Argument) *Scope {
	if s.Magic == Merge || s.Magic == Remote {
		for i := range s.Ss {
			s.Ss[i] = pushRestrict(s.Ss[i], arg)
		}
	} else {
		n := len(s.Ins) - 1
		s.Ins = append(s.Ins, vm.Instruction{
			Arg: arg,
			Op:  vm.Restrict,
		})
		s.Ins[n], s.Ins[n+1] = s.Ins[n+1], s.Ins[n]
	}
	return s

}
