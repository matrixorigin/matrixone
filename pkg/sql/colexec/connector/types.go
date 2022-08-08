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

package connector

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Argument pipe connector
type Argument struct {
	vecs []*vector.Vector
	Reg  *process.WaitRegister
}

func (arg *Argument) MarshalBinary() ([]byte, error) {
	return encoding.Encode(&Argument{
		vecs: arg.vecs,
		Reg:  arg.Reg,
	})
}

func (arg *Argument) UnmarshalBinary(data []byte) error {
	rs := new(Argument)
	if err := encoding.Decode(data, rs); err != nil {
		return err
	}
	arg.Reg = rs.Reg
	arg.vecs = rs.vecs
	return nil
}
