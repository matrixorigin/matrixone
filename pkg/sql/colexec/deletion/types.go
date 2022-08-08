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

package deletion

import (
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type Argument struct {
	Ts           uint64
	DeleteCtxs   []*DeleteCtx
	AffectedRows uint64
}

type DeleteCtx struct {
	IsHideKey    bool
	TableSource  engine.Relation
	UseDeleteKey string
	CanTruncate  bool
}

func (arg *Argument) MarshalBinary() ([]byte, error) {
	return encoding.Encode(&Argument{
		Ts:           arg.Ts,
		DeleteCtxs:   arg.DeleteCtxs,
		AffectedRows: arg.AffectedRows,
	})
}

func (arg *Argument) UnmarshalBinary(data []byte) error {
	rs := new(Argument)
	if err := encoding.Decode(data, rs); err != nil {
		return err
	}
	arg.Ts = rs.Ts
	arg.AffectedRows = rs.AffectedRows
	arg.DeleteCtxs = rs.DeleteCtxs
	return nil
}
