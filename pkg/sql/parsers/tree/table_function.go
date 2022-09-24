// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"bytes"
	"encoding/gob"
)

var (
	// use this to avoid the error: gob: type not registered for interface: tree.UnresolvedName
	registered = false
)

type TableFunctionParam struct {
	Name  string
	Exprs Exprs
}

type TableFunction struct {
	statementImpl
	Func *FuncExpr
}

func (t *TableFunction) Format(ctx *FmtCtx) {
	t.Func.Format(ctx)
}

func (t TableFunction) Id() string {
	_, _, name := t.Func.Func.FunctionReference.(*UnresolvedName).GetNames()
	return name
}

func (t TableFunction) MarshalParam() ([]byte, error) {
	maybeRegister()
	dt := &TableFunctionParam{
		Name:  t.Id(),
		Exprs: t.Func.Exprs,
	}
	return dt.Marshal()
}

func (tp TableFunctionParam) Marshal() ([]byte, error) {
	maybeRegister()
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(tp)
	//logutil.Infof("TableFunctionParam.Marshal: %v, dt:%v", tp, string(buf.Bytes()))
	return buf.Bytes(), err
}

func (tp *TableFunctionParam) Unmarshal(dt []byte) error {
	maybeRegister()
	dec := gob.NewDecoder(bytes.NewBuffer(dt))
	return dec.Decode(tp)
}

func maybeRegister() {
	if registered {
		return
	}
	gob.Register(&UnresolvedName{})
	gob.Register(&NumVal{})
	gob.Register(&StrVal{})
	registered = true
}
