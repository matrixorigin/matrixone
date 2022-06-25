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

package tree

var (
	constDBoolTrue  DBool = true
	constDBoolFalse DBool = false

	DBoolTrue  = &constDBoolTrue
	DBoolFalse = &constDBoolFalse

	DNull Datum = &dNull{}
)

type Datum interface {
	Expr
}

type DBool bool

func (node *DBool) Format(ctx *FmtCtx) {
	ctx.WriteString(node.String())
}

func (node *DBool) String() string {
	if *node {
		return "true"
	} else {
		return "false"
	}
}

func MakeDBool(d bool) *DBool {
	if d {
		return DBoolTrue
	}
	return DBoolFalse
}

type dNull struct {
	Datum
}

func (node *dNull) Format(ctx *FmtCtx) {
	ctx.WriteString("null")
}
