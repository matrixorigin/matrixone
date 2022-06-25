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

package main

import (
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
	"github.com/reusee/dscope"
)

type Def struct{}

type Def2 struct{}

func NewScope() Scope {
	scope := fz.NewScope(dscope.Methods(new(Def))...)
	scope = scope.Fork(dscope.Methods(new(Def2))...)

	var load fz.LoadScript
	scope.Assign(&load)
	defs, err := load()
	ce(err)
	if len(defs) > 0 {
		scope = scope.Fork(defs...)
	}

	return scope
}
