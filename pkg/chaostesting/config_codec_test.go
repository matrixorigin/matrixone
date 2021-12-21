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

package fz

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/reusee/dscope"
	"github.com/reusee/e4"
)

func TestConfigCodec(t *testing.T) {
	defer he(nil, e4.TestingFatal(t))

	scope := NewScope().Fork(
		func() MainAction {
			return MainAction{
				Action: RandomActionTree([]ActionMaker{
					func() Action {
						return Seq()
					},
				}, 128),
			}
		},
	)

	scope.Call(func(
		write WriteConfig,
		read ReadConfig,
		createdAt CreatedAt,
		id uuid.UUID,
		scope dscope.Scope,
		action MainAction,
	) {
		buf := new(bytes.Buffer)
		ce(write(buf))

		decls, err := read(buf)
		ce(err)

		loaded := scope.Fork(decls...)
		loaded.Call(func(
			createdAt2 CreatedAt,
			id2 uuid.UUID,
			action2 MainAction,
		) {
			if createdAt2 != createdAt {
				t.Fatal()
			}
			if id2 != id {
				t.Fatal()
			}
			if !reflect.DeepEqual(action2.Action, action.Action) {
				t.Fatal()
			}
		})
	})

}
