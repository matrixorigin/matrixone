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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func (_ Def2) Do(
	newKV NewKV,
	nodes Nodes,
	log LogPorcupineOp,
) fz.Do {

	kv := newKV(nodes[0])

	return func(action fz.Action) error {

		switch action := action.(type) {

		case ActionSet:
			return log(
				func() (any, any, error) {
					if err := kv.Set(action.Key, action.Value, time.Second*32); err != nil {
						return nil, nil, err
					}
					return [2]any{"set", action.Key}, action.Value, nil
				},
			)

		case ActionGet:
			return log(
				func() (any, any, error) {
					var res int
					if err := kv.Get(action.Key, &res, time.Second*32); err != nil {
						return nil, nil, err
					}
					return [2]any{"get", action.Key}, res, nil
				},
			)

		default:
			panic(fmt.Errorf("unknown action: %#v", action))

		}

	}
}
